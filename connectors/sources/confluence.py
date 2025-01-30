#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Confluence source module responsible to fetch documents from Confluence Cloud/Server."""

import asyncio
import os
from copy import copy
from functools import partial
from urllib.parse import urljoin

import aiohttp
from aiohttp.client_exceptions import (
    ClientResponseError,
    ServerDisconnectedError,
)

from connectors.access_control import ACCESS_CONTROL
from connectors.logger import logger
from connectors.source import BaseDataSource, ConfigurableFieldValueError
from connectors.sources.atlassian import (
    AtlassianAccessControl,
    AtlassianAdvancedRulesValidator,
    prefix_account_email,
    prefix_account_id,
    prefix_account_name,
    prefix_group,
    prefix_group_id,
    prefix_user,
)
from connectors.utils import (
    CancellableSleeps,
    ConcurrentTasks,
    MemQueue,
    RetryStrategy,
    html_to_text,
    iso_utc,
    nested_get_from_dict,
    retryable,
    ssl_context,
)

RETRIES = 3
RETRY_INTERVAL = 2
DEFAULT_RETRY_SECONDS = 30

LIMIT = 50
SPACE = "space"
SPACE_PERMISSION = "space_permission"
BLOGPOST = "blogpost"
PAGE = "page"
ATTACHMENT = "attachment"
CONTENT = "content"
DOWNLOAD = "download"
SEARCH = "search"
USER = "user"
USERS_FOR_DATA_CENTER = "users_for_data_center"
SEARCH_FOR_DATA_CENTER = "search_for_data_center"
USERS_FOR_SERVER = "users_for_server"
SPACE_QUERY = "limit=100&expand=permissions,history"
ATTACHMENT_QUERY = "limit=100&expand=version,history"
CONTENT_QUERY = "limit=50&expand=ancestors,children.attachment,history.lastUpdated,body.storage,space,space.permissions,restrictions.read.restrictions.user,restrictions.read.restrictions.group"
SEARCH_QUERY = "limit=100&expand=content.history,content.extensions,content.container,content.space,content.body.storage,space.description,space.history"
USER_QUERY = "expand=groups,applicationRoles"
LABEL = "label"

URLS = {
    SPACE: "rest/api/space?{api_query}",
    SPACE_PERMISSION: "rest/extender/1.0/permission/space/{space_key}/getSpacePermissionActors/VIEWSPACE",
    CONTENT: "rest/api/content/search?{api_query}",
    ATTACHMENT: "rest/api/content/{id}/child/attachment?{api_query}",
    SEARCH: "rest/api/search?cql={query}",
    SEARCH_FOR_DATA_CENTER: "rest/api/search?cql={query}&start={start}",
    USER: "rest/api/3/users/search",
    USERS_FOR_DATA_CENTER: "rest/api/user/list?limit={limit}&start={start}",
    USERS_FOR_SERVER: "rest/extender/1.0/user/getUsersWithConfluenceAccess?showExtendedDetails=true&startAt={start}&maxResults={limit}",
    LABEL: "rest/api/content/{id}/label",
}
PING_URL = "rest/api/space?limit=1"
MAX_CONCURRENT_DOWNLOADS = 50  # Max concurrent download supported by confluence
MAX_CONCURRENCY = 50
QUEUE_SIZE = 1024
QUEUE_MEM_SIZE = 25 * 1024 * 1024  # Size in Megabytes
SERVER_USER_BATCH = 1000
DATACENTER_USER_BATCH = 200
END_SIGNAL = "FINISHED_TASK"

CONFLUENCE_CLOUD = "confluence_cloud"
CONFLUENCE_SERVER = "confluence_server"
CONFLUENCE_DATA_CENTER = "confluence_data_center"
WILDCARD = "*"


class InvalidConfluenceDataSourceTypeError(ValueError):
    pass


class ThrottledError(Exception):
    """Internal exception class to indicate that request was throttled by the API"""

    pass


class InternalServerError(Exception):
    pass


class NotFound(Exception):
    pass


class ConfluenceClient:
    """Confluence client to handle API calls made to Confluence"""

    def __init__(self, configuration):
        self._sleeps = CancellableSleeps()
        self.configuration = configuration
        self._logger = logger
        self.data_source_type = self.configuration["data_source"]
        self.host_url = self.configuration["confluence_url"]
        self.ssl_enabled = self.configuration["ssl_enabled"]
        self.certificate = self.configuration["ssl_ca"]
        self.retry_count = self.configuration["retry_count"]
        self.index_labels = self.configuration["index_labels"]
        if self.data_source_type == CONFLUENCE_CLOUD:
            self.host_url = os.path.join(self.host_url, "wiki")

        if self.ssl_enabled and self.certificate:
            self.ssl_ctx = ssl_context(certificate=self.certificate)
        else:
            self.ssl_ctx = False
        self.session = None

    def set_logger(self, logger_):
        self._logger = logger_

    def _get_session(self):
        """Generate and return base client session with configuration fields

        Returns:
            aiohttp.ClientSession: An instance of Client Session
        """
        if self.session:
            return self.session

        self._logger.debug(f"Creating a '{self.data_source_type}' client session")
        if self.data_source_type == CONFLUENCE_CLOUD:
            auth = (
                self.configuration["account_email"],
                self.configuration["api_token"],
            )
        elif self.data_source_type == CONFLUENCE_SERVER:
            auth = (
                self.configuration["username"],
                self.configuration["password"],
            )
        elif self.data_source_type == CONFLUENCE_DATA_CENTER:
            auth = (
                self.configuration["data_center_username"],
                self.configuration["data_center_password"],
            )
        else:
            msg = f"Unknown data source type '{self.data_source_type}' for Confluence connector"
            self._logger.error(msg)

            raise InvalidConfluenceDataSourceTypeError(msg)

        basic_auth = aiohttp.BasicAuth(login=auth[0], password=auth[1])
        timeout = aiohttp.ClientTimeout(total=None)  # pyright: ignore
        self.session = aiohttp.ClientSession(
            auth=basic_auth,
            headers={
                "accept": "application/json",
                "content-type": "application/json",
            },
            timeout=timeout,
            raise_for_status=True,
        )
        return self.session

    async def close_session(self):
        """Closes unclosed client session"""
        self._sleeps.cancel()
        if self.session is None:
            return
        await self.session.close()
        self.session = None

    async def _handle_client_errors(self, url, exception):
        if exception.status == 429:
            response_headers = exception.headers or {}
            retry_seconds = DEFAULT_RETRY_SECONDS
            if "Retry-After" in response_headers:
                try:
                    retry_seconds = int(response_headers["Retry-After"])
                except (TypeError, ValueError) as exception:
                    self._logger.warning(
                        f"Rate limit reached but an unexpected error occurred while reading value of 'Retry-After' header: {exception}. Retrying in {DEFAULT_RETRY_SECONDS} seconds..."
                    )
            else:
                self._logger.warning(
                    f"Rate limit reached but no 'Retry-After' header was found. Retrying in {DEFAULT_RETRY_SECONDS} seconds..."
                )
            self._logger.debug(
                f"Rate Limit reached: retrying in {retry_seconds} seconds..."
            )

            await self._sleeps.sleep(retry_seconds)
            raise ThrottledError
        elif exception.status == 404:
            self._logger.error(f"Received Not Found error for URL: {url}")
            raise NotFound
        elif exception.status == 500:
            self._logger.error(f"Internal Server Error occurred for URL: {url}")
            raise InternalServerError
        else:
            self._logger.error(
                f"Error while making a GET call for URL: {url}.", exc_info=exception
            )
            raise

    @retryable(
        retries=RETRIES,
        interval=RETRY_INTERVAL,
        strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
        skipped_exceptions=NotFound,
    )
    async def api_call(self, url):
        """Make a GET call for Atlassian API using the passed url with retry for the failed API calls.

        Args:
            url: Request URL to hit the get call

        Raises:
            exception: An instance of an exception class.

        Yields:
            response: Client response
        """
        self._logger.debug(f"Making a GET request to URL: {url}")

        try:
            return await self._get_session().get(
                url=url,
                ssl=self.ssl_ctx,
            )
        except ServerDisconnectedError:
            self._logger.error(
                f"Server was disconnected during GET request to URL: {url}. Closing the session."
            )
            await self.close_session()
            raise
        except ClientResponseError as exception:
            await self._handle_client_errors(url=url, exception=exception)

    async def paginated_api_call(self, url_name, **url_kwargs):
        """Make a paginated API call for Confluence objects using the passed url_name.
        Args:
            url_name (str): URL Name to identify the API endpoint to hit
        Yields:
            response: JSON response.
        """
        url = os.path.join(self.host_url, URLS[url_name].format(**url_kwargs))
        while True:
            try:
                self._logger.debug(f"Starting pagination for API endpoint {url}")
                response = await self.api_call(url=url)
                json_response = await response.json()
                links = json_response.get("_links", {})
                yield json_response
                if links.get("next") is None:
                    return
                url = os.path.join(
                    self.host_url,
                    links.get("next")[1:],
                )
            except Exception as exception:
                self._logger.warning(
                    f"Error raised while paginating {url_name} from {url}.",
                    exc_info=exception,
                )
                raise exception

    async def paginated_api_call_for_datacenter_syncrule(self, url_name, **url_kwargs):
        """Make a paginated API call for datacenter using the passed url_name.
        Args:
            url_name (str): URL Name to identify the API endpoint to hit
        Yields:
            response: JSON response.
        """
        start = 0
        while True:
            url = os.path.join(self.host_url, URLS[url_name].format(**url_kwargs))
            json_response = {}
            try:
                self._logger.debug(
                    f"Starting pagination for API endpoint: {URLS[url_name].format(**url_kwargs)} to host: {self.host_url} with the following parameters -> start: {start}, limit: {LIMIT}"
                )
                response = await self.api_call(url=url)
                json_response = await response.json()
                yield json_response

                start = url_kwargs.get("start", 0)
                start += LIMIT
                url_kwargs["start"] = start
                if len(json_response.get("results", [])) < LIMIT:
                    break
            except Exception as exception:
                self._logger.warning(
                    f"Error raised while paginating {url_name} from {url}.",
                    exc_info=exception,
                )
                raise exception

    async def download_func(self, url):
        yield await self.api_call(url)

    async def search_by_query(self, query):
        if self.data_source_type == CONFLUENCE_DATA_CENTER:
            search_documents = self.paginated_api_call_for_datacenter_syncrule(
                url_name=SEARCH_FOR_DATA_CENTER,
                query=f"{query}&{SEARCH_QUERY}",
                start=0,
            )
        else:
            search_documents = self.paginated_api_call(
                url_name=SEARCH,
                query=f"{query}&{SEARCH_QUERY}",
            )
        async for response in search_documents:
            for entity in response.get("results", []):
                yield entity

    async def fetch_spaces(self):
        async for response in self.paginated_api_call(
            url_name=SPACE,
            api_query=SPACE_QUERY,
        ):
            for space in response.get("results", []):
                spaces = self.configuration.get("spaces", "")
                if (spaces == [WILDCARD]) or (space.get("key", "") in spaces):
                    yield space

    async def fetch_server_space_permission(self, url):
        try:
            permissions = await self.api_call(url=os.path.join(self.host_url, url))
            permission = await permissions.json()
            return permission
        except ClientResponseError as exception:
            self._logger.warning(
                "Something went wrong. Make sure you have installed Extender for running confluence datacenter/server DLS.",
                exc_info=exception,
            )
            return {}

    async def fetch_page_blog_documents(self, api_query):
        try:
            async for response in self.paginated_api_call(
                url_name=CONTENT,
                api_query=api_query,
            ):
                for document in response.get("results", []):
                    try:
                        if document.get("children").get("attachment"):
                            attachment_count = (
                                document.get("children", {})
                                .get("attachment", {})
                                .get("size", 0)
                            )
                            if self.index_labels:
                                labels = await self.fetch_label(document["id"])
                                document["labels"] = labels
                            yield document, attachment_count
                    except Exception as e:
                        # avoid letting single-document issues bubble up to interrupt the iteration
                        self._logger.warning(
                            f"Encountered error while handling content documents: '{document.get('id')}'. Skipping it.",
                            exc_info=e,
                        )
        except Exception as e:
            # avoid letting errors while paginating content cause a sync failure
            self._logger.warning(
                f"Encountered an error while fetching content with query: '{api_query}'. Any remaining content from this query will be skipped.",
                exc_info=e,
            )

    async def fetch_attachments(self, content_id):
        try:
            async for response in self.paginated_api_call(
                url_name=ATTACHMENT,
                api_query=ATTACHMENT_QUERY,
                id=content_id,
            ):
                for attachment in response.get("results", []):
                    yield attachment
        except Exception as e:
            # swallow attachment exceptions during pagination, rather than let them fail the sync.
            self._logger.warning(
                f"Encountered an error while fetching attachments for '{content_id}'. Any remaining attachments will be skipped.",
                exc_info=e,
            )

    async def ping(self):
        await self.api_call(
            url=os.path.join(self.host_url, PING_URL),
        )

    async def fetch_confluence_server_users(self):
        start_at = 0
        if self.data_source_type == CONFLUENCE_DATA_CENTER:
            limit = DATACENTER_USER_BATCH
            key = "results"
            url = urljoin(self.host_url, URLS[USERS_FOR_DATA_CENTER])
        else:
            limit = SERVER_USER_BATCH
            key = "users"
            url = urljoin(self.host_url, URLS[USERS_FOR_SERVER])

        while True:
            url_ = url.format(start=start_at, limit=limit)
            users = await self.api_call(url=url_)
            response = await users.json()
            if len(response.get(key)) == 0:
                return
            yield response.get(key)
            start_at += limit

    async def fetch_label(self, label_id):
        url = os.path.join(self.host_url, URLS[LABEL].format(id=label_id))
        label_data = await self.api_call(url=url)
        labels = await label_data.json()
        return [label.get("name") for label in labels["results"]]


class ConfluenceDataSource(BaseDataSource):
    """Confluence"""

    name = "Confluence"
    service_type = "confluence"
    advanced_rules_enabled = True
    dls_enabled = True
    incremental_sync_enabled = True

    def __init__(self, configuration):
        """Setup the connection to Confluence

        Args:
            configuration (DataSourceConfiguration): Object of DataSourceConfiguration class.
        """
        super().__init__(configuration=configuration)
        self.spaces = self.configuration["spaces"]
        self.concurrent_downloads = self.configuration["concurrent_downloads"]
        self.confluence_client = ConfluenceClient(configuration=configuration)
        self.atlassian_access_control = AtlassianAccessControl(
            self, self.confluence_client
        )

        self.queue = MemQueue(maxsize=QUEUE_SIZE, maxmemsize=QUEUE_MEM_SIZE)
        self.fetchers = ConcurrentTasks(max_concurrency=MAX_CONCURRENCY)
        self.fetcher_count = 0
        self.authorkey = (
            "username"
            if self.confluence_client.data_source_type == "confluence_data_center"
            else "publicName"
        )

    def _set_internal_logger(self):
        self.confluence_client.set_logger(self._logger)

    @classmethod
    def get_default_configuration(cls):
        """Get the default configuration for Confluence

        Returns:
            dictionary: Default configuration.
        """
        return {
            "data_source": {
                "display": "dropdown",
                "label": "Confluence data source",
                "options": [
                    {"label": "Confluence Cloud", "value": CONFLUENCE_CLOUD},
                    {"label": "Confluence Server", "value": CONFLUENCE_SERVER},
                    {
                        "label": "Confluence Data Center",
                        "value": CONFLUENCE_DATA_CENTER,
                    },
                ],
                "order": 1,
                "type": "str",
                "value": CONFLUENCE_SERVER,
            },
            "username": {
                "depends_on": [{"field": "data_source", "value": CONFLUENCE_SERVER}],
                "label": "Confluence Server username",
                "order": 2,
                "type": "str",
            },
            "password": {
                "depends_on": [{"field": "data_source", "value": CONFLUENCE_SERVER}],
                "label": "Confluence Server password",
                "sensitive": True,
                "order": 3,
                "type": "str",
            },
            "data_center_username": {
                "depends_on": [
                    {"field": "data_source", "value": CONFLUENCE_DATA_CENTER}
                ],
                "label": "Confluence Data Center username",
                "order": 4,
                "type": "str",
            },
            "data_center_password": {
                "depends_on": [
                    {"field": "data_source", "value": CONFLUENCE_DATA_CENTER}
                ],
                "label": "Confluence Data Center password",
                "sensitive": True,
                "order": 5,
                "type": "str",
            },
            "account_email": {
                "depends_on": [{"field": "data_source", "value": CONFLUENCE_CLOUD}],
                "label": "Confluence Cloud account email",
                "order": 6,
                "type": "str",
            },
            "api_token": {
                "depends_on": [{"field": "data_source", "value": CONFLUENCE_CLOUD}],
                "label": "Confluence Cloud API token",
                "sensitive": True,
                "order": 7,
                "type": "str",
            },
            "confluence_url": {
                "label": "Confluence URL",
                "order": 8,
                "type": "str",
            },
            "spaces": {
                "display": "textarea",
                "label": "Confluence space keys",
                "order": 9,
                "tooltip": "This configurable field is ignored when Advanced Sync Rules are used.",
                "type": "list",
            },
            "index_labels": {
                "display": "toggle",
                "label": "Enable indexing labels",
                "order": 10,
                "tooltip": "Enabling this will increase the amount of network calls to the source, and may decrease performance",
                "type": "bool",
                "value": False,
            },
            "ssl_enabled": {
                "display": "toggle",
                "label": "Enable SSL",
                "order": 11,
                "type": "bool",
                "value": False,
            },
            "ssl_ca": {
                "depends_on": [{"field": "ssl_enabled", "value": True}],
                "label": "SSL certificate",
                "order": 12,
                "type": "str",
            },
            "retry_count": {
                "default_value": 3,
                "display": "numeric",
                "label": "Retries per request",
                "order": 13,
                "required": False,
                "type": "int",
                "ui_restrictions": ["advanced"],
            },
            "concurrent_downloads": {
                "default_value": MAX_CONCURRENT_DOWNLOADS,
                "display": "numeric",
                "label": "Maximum concurrent downloads",
                "order": 14,
                "required": False,
                "type": "int",
                "ui_restrictions": ["advanced"],
                "validations": [
                    {"type": "less_than", "constraint": MAX_CONCURRENT_DOWNLOADS + 1}
                ],
            },
            "use_document_level_security": {
                "display": "toggle",
                "label": "Enable document level security",
                "order": 15,
                "tooltip": "Document level security ensures identities and permissions set in confluence are maintained in Elasticsearch. This enables you to restrict and personalize read-access users have to documents in this index. Access control syncs ensure this metadata is kept up to date in your Elasticsearch documents.",
                "type": "bool",
                "value": False,
            },
            "use_text_extraction_service": {
                "display": "toggle",
                "label": "Use text extraction service",
                "order": 16,
                "tooltip": "Requires a separate deployment of the Elastic Text Extraction Service. Requires that pipeline settings disable text extraction.",
                "type": "bool",
                "ui_restrictions": ["advanced"],
                "value": False,
            },
        }

    def _dls_enabled(self):
        """Check if document level security is enabled. This method checks whether document level security (DLS) is enabled based on the provided configuration.

        Returns:
            bool: True if document level security is enabled, False otherwise.
        """
        if self._features is None:
            return False

        if not self._features.document_level_security_enabled():
            return False

        return self.configuration["use_document_level_security"]

    def _decorate_with_access_control(self, document, access_control):
        if self._dls_enabled():
            document[ACCESS_CONTROL] = list(
                set(document.get(ACCESS_CONTROL, []) + access_control)
            )

        return document

    async def user_access_control_confluence_server(self, user):
        """Generate a user access control document for confluence server.

        This method generates a user access control document based on the provided user information.
        The document includes the user's account ID, prefixed account ID, and prefixed account name
        The access control list is then constructed using these values.

        Args:
            user (dict): A dictionary containing user information, such as account ID, display name and email.

        Returns:
            dict: A user access control document with the following structure:
                {
                    "_id": <account_id>,
                    "identity": {
                        "account_id": <prefixed_account_id>,
                        "display_name": <prefixed_account_name>
                        "email_address": <prefixed_account_email>
                    },
                    "created_at": <iso_utc_timestamp>,
                    ACCESS_CONTROL: [<prefixed_account_id>, <prefixed_account_email>, <prefixed_account_name>]
                }
        """
        account_id = user.get("key") or user.get("name")
        account_name = user.get("fullName")
        email = user.get("email")

        prefixed_account_email = prefix_account_email(email=email)
        prefixed_account_id = prefix_account_id(account_id=account_id)
        prefixed_account_name = prefix_account_name(account_name=account_name)

        access_control = [
            prefixed_account_id,
            prefixed_account_email,
            prefixed_account_name,
        ]

        return {
            "_id": account_id,
            "identity": {
                "account_id": prefixed_account_id,
                "display_name": prefixed_account_name,
                "email_address": prefixed_account_email,
            },
            "created_at": iso_utc(),
        } | self.atlassian_access_control.access_control_query(
            access_control=access_control
        )

    async def user_access_control_data_center(self, user):
        """Generate a user access control document for confluence data enter.

        This method generates a user access control document based on the provided user information.
        The document includes the user's account ID, prefixed account ID, and prefixed account name
        The access control list is then constructed using these values.

        Args:
            user (dict): A dictionary containing user information, such as account ID, display name and email.

        Returns:
            dict: A user access control document with the following structure:
                {
                    "_id": <account_id>,
                    "identity": {
                        "account_id": <prefixed_account_id>,
                        "username": <prefixed_account_name>,
                        "display_name: <prefixed_display_name>
                    },
                    "created_at": <iso_utc_timestamp>,
                    ACCESS_CONTROL: [<prefixed_account_id>, <prefixed_account_name>, <prefixed_display_name>]
                }
        """
        account_id = user.get("userKey")
        account_name = user.get("username")
        display_name = user.get("displayName")

        prefixed_account_id = prefix_account_id(account_id=account_id)
        prefixed_account_name = prefix_account_name(account_name=account_name)
        prefixed_display_name = prefix_account_name(account_name=display_name)

        access_control = [
            prefixed_account_id,
            prefixed_account_name,
            prefixed_display_name,
        ]

        return {
            "_id": account_id,
            "identity": {
                "account_id": prefixed_account_id,
                "username": prefixed_account_name,
                "display_name": prefixed_display_name,
            },
            "created_at": iso_utc(),
        } | self.atlassian_access_control.access_control_query(
            access_control=access_control
        )

    async def get_user_for_server(self):
        async for users in self.confluence_client.fetch_confluence_server_users():
            for user_info in users:
                if self.confluence_client.data_source_type == CONFLUENCE_DATA_CENTER:
                    yield await self.user_access_control_data_center(user=user_info)
                else:
                    yield await self.user_access_control_confluence_server(
                        user=user_info
                    )

    async def get_user(self):
        url = os.path.join(self.configuration["confluence_url"], URLS[USER])
        async for users in self.atlassian_access_control.fetch_all_users_for_confluence(
            url=url
        ):
            active_atlassian_users = filter(
                self.atlassian_access_control.is_active_atlassian_user, users
            )
            tasks = [
                anext(
                    self.atlassian_access_control.fetch_user_for_confluence(
                        url=f"{user_info.get('self')}&{USER_QUERY}"
                    )
                )
                for user_info in active_atlassian_users
            ]
            user_results = await asyncio.gather(*tasks)

            for user in user_results:
                yield await self.atlassian_access_control.user_access_control_doc(
                    user=user
                )

    async def get_access_control(self):
        """Get access control documents for active Atlassian users.

        This method fetches access control documents for active Atlassian users when document level security (DLS)
        is enabled.
        If DLS is enabled, the method fetches all users from the Confluence API, filters out active Atlassian users,
        and fetches additional information for each active user using the fetch_user method. After gathering the user information,
        it generates an access control document for each user using the user_access_control_doc method and yields the results.

        Yields:
            dict: An access control document for each active Atlassian user.
        """
        if not self._dls_enabled():
            self._logger.warning("DLS is not enabled. Skipping")
            return

        self._logger.info("Fetching all users for Access Control sync")
        if self.confluence_client.data_source_type == CONFLUENCE_CLOUD:
            users = self.get_user()
        else:
            users = self.get_user_for_server()

        async for user in users:
            yield user

    def _get_access_control_from_permission(self, permissions, target_type):
        if not self._dls_enabled():
            return []

        access_control = set()
        for permission in permissions:
            permission_operation = permission.get("operation", {})
            if permission_operation.get("targetType") != target_type and (
                permission_operation.get("targetType") != SPACE
                and permission_operation.get("operation") != "read"
            ):
                continue

            access_control = access_control.union(
                self._extract_identities(response=permission.get("subjects", {}))
            )

        return access_control

    def _extract_identities(self, response):
        if not self._dls_enabled():
            return set()

        identities = set()
        user_results = response.get("user", {}).get("results", [])
        group_results = response.get("group", {}).get("results", [])

        for item in user_results + group_results:
            item_type = item.get("type")
            if item_type == "known" and item.get("accountType") == "atlassian":
                identities.add(prefix_account_id(account_id=item.get("accountId", "")))
            elif item_type == "group":
                identities.add(prefix_group_id(group_id=item.get("id", "")))

        return identities

    def _extract_identities_for_datacenter(self, response):
        if not self._dls_enabled():
            return set()
        identities = set()
        user_results = response.get("user", {}).get("results", [])
        group_results = response.get("group", {}).get("results", [])

        for item in user_results + group_results:
            item_type = item.get("type")
            if item_type == "known":
                identities.add(prefix_user(item.get("username")))
            elif item_type == "group":
                identities.add(prefix_group(item.get("name", "")))

        return identities

    async def close(self):
        """Closes unclosed client session"""
        await self.confluence_client.close_session()

    def advanced_rules_validators(self):
        return [AtlassianAdvancedRulesValidator(self)]

    def tweak_bulk_options(self, options):
        """Tweak bulk options as per concurrent downloads support by Confluence

        Args:
            options (dictionary): Config bulker options
        """
        options["concurrent_downloads"] = self.concurrent_downloads

    async def validate_config(self):
        """Validates whether user input is empty or not for configuration fields.
        Also validate, if user configured spaces are available in Confluence.

        Raises:
            Exception: Configured keys can't be empty
        """
        await super().validate_config()
        await self._remote_validation()

    async def _remote_validation(self):
        await self.confluence_client.ping()
        if self.spaces == [WILDCARD]:
            return
        space_keys = []
        async for response in self.confluence_client.paginated_api_call(
            url_name=SPACE, api_query=SPACE_QUERY
        ):
            spaces = response.get("results", [])
            space_keys.extend([space.get("key", "") for space in spaces])
        if unavailable_spaces := set(self.spaces) - set(space_keys):
            msg = f"Spaces '{', '.join(unavailable_spaces)}' are not available. Available spaces are: '{', '.join(space_keys)}'"
            raise ConfigurableFieldValueError(msg)

    async def ping(self):
        """Verify the connection with Confluence"""
        try:
            await self.confluence_client.ping()
        except Exception as e:
            self._logger.warning("Error while connecting to Confluence", exc_info=e)
            raise

    def get_permission(self, permission):
        permissions = set()
        if permission.get("users"):
            for user in permission.get("users"):
                permissions.add(prefix_user(user))

        if permission.get("groups"):
            for group in permission.get("groups"):
                permissions.add(prefix_group(group))

        return permissions

    async def fetch_server_space_permission(self, space_key):
        if not self._dls_enabled():
            return {}

        url = URLS[SPACE_PERMISSION].format(space_key=space_key)
        self._logger.info(f"Fetching permissions for '{space_key}' space")
        return await self.confluence_client.fetch_server_space_permission(url=url)

    async def fetch_documents(self, api_query):
        """Get pages and blog posts with the help of REST APIs

        Args:
            api_query (str): Query parameter for the API call

        Yields:
            Dictionary: Page or blog post to be indexed
            Integer: Number of attachments in a page/blogpost
            List: List of permissions attached to document
            Dictionary: Dictionary of restrictions attached to document
        """
        async for (
            document,
            attachment_count,
        ) in self.confluence_client.fetch_page_blog_documents(
            api_query=api_query,
        ):
            document_url = os.path.join(
                self.confluence_client.host_url,
                document.get("_links", {}).get("webui", "")[1:],
            )
            ancestor_title = [
                {"title": ancestor["title"]}
                for ancestor in document.get("ancestors", [])
            ]
            doc = {
                "_id": str(document["id"]),
                "type": document["type"],
                "_timestamp": nested_get_from_dict(
                    document, ["history", "lastUpdated", "when"]
                ),
                "title": document.get("title"),
                "ancestors": ancestor_title,
                "space": nested_get_from_dict(document, ["space", "name"]),
                "body": html_to_text(
                    nested_get_from_dict(document, ["body", "storage", "value"])
                ),
                "url": document_url,
                "author": nested_get_from_dict(
                    document, ["history", "createdBy", self.authorkey]
                ),
                "createdDate": nested_get_from_dict(
                    document, ["history", "createdDate"]
                ),
            }
            if self.confluence_client.index_labels:
                doc["labels"] = document["labels"]
            yield (
                doc,
                attachment_count,
                document.get("space", {}).get("key"),
                document.get("space", {}).get("permissions", []),
                document.get("restrictions", {})
                .get("read", {})
                .get("restrictions", {}),
            )

    async def fetch_attachments(
        self, content_id, parent_name, parent_space, parent_type
    ):
        """Fetches all the attachments present in the given content (pages and blog posts)

        Args:
            content_id (str): Unique identifier of Confluence content (pages and blogposts)
            parent_name (str): Page/Blog post where the attachment is present
            parent_space (str): Confluence Space where the attachment is present
            parent_type (str): Type of the parent (Blog post or Page)

        Yields:
            Dictionary: Confluence attachment on the given page or blog post
            String: Download link to get the content of the attachment
        """
        self._logger.info(
            f"Fetching attachments for '{parent_name}' {parent_type} from '{parent_space}' space"
        )
        async for attachment in self.confluence_client.fetch_attachments(
            content_id=content_id,
        ):
            attachment_url = os.path.join(
                self.confluence_client.host_url,
                attachment.get("_links", {}).get("webui", "")[1:],
            )
            yield (
                {
                    "type": attachment.get("type"),
                    "title": attachment.get("title"),
                    "_id": attachment.get("id"),
                    "space": parent_space,
                    parent_type: parent_name,
                    "_timestamp": attachment.get("version", {}).get("when", iso_utc()),
                    "size": attachment.get("extensions", {}).get("fileSize", 0),
                    "url": attachment_url,
                    "createdDate": nested_get_from_dict(
                        attachment, ["history", "createdDate"]
                    ),
                },
                attachment.get("_links", {}).get("download"),
            )

    async def search_by_query(self, query):
        async for entity in self.confluence_client.search_by_query(query=query):
            # entity can be space or content
            entity_details = entity.get(SPACE) or entity.get(CONTENT)

            if (
                entity_details.get("type", "") == "attachment"
                and entity_details.get("container", {}).get("title") is None
            ):
                continue

            document = {
                "_id": entity_details.get("id"),
                "title": entity.get("title"),
                "_timestamp": entity.get("lastModified"),
                "body": html_to_text(
                    entity_details.get("body", {}).get("storage", {}).get("value")
                ),
                "type": entity.get("entityType"),
                "url": os.path.join(
                    self.confluence_client.host_url, entity.get("url")[1:]
                ),
            }
            download_url = None
            if (
                document["type"] == "space"
                and self.confluence_client.data_source_type == CONFLUENCE_CLOUD
            ):
                document["author"] = nested_get_from_dict(
                    entity, ["space", "history", "createdBy", self.authorkey]
                )
                document["createdDate"] = nested_get_from_dict(
                    entity, ["space", "history", "createdDate"]
                )
            if document.get("type", "") == "content":
                document.update(
                    {
                        "type": entity_details.get("type"),
                        "space": entity_details.get("space", {}).get("name"),
                    }
                )
                document["author"] = nested_get_from_dict(
                    entity, ["content", "history", "createdBy", self.authorkey]
                )
                document["createdDate"] = nested_get_from_dict(
                    entity, ["content", "history", "createdDate"]
                )

                if document.get("type", "") == "attachment":
                    container_type = entity_details.get("container", {}).get("type")
                    container_title = entity_details.get("container", {}).get("title")
                    file_size = entity_details.get("extensions", {}).get("fileSize")
                    document.update(
                        {"size": file_size, container_type: container_title}
                    )
                    # Removing body as attachment will be downloaded lazily
                    document.pop("body")
                    download_url = entity_details.get("_links", {}).get("download")

            yield document, download_url

    async def download_attachment(self, url, attachment, timestamp=None, doit=False):
        """Downloads the content of the given attachment in chunks using REST API call

        Args:
            url (str): url endpoint to download the attachment content
            attachment (dict): Dictionary containing details of the attachment
            timestamp (timestamp, optional): Timestamp of Confluence last modified. Defaults to None.
            doit (boolean, optional): Boolean value for whether to get content or not. Defaults to False.

        Returns:
            Dictionary: Document of the attachment to be indexed.
        """
        file_size = int(attachment["size"])
        if not (doit and file_size):
            return

        filename = attachment["title"]
        file_extension = self.get_file_extension(filename)
        if not self.can_file_be_downloaded(file_extension, filename, file_size):
            return

        self._logger.debug(f"Downloading content for file: {filename}")
        document = {"_id": attachment["_id"], "_timestamp": attachment["_timestamp"]}
        return await self.download_and_extract_file(
            document,
            filename,
            file_extension,
            partial(
                self.generic_chunked_download_func,
                partial(
                    self.confluence_client.download_func,
                    url=os.path.join(self.confluence_client.host_url, url),
                ),
            ),
        )

    async def _attachment_coro(self, document, access_control):
        """Coroutine to add attachments to Queue and download content

        Args:
            document (dict): Formatted document of page/blogpost
            access_control (list): List of identities which have access to document
        """
        try:
            async for attachment, download_link in self.fetch_attachments(
                content_id=document.get("_id"),
                parent_name=document.get("title"),
                parent_space=document.get("space"),
                parent_type=document.get("type"),
            ):
                attachment = self._decorate_with_access_control(
                    document=attachment, access_control=access_control
                )
                await self.queue.put(
                    (  # pyright: ignore
                        attachment,
                        partial(
                            self.download_attachment,
                            download_link[1:],
                            copy(attachment),
                        ),
                    )
                )
        except Exception as exception:
            self._logger.warning(
                f"Error while fetching attachments of {document.get('title')} with id {document.get('_id')}, type: {document.get('type')} in space {document.get('space')}",
                exc_info=exception,
            )
        finally:
            await self.queue.put(END_SIGNAL)  # pyright: ignore

    def format_space(self, space):
        space_url = os.path.join(
            self.confluence_client.host_url,
            space.get("_links", {}).get("webui", "")[1:],
        )
        document = {
            "_id": space.get("id"),
            "type": "Space",
            "title": space.get("name"),
            "_timestamp": iso_utc(),
            "url": space_url,
            "key": space.get("key"),
        }
        if self.confluence_client.data_source_type == CONFLUENCE_CLOUD:
            document["createdDate"] = nested_get_from_dict(
                space, ["history", "createdDate"]
            )
            document["author"] = nested_get_from_dict(
                space, ["history", "createdBy", self.authorkey]
            )
        return document

    async def _space_coro(self):
        """Coroutine to add spaces documents to Queue"""
        try:
            self._logger.info("Fetching spaces and its permissions from Confluence")
            async for space_metadata in self.confluence_client.fetch_spaces():
                space = self.format_space(space=space_metadata)
                if self.confluence_client.data_source_type == CONFLUENCE_CLOUD:
                    access_control = list(
                        self._get_access_control_from_permission(
                            permissions=space_metadata.get("permissions", []),
                            target_type=SPACE,
                        )
                    )
                else:
                    permission = await self.fetch_server_space_permission(
                        space_key=space_metadata.get("key")
                    )
                    access_control = list(
                        self.get_permission(
                            permission=permission.get("permissions", {}).get(
                                "VIEWSPACE", {}
                            )
                        )
                    )
                space = self._decorate_with_access_control(
                    document=space, access_control=access_control
                )
                yield space
        except Exception as exception:
            self._logger.exception("Error while fetching spaces", exc_info=exception)
            raise

    async def _page_blog_coro(self, api_query, target_type):
        """Coroutine to add pages/blogposts to Queue

        Args:
            api_query (str): API Query Parameters for fetching page/blogpost
            target_type (str): Type of object to filter permission
        """
        try:
            self._logger.info(
                f"Fetching {target_type} and its permissions from Confluence"
            )
            self._logger.debug(
                f"Fetching {target_type} using Confluence query: '{api_query}'"
            )
            async for (
                document,
                attachment_count,
                space_key,
                permissions,
                restrictions,
            ) in self.fetch_documents(api_query):
                # Pages and blog posts are open to viewing or editing by default,
                # but you can restrict either viewing or editing to certain users or groups.
                if self.confluence_client.data_source_type == CONFLUENCE_CLOUD:
                    access_control = list(
                        self._extract_identities(response=restrictions)
                    )
                    if len(access_control) == 0:
                        # Every space has its own independent set of permissions, managed by the space admin(s),
                        # which determine the access settings for different users and groups.
                        access_control = list(
                            self._get_access_control_from_permission(
                                permissions=permissions, target_type=target_type
                            )
                        )
                else:
                    access_control = list(
                        self._extract_identities_for_datacenter(response=restrictions)
                    )
                    if len(access_control) == 0:
                        permission = await self.fetch_server_space_permission(
                            space_key=space_key
                        )
                        access_control = list(
                            self.get_permission(
                                permission=permission.get("permissions", {}).get(
                                    "VIEWSPACE", {}
                                )
                            )
                        )
                document = self._decorate_with_access_control(
                    document=document, access_control=access_control
                )
                await self.queue.put((document, None))  # pyright: ignore
                if attachment_count > 0:
                    await self.fetchers.put(
                        partial(self._attachment_coro, copy(document), access_control)
                    )
                    self.fetcher_count += 1
        except Exception as exception:
            self._logger.exception(
                f"Error while fetching pages and blogposts with query '{api_query}'",
                exc_info=exception,
            )
            raise
        finally:
            await self.queue.put(END_SIGNAL)  # pyright: ignore

    async def _consumer(self):
        """Async generator to process entries of the queue

        Yields:
            dictionary: Documents from Confluence.
        """
        while self.fetcher_count > 0:
            _, item = await self.queue.get()
            if item == END_SIGNAL:
                self.fetcher_count -= 1
            else:
                yield item

    async def get_docs(self, filtering=None):
        """Executes the logic to fetch Confluence content in async manner.

        Args:
            filtering (Filtering): Object of Class Filtering
        Yields:
            dictionary: dictionary containing meta-data of the content.
        """
        self._logger.info("Successfully connected to Confluence")
        if filtering and filtering.has_advanced_rules():
            advanced_rules = filtering.get_advanced_rules()
            for query_info in advanced_rules:
                query = query_info.get("query")
                self._logger.debug(f"Fetching content using custom query: {query}")
                async for document, download_link in self.search_by_query(query):
                    if download_link:
                        yield (
                            document,
                            partial(
                                self.download_attachment,
                                download_link[1:],
                                copy(document),
                            ),
                        )
                    else:
                        yield document, None

        else:
            async for space in self._space_coro():
                yield space, None
                self._logger.info(f"Fetching docs from space: {space['key']}")
                configured_spaces_query = f"cql=space in ('{space['key']}') AND type="
                await self.fetchers.put(
                    partial(
                        self._page_blog_coro,
                        f"{configured_spaces_query}{BLOGPOST}&{CONTENT_QUERY}",
                        BLOGPOST,
                    )
                )
                await self.fetchers.put(
                    partial(
                        self._page_blog_coro,
                        f"{configured_spaces_query}{PAGE}&{CONTENT_QUERY}",
                        PAGE,
                    )
                )
                self.fetcher_count += 2

                async for item in self._consumer():
                    yield item
            await self.fetchers.join()
