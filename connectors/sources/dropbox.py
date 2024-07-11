#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Dropbox source module responsible to fetch documents from Dropbox online.
"""
import json
import os
from datetime import datetime
from enum import Enum
from functools import cached_property, partial
from urllib import parse

import aiohttp
import fastjsonschema
from aiohttp.client_exceptions import ClientResponseError, ServerDisconnectedError

from connectors.access_control import (
    ACCESS_CONTROL,
    es_access_control_query,
    prefix_identity,
)
from connectors.filtering.validation import (
    AdvancedRulesValidator,
    SyncRuleValidationResult,
)
from connectors.logger import logger
from connectors.source import BaseDataSource
from connectors.utils import (
    CancellableSleeps,
    RetryStrategy,
    evaluate_timedelta,
    is_expired,
    iso_utc,
    retryable,
)

RETRY_COUNT = 3
DEFAULT_RETRY_AFTER = 300  # seconds
RETRY_INTERVAL = 2
MAX_CONCURRENT_DOWNLOADS = 100
REQUEST_BATCH_SIZE = 50
BATCH_RESPONSE_COUNT = 20
LIMIT = 300  # Limit for fetching shared files per call
FILE_LIMIT = 2000
PAPER = "paper"
FILE = "File"
FOLDER = "Folder"
RECEIVED_FILE = "Received File"

API_VERSION = 2

if "RUNNING_FTEST" in os.environ:
    logger.warning("x" * 100)
    logger.warning(
        f"DROPBOX CONNECTOR CALLS ARE REDIRECTED TO {os.environ['DROPBOX_API_URL']}"
    )
    logger.warning("IT'S SUPPOSED TO BE USED ONLY FOR TESTING")
    logger.warning("x" * 100)
    BASE_URLS = {
        "ACCESS_TOKEN_BASE_URL": os.environ["DROPBOX_API_URL"],
        "FILES_FOLDERS_BASE_URL": os.environ["DROPBOX_API_URL_V2"],
        "DOWNLOAD_BASE_URL": os.environ["DROPBOX_API_URL_V2"],
    }
else:
    BASE_URLS = {
        "ACCESS_TOKEN_BASE_URL": "https://api.dropboxapi.com/",
        "FILES_FOLDERS_BASE_URL": f"https://api.dropboxapi.com/{API_VERSION}/",
        "DOWNLOAD_BASE_URL": f"https://content.dropboxapi.com/{API_VERSION}/",
    }


class EndpointName(Enum):
    ACCESS_TOKEN = "access_token"  # noqa S105
    PING = "ping"
    CHECK_PATH = "check_path"
    FILES_FOLDERS = "files_folders"
    FILES_FOLDERS_CONTINUE = "files_folders_continue"
    FILE_MEMBERS = "file_members"
    FILE_MEMBERS_BATCH = "file_members_batch"
    FILE_MEMBERS_CONTINUE = "file_members_continue"
    FOLDER_MEMBERS = "folder_members"
    FOLDER_MEMBERS_CONTINUE = "folder_members_continue"
    RECEIVED_FILES = "received_files"
    RECEIVED_FILES_CONTINUE = "received_files_continue"
    RECEIVED_FILE_METADATA = "received_files_metadata"
    DOWNLOAD = "download"
    PAPER_FILE_DOWNLOAD = "paper_file_download"
    RECEIVED_FILE_DOWNLOAD = "received_files_download"
    SEARCH = "search"
    SEARCH_CONTINUE = "search_continue"
    MEMBERS = "members"
    MEMBERS_CONTINUE = "members_continue"
    AUTHENTICATED_ADMIN = "authenticated_admin"
    TEAM_FOLDER_LIST = "team_folder_list"
    TEAM_FOLDER_LIST_CONTINUE = "team_folder_list_continue"


ENDPOINTS = {
    EndpointName.ACCESS_TOKEN.value: "oauth2/token",
    EndpointName.PING.value: "users/get_current_account",
    EndpointName.CHECK_PATH.value: "files/get_metadata",
    EndpointName.FILES_FOLDERS.value: "files/list_folder",
    EndpointName.FILES_FOLDERS_CONTINUE.value: "files/list_folder/continue",
    EndpointName.FILE_MEMBERS.value: "sharing/list_file_members",
    EndpointName.FILE_MEMBERS_BATCH.value: "sharing/list_file_members/batch",
    EndpointName.FILE_MEMBERS_CONTINUE.value: "sharing/list_file_members/continue",
    EndpointName.FOLDER_MEMBERS.value: "sharing/list_folder_members",
    EndpointName.FOLDER_MEMBERS_CONTINUE.value: "sharing/list_folder_members/continue",
    EndpointName.RECEIVED_FILES.value: "sharing/list_received_files",
    EndpointName.RECEIVED_FILES_CONTINUE.value: "sharing/list_received_files/continue",
    EndpointName.RECEIVED_FILE_METADATA.value: "sharing/get_shared_link_metadata",
    EndpointName.DOWNLOAD.value: "files/download",
    EndpointName.PAPER_FILE_DOWNLOAD.value: "files/export",
    EndpointName.RECEIVED_FILE_DOWNLOAD.value: "sharing/get_shared_link_file",
    EndpointName.SEARCH.value: "files/search_v2",
    EndpointName.SEARCH_CONTINUE.value: "files/search/continue_v2",
    EndpointName.MEMBERS.value: "team/members/list_v2",
    EndpointName.MEMBERS_CONTINUE.value: "team/members/list/continue_v2",
    EndpointName.AUTHENTICATED_ADMIN.value: "team/token/get_authenticated_admin",
    EndpointName.TEAM_FOLDER_LIST.value: "team/team_folder/list",
    EndpointName.TEAM_FOLDER_LIST_CONTINUE.value: "team/team_folder/list/continue",
}


class InvalidClientCredentialException(Exception):
    pass


class InvalidRefreshTokenException(Exception):
    pass


class InvalidPathException(Exception):
    pass


class BreakingField(Enum):
    CURSOR = "cursor"
    HAS_MORE = "has_more"


def _prefix_user(user):
    if not user:
        return
    return prefix_identity("user", user)


def _prefix_user_id(user_id):
    return prefix_identity("user_id", user_id)


def _prefix_email(email):
    return prefix_identity("email", email)


def _prefix_group(group):
    return prefix_identity("group", group)


class DropboxClient:
    def __init__(self, configuration):
        self._sleeps = CancellableSleeps()
        self.configuration = configuration
        self.path = (
            ""
            if self.configuration["path"] in ["/", None]
            else self.configuration["path"]
        )
        self.retry_count = self.configuration["retry_count"]
        self.app_key = self.configuration["app_key"]
        self.app_secret = self.configuration["app_secret"]
        self.refresh_token = self.configuration["refresh_token"]
        self.access_token = None
        self.token_expiration_time = None
        self.member_id = None
        self._logger = logger

    def set_logger(self, logger_):
        self._logger = logger_

    @retryable(
        retries=RETRY_COUNT,
        interval=RETRY_INTERVAL,
        strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
    )
    async def _set_access_token(self):
        if self.token_expiration_time and (
            not isinstance(self.token_expiration_time, datetime)
        ):
            self.token_expiration_time = datetime.fromisoformat(
                self.token_expiration_time
            )
        if not is_expired(expires_at=self.token_expiration_time):
            return
        url = f"{BASE_URLS['ACCESS_TOKEN_BASE_URL']}{ENDPOINTS.get(EndpointName.ACCESS_TOKEN.value)}"
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        data = {
            "grant_type": "refresh_token",
            "refresh_token": self.refresh_token,
            "client_id": self.app_key,
            "client_secret": self.app_secret,
        }

        self._logger.debug(
            f"Generating an access token with url: {url}, headers: {headers}"
        )
        async with aiohttp.ClientSession() as session:
            async with session.post(url=url, headers=headers, data=data) as response:
                response_data = await response.json()
                if response.status != 200:
                    self.check_errors(response=response_data)
                self.access_token = response_data["access_token"]
                self.token_expiration_time = evaluate_timedelta(
                    seconds=int(response_data["expires_in"]), time_skew=20
                )
                self._logger.debug("Access Token generated successfully")

    def check_errors(self, response):
        error_response = response.get("error")
        if error_response == "invalid_grant":
            msg = "Configured Refresh Token is invalid."
            raise InvalidRefreshTokenException(msg)
        elif error_response == "invalid_client: Invalid client_id or client_secret":
            msg = "Configured App Key or App Secret is invalid."
            raise InvalidClientCredentialException(msg)
        else:
            msg = f"Error while generating an access token. Error: {error_response}"
            raise Exception(msg)

    @cached_property
    def _get_session(self):
        self._logger.debug("Generating aiohttp client session")
        timeout = aiohttp.ClientTimeout(total=None)

        return aiohttp.ClientSession(timeout=timeout, raise_for_status=True)

    async def close(self):
        self._sleeps.cancel()
        await self._get_session.close()
        del self._get_session

    def _get_request_headers(self, file_type, url_name, **kwargs):
        kwargs = kwargs["kwargs"]
        request_headers = {
            "Authorization": f"Bearer {self.access_token}",
        }
        if file_type == FILE:
            request_headers["Dropbox-API-Arg"] = f'{{"path": "{kwargs["path"]}"}}'
        elif file_type == PAPER:
            request_headers[
                "Dropbox-API-Arg"
            ] = f'{{"path": "{kwargs["path"]}", "export_format": "markdown"}}'
        elif file_type == RECEIVED_FILE:
            request_headers["Dropbox-API-Arg"] = f'{{"url": "{kwargs["url"]}"}}'
        else:
            request_headers["Content-Type"] = "application/json"

        if self.member_id and (
            url_name
            not in [
                ENDPOINTS.get(EndpointName.MEMBERS.value),
                ENDPOINTS.get(EndpointName.MEMBERS_CONTINUE.value),
                ENDPOINTS.get(EndpointName.AUTHENTICATED_ADMIN.value),
                ENDPOINTS.get(EndpointName.TEAM_FOLDER_LIST.value),
                ENDPOINTS.get(EndpointName.TEAM_FOLDER_LIST_CONTINUE.value),
            ]
        ):
            request_headers["Dropbox-API-Select-User"] = self.member_id
        if kwargs.get("folder_id"):
            request_headers["Dropbox-API-Path-Root"] = json.dumps(
                {".tag": "namespace_id", "namespace_id": kwargs["folder_id"]}
            )
        return request_headers

    def _get_retry_after(self, retry, exception):
        self._logger.warning(
            f"Retry count: {retry} out of {self.retry_count}. Exception: {exception}"
        )
        if retry >= self.retry_count:
            raise exception
        retry += 1
        return retry, RETRY_INTERVAL**retry

    async def _handle_client_errors(self, retry, exception):
        retry, retry_seconds = self._get_retry_after(retry=retry, exception=exception)
        match exception.status:
            case 401:
                await self._set_access_token()
            # For Dropbox APIs, 409 status code is responsible for endpoint-specific errors.
            # Refer `Errors by status code` section in the documentation: https://www.dropbox.com/developers/documentation/http/documentation
            case 409:
                msg = f"Configured Path: {self.path} is invalid or not found."
                raise InvalidPathException(msg) from exception
            case 429:
                response_headers = exception.headers
                updated_response_headers = {
                    key.lower(): value for key, value in response_headers.items()
                }
                retry_seconds = int(
                    updated_response_headers.get("retry-after", DEFAULT_RETRY_AFTER)
                )
                self._logger.warning(
                    f"Rate limited by Dropbox. Retrying after {retry_seconds} seconds"
                )
            case _:
                raise
        self._logger.debug(f"Will retry in {retry_seconds} seconds")
        await self._sleeps.sleep(retry_seconds)
        return retry

    async def api_call(self, base_url, url_name, data=None, file_type=None, **kwargs):
        retry = 1
        url = parse.urljoin(base_url, url_name)
        while True:
            try:
                await self._set_access_token()
                headers = self._get_request_headers(
                    file_type=file_type, url_name=url_name, kwargs=kwargs
                )
                self._logger.debug(
                    f"Making a POST call for url: {url} with a payload: {data}"
                )
                async with self._get_session.post(
                    url=url, headers=headers, data=data
                ) as response:
                    yield response
                    break
            # These errors are handled in `check_errors` method
            except (InvalidClientCredentialException, InvalidRefreshTokenException):
                raise
            except ClientResponseError as exception:
                retry = await self._handle_client_errors(
                    retry=retry, exception=exception
                )
            except Exception as exception:
                if isinstance(
                    exception,
                    ServerDisconnectedError,
                ):
                    await self.close()
                retry, retry_seconds = self._get_retry_after(
                    retry=retry, exception=exception
                )
                await self._sleeps.sleep(retry_seconds)

    async def _paginated_api_call(
        self, base_url, breaking_field, continue_endpoint=None, **kwargs
    ):
        """Make a paginated API call for fetching Dropbox files/folders.

        Args:
            base_url (str): Base URL for dropbox APIs
            breaking_field (str): Breaking field to break the pagination
            is_shared (bool): Flag to determine the method flow. If `True` then fetching shared files, else normal files/folders.

        Yields:
            dictionary: JSON response
        """
        data = kwargs["data"]
        url_name = kwargs["url_name"]
        while True:
            try:
                async for response in self.api_call(
                    base_url=base_url,
                    url_name=url_name,
                    data=json.dumps(data),
                    folder_id=kwargs.get("folder_id"),
                ):
                    json_response = await response.json()
                    yield json_response

                    if isinstance(json_response, list):
                        return

                    # Breaking condition for pagination
                    # breaking_fields:
                    #  - "has_more" for fetching files/folders
                    #  - "cursor" for fetching shared files
                    if not json_response.get(breaking_field):
                        return

                    data = {
                        BreakingField.CURSOR.value: json_response[
                            BreakingField.CURSOR.value
                        ]
                    }
                    if continue_endpoint:
                        url_name = ENDPOINTS.get(continue_endpoint)
                    else:
                        url_name = ENDPOINTS.get(
                            EndpointName.FILES_FOLDERS_CONTINUE.value
                        )
            except Exception as exception:
                self._logger.warning(
                    f"Skipping of fetching files/folders for url: {parse.urljoin(base_url, url_name)}. Exception: {exception}"
                )
                return

    async def ping(self, endpoint):
        await anext(
            self.api_call(
                base_url=BASE_URLS["FILES_FOLDERS_BASE_URL"],
                url_name=ENDPOINTS.get(endpoint),
                data=json.dumps(None),
            )
        )

    async def check_path(self, folder_id=None):
        return await anext(
            self.api_call(
                base_url=BASE_URLS["FILES_FOLDERS_BASE_URL"],
                url_name=ENDPOINTS.get(EndpointName.CHECK_PATH.value),
                data=json.dumps({"path": self.path}),
                folder_id=folder_id,
            )
        )

    async def get_files_folders(self, path, folder_id):
        data = {
            "path": path,
            "recursive": True,
            "limit": FILE_LIMIT,
        }
        async for result in self._paginated_api_call(
            base_url=BASE_URLS["FILES_FOLDERS_BASE_URL"],
            url_name=ENDPOINTS.get(EndpointName.FILES_FOLDERS.value),
            data=data,
            breaking_field=BreakingField.HAS_MORE.value,
            folder_id=folder_id,
        ):
            yield result

    async def list_file_permission_without_batching(self, file_id):
        data = {
            "file": file_id,
            "limit": LIMIT,
        }
        async for result in self._paginated_api_call(
            continue_endpoint=EndpointName.FILE_MEMBERS_CONTINUE.value,
            base_url=BASE_URLS["FILES_FOLDERS_BASE_URL"],
            url_name=ENDPOINTS.get(EndpointName.FILE_MEMBERS.value),
            data=data,
            breaking_field=BreakingField.CURSOR.value,
        ):
            yield result

    async def list_file_permission_with_batching(self, file_ids):
        data = {
            "files": list(file_ids),
            "limit": BATCH_RESPONSE_COUNT,
        }
        async for result in self._paginated_api_call(
            base_url=BASE_URLS["FILES_FOLDERS_BASE_URL"],
            url_name=ENDPOINTS.get(EndpointName.FILE_MEMBERS_BATCH.value),
            data=data,
            breaking_field=None,
        ):
            yield result

    async def list_folder_permission(self, shared_folder_id):
        data = {
            "shared_folder_id": shared_folder_id,
            "limit": LIMIT,
        }
        async for result in self._paginated_api_call(
            continue_endpoint=EndpointName.FOLDER_MEMBERS_CONTINUE.value,
            base_url=BASE_URLS["FILES_FOLDERS_BASE_URL"],
            url_name=ENDPOINTS.get(EndpointName.FOLDER_MEMBERS.value),
            data=data,
            breaking_field=BreakingField.CURSOR.value,
        ):
            yield result

    async def get_shared_files(self):
        data = {
            "limit": LIMIT,
        }
        async for result in self._paginated_api_call(
            continue_endpoint=EndpointName.RECEIVED_FILES_CONTINUE.value,
            base_url=BASE_URLS["FILES_FOLDERS_BASE_URL"],
            url_name=ENDPOINTS.get(EndpointName.RECEIVED_FILES.value),
            data=data,
            breaking_field=BreakingField.CURSOR.value,
        ):
            yield result

    async def get_received_file_metadata(self, url):
        data = {"url": url}
        async for response in self.api_call(
            base_url=BASE_URLS["FILES_FOLDERS_BASE_URL"],
            url_name=ENDPOINTS.get(EndpointName.RECEIVED_FILE_METADATA.value),
            data=json.dumps(data),
        ):
            yield response

    async def download_files(self, path, folder_id):
        async for response in self.api_call(
            base_url=BASE_URLS["DOWNLOAD_BASE_URL"],
            url_name=ENDPOINTS.get(EndpointName.DOWNLOAD.value),
            file_type=FILE,
            path=path,
            folder_id=folder_id,
        ):
            yield response

    async def download_shared_file(self, url):
        async for response in self.api_call(
            base_url=BASE_URLS["DOWNLOAD_BASE_URL"],
            url_name=ENDPOINTS.get(EndpointName.RECEIVED_FILE_DOWNLOAD.value),
            file_type=RECEIVED_FILE,
            url=url,
        ):
            yield response

    async def download_paper_files(self, path, folder_id=None):
        async for response in self.api_call(
            base_url=BASE_URLS["DOWNLOAD_BASE_URL"],
            url_name=ENDPOINTS.get(EndpointName.PAPER_FILE_DOWNLOAD.value),
            file_type=PAPER,
            path=path,
            folder_id=folder_id,
        ):
            yield response

    async def search_files_folders(self, rule):
        async for result in self._paginated_api_call(
            base_url=BASE_URLS["FILES_FOLDERS_BASE_URL"],
            url_name=ENDPOINTS.get(EndpointName.SEARCH.value),
            data=rule,
            breaking_field=BreakingField.HAS_MORE.value,
            continue_endpoint=EndpointName.SEARCH_CONTINUE.value,
        ):
            yield result

    async def list_members(self):
        data = {
            "limit": LIMIT,
        }
        async for result in self._paginated_api_call(
            continue_endpoint=EndpointName.MEMBERS_CONTINUE.value,
            base_url=BASE_URLS["FILES_FOLDERS_BASE_URL"],
            url_name=ENDPOINTS.get(EndpointName.MEMBERS.value),
            data=data,
            breaking_field=BreakingField.HAS_MORE.value,
        ):
            yield result

    async def get_team_folder_list(self):
        data = {
            "limit": LIMIT,
        }
        async for result in self._paginated_api_call(
            continue_endpoint=EndpointName.TEAM_FOLDER_LIST_CONTINUE.value,
            base_url=BASE_URLS["FILES_FOLDERS_BASE_URL"],
            url_name=ENDPOINTS.get(EndpointName.TEAM_FOLDER_LIST.value),
            data=data,
            breaking_field=BreakingField.HAS_MORE.value,
        ):
            yield result


class DropBoxAdvancedRulesValidator(AdvancedRulesValidator):
    FILE_CATEGORY_DEFINITION = {
        "type": "object",
        "properties": {
            ".tag": {"type": "string", "minLength": 1},
        },
    }

    OPTIONS_DEFINITION = {
        "type": "object",
        "properties": {
            "path": {"type": "string", "minLength": 1},
            "max_results": {"type": "string", "minLength": 1},
            "order_by": FILE_CATEGORY_DEFINITION,
            "file_status": FILE_CATEGORY_DEFINITION,
            "account_id": {"type": "string", "minLength": 1},
            "file_extensions": {
                "type": "array",
                "items": {"type": "string"},
                "minItems": 1,
            },
            "file_categories": {
                "type": "array",
                "items": FILE_CATEGORY_DEFINITION,
                "minItems": 1,
            },
        },
    }

    RULES_OBJECT_SCHEMA_DEFINITION = {
        "type": "object",
        "properties": {
            "match_field_options": {"type": "object"},
            "options": OPTIONS_DEFINITION,
            "query": {"type": "string", "minLength": 1},
        },
        "required": ["query"],
        "additionalProperties": False,
    }

    SCHEMA_DEFINITION = {"type": "array", "items": RULES_OBJECT_SCHEMA_DEFINITION}

    SCHEMA = fastjsonschema.compile(definition=SCHEMA_DEFINITION)

    def __init__(self, source):
        self.source = source

    async def validate(self, advanced_rules):
        if len(advanced_rules) == 0:
            return SyncRuleValidationResult.valid_result(
                SyncRuleValidationResult.ADVANCED_RULES
            )

        return await self._remote_validation(advanced_rules)

    @retryable(
        retries=RETRY_COUNT,
        interval=RETRY_INTERVAL,
        strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
    )
    async def _remote_validation(self, advanced_rules):
        try:
            DropBoxAdvancedRulesValidator.SCHEMA(advanced_rules)
        except fastjsonschema.JsonSchemaValueException as e:
            return SyncRuleValidationResult(
                rule_id=SyncRuleValidationResult.ADVANCED_RULES,
                is_valid=False,
                validation_message=e.message,
            )

        invalid_paths = []
        for rule in advanced_rules:
            self.source.dropbox_client.path = rule.get("options", {}).get("path")
            try:
                if self.source.dropbox_client.path:
                    await self.source.dropbox_client.check_path()
            except InvalidPathException:
                invalid_paths.append(self.source.dropbox_client.path)

        if len(invalid_paths) > 0:
            return SyncRuleValidationResult(
                SyncRuleValidationResult.ADVANCED_RULES,
                is_valid=False,
                validation_message=f"Invalid paths '{', '.join(invalid_paths)}'.",
            )
        return SyncRuleValidationResult.valid_result(
            SyncRuleValidationResult.ADVANCED_RULES
        )


class DropboxDataSource(BaseDataSource):
    """Dropbox"""

    name = "Dropbox"
    service_type = "dropbox"
    advanced_rules_enabled = True
    dls_enabled = True
    incremental_sync_enabled = True

    def __init__(self, configuration):
        """Setup the connection to the Dropbox

        Args:
            configuration (DataSourceConfiguration): Object of DataSourceConfiguration class.
        """
        super().__init__(configuration=configuration)
        self.dropbox_client = DropboxClient(configuration=configuration)
        self.concurrent_downloads = self.configuration["concurrent_downloads"]
        self.include_inherited_users_and_groups = self.configuration[
            "include_inherited_users_and_groups"
        ]

    def _set_internal_logger(self):
        self.dropbox_client.set_logger(self._logger)

    @classmethod
    def get_default_configuration(cls):
        """Get the default configuration for Dropbox

        Returns:
            dictionary: Default configuration.
        """
        return {
            "path": {
                "label": "Path to fetch files/folders",
                "order": 1,
                "required": False,
                "tooltip": "Path is ignored when Advanced Sync Rules are used.",
                "type": "str",
            },
            "app_key": {
                "label": "App Key",
                "sensitive": True,
                "order": 2,
                "type": "str",
            },
            "app_secret": {
                "label": "App secret",
                "sensitive": True,
                "order": 3,
                "type": "str",
            },
            "refresh_token": {
                "label": "Refresh token",
                "sensitive": True,
                "order": 4,
                "type": "str",
            },
            "retry_count": {
                "default_value": RETRY_COUNT,
                "display": "numeric",
                "label": "Retries per request",
                "order": 5,
                "required": False,
                "type": "int",
                "ui_restrictions": ["advanced"],
            },
            "concurrent_downloads": {
                "default_value": MAX_CONCURRENT_DOWNLOADS,
                "display": "numeric",
                "label": "Maximum concurrent downloads",
                "order": 6,
                "required": False,
                "type": "int",
                "ui_restrictions": ["advanced"],
            },
            "use_text_extraction_service": {
                "display": "toggle",
                "label": "Use text extraction service",
                "order": 7,
                "tooltip": "Requires a separate deployment of the Elastic Text Extraction Service. Requires that pipeline settings disable text extraction.",
                "type": "bool",
                "ui_restrictions": ["advanced"],
                "value": False,
            },
            "use_document_level_security": {
                "display": "toggle",
                "label": "Enable document level security",
                "order": 8,
                "tooltip": "Document level security ensures identities and permissions set in Dropbox are maintained in Elasticsearch. This enables you to restrict and personalize read-access users and groups have to documents in this index. Access control syncs ensure this metadata is kept up to date in your Elasticsearch documents.",
                "type": "bool",
                "value": False,
            },
            "include_inherited_users_and_groups": {
                "depends_on": [{"field": "use_document_level_security", "value": True}],
                "display": "toggle",
                "label": "Include groups and inherited users",
                "order": 9,
                "tooltip": "Include groups and inherited users when indexing permissions. Enabling this configurable field will cause a significant performance degradation.",
                "type": "bool",
                "value": False,
            },
        }

    def _dls_enabled(self):
        """Check if document level security is enabled. This method checks whether document level security (DLS) is enabled based on the provided configuration.
        Returns:
            bool: True if document level security is enabled, False otherwise.
        """
        if (
            self._features is None
            or not self._features.document_level_security_enabled()
        ):
            return False

        return self.configuration["use_document_level_security"]

    def _decorate_with_access_control(self, document, access_control):
        if self._dls_enabled():
            document[ACCESS_CONTROL] = list(
                set(document.get(ACCESS_CONTROL, []) + access_control)
            )
        return document

    async def _user_access_control_doc(self, user):
        profile = user.get("profile", {})
        email = profile.get("email")
        username = profile.get("name", {}).get("display_name")

        prefixed_email = _prefix_email(email)
        prefixed_username = _prefix_user(username)
        prefixed_user_id = _prefix_user_id(profile.get("account_id"))

        prefixed_groups = set()
        for group_id in profile.get("groups", []):
            prefixed_groups.add(_prefix_group(group_id))

        access_control = list(
            {prefixed_email, prefixed_username, prefixed_user_id}.union(prefixed_groups)
        )
        return {
            "_id": email,
            "identity": {
                "email": prefixed_email,
                "username": prefixed_username,
                "user_id": prefixed_user_id,
            },
            "status": profile.get("status", {}).get(".tag"),
            "created_at": profile.get("joined_on", iso_utc()),
        } | es_access_control_query(access_control)

    async def get_access_control(self):
        if not self._dls_enabled():
            self._logger.warning("DLS is not enabled. Skipping")
            return

        self._logger.info("Fetching users for Access Control sync")
        async for users in self.dropbox_client.list_members():
            for user in users.get("members", []):
                yield await self._user_access_control_doc(user=user)

    async def validate_config(self):
        """Validates whether user input is empty or not for configuration fields
        Also validate, if user configured path is available in Dropbox."""

        await super().validate_config()
        await self._remote_validation()

    async def _remote_validation(self):
        try:
            if self.dropbox_client.path not in ["", None]:
                if self._dls_enabled():
                    _, member_id = await self.get_account_details()
                    self.dropbox_client.member_id = member_id
                    async for folder_id in self.get_team_folder_id():
                        await self.dropbox_client.check_path(folder_id=folder_id)
                else:
                    await self.dropbox_client.check_path()
        except InvalidPathException:
            raise
        except Exception as exception:
            msg = f"Error while validating path: {self.dropbox_client.path}. Error: {exception}"
            raise Exception(msg) from exception

    def advanced_rules_validators(self):
        return [DropBoxAdvancedRulesValidator(self)]

    def tweak_bulk_options(self, options):
        """Tweak bulk options as per concurrent downloads support by dropbox

        Args:
            options (dictionary): Config bulker options
        """
        options["concurrent_downloads"] = self.concurrent_downloads

    async def close(self):
        await self.dropbox_client.close()

    async def ping(self):
        endpoint = EndpointName.PING.value
        if self._dls_enabled():
            endpoint = EndpointName.AUTHENTICATED_ADMIN.value
        await self.dropbox_client.ping(endpoint=endpoint)
        self._logger.info("Successfully connected to Dropbox")

    async def get_content(
        self, attachment, is_shared=False, folder_id=None, timestamp=None, doit=False
    ):
        """Extracts the content for allowed file types.

        Args:
            attachment (object): Attachment object
            is_shared (boolean, optional): Flag to check if want a content for shared file. Defaults to False.
            timestamp (timestamp, optional): Timestamp of attachment last modified. Defaults to None.
            doit (boolean, optional): Boolean value for whether to get content or not. Defaults to False.

        Returns:
            dictionary: Content document with _id, _timestamp and attachment content
        """
        file_size = int(attachment["size"])
        if not (doit and file_size > 0):
            return

        filename = attachment["name"]
        file_extension = self.get_file_extension(filename)
        if not self.can_file_be_downloaded(
            file_extension,
            filename,
            file_size,
        ):
            return

        download_func = self.download_func(is_shared, attachment, filename, folder_id)
        if not download_func:
            self._logger.warning(
                f"Skipping file '{filename}' since it is not downloadable."
            )
            return

        self._logger.debug(f"Downloading content for file: {filename}")
        document = {
            "_id": attachment["id"],
            "_timestamp": attachment["server_modified"],
        }
        return await self.download_and_extract_file(
            document,
            filename,
            file_extension,
            partial(
                self.generic_chunked_download_func,
                download_func,
            ),
        )

    def download_func(self, is_shared, attachment, filename, folder_id):
        if is_shared:
            return partial(
                self.dropbox_client.download_shared_file, url=attachment["url"]
            )
        elif attachment["is_downloadable"]:
            return partial(
                self.dropbox_client.download_files,
                path=attachment["path_display"],
                folder_id=folder_id,
            )
        elif filename.split(".")[-1] == PAPER:
            return partial(
                self.dropbox_client.download_paper_files,
                path=attachment["path_display"],
                folder_id=folder_id,
            )
        else:
            return

    def _adapt_dropbox_doc_to_es_doc(self, response):
        is_file = response.get(".tag") == "file"
        if is_file and response.get("name").split(".")[-1] == PAPER:
            timestamp = response.get("client_modified")
        else:
            timestamp = response.get("server_modified") if is_file else iso_utc()
        return {
            "_id": response.get("id"),
            "type": FILE if is_file else FOLDER,
            "name": response.get("name"),
            "file_path": response.get("path_display"),
            "size": response.get("size") if is_file else 0,
            "_timestamp": timestamp,
        }

    def _adapt_dropbox_shared_file_doc_to_es_doc(self, response):
        return {
            "_id": response.get("id"),
            "type": FILE,
            "name": response.get("name"),
            "url": response.get("url"),
            "size": response.get("size"),
            "_timestamp": response.get("server_modified"),
        }

    async def _fetch_files_folders(self, path, folder_id=None):
        self._logger.info(f"Fetching files and folders from path: '{path}'")
        async for response in self.dropbox_client.get_files_folders(
            path=path, folder_id=folder_id
        ):
            for entry in response.get("entries"):
                yield self._adapt_dropbox_doc_to_es_doc(response=entry), entry

    async def _fetch_shared_files(self):
        self._logger.info("Fetching shared files")
        async for response in self.dropbox_client.get_shared_files():
            for entry in response.get("entries"):
                async for metadata in self.dropbox_client.get_received_file_metadata(
                    url=entry["preview_url"]
                ):
                    json_metadata = await metadata.json()
                    yield self._adapt_dropbox_shared_file_doc_to_es_doc(
                        response=json_metadata
                    ), json_metadata

    async def advanced_sync(self, rule):
        self._logger.debug(f"Fetching files/folders for sync rule: {rule}")
        async for response in self.dropbox_client.search_files_folders(rule=rule):
            for entry in response.get("matches"):
                data = entry.get("metadata", {}).get("metadata")
                if data.get("preview_url") and not data.get("export_info"):
                    async for metadata in self.dropbox_client.get_received_file_metadata(
                        url=data["preview_url"]
                    ):
                        json_metadata = await metadata.json()
                        yield self._adapt_dropbox_shared_file_doc_to_es_doc(
                            response=json_metadata
                        ), json_metadata
                else:
                    yield self._adapt_dropbox_doc_to_es_doc(response=data), data

    def get_group_id(self, permission, identity):
        if identity in permission:
            return permission.get(identity).get("group_id")

    def get_email(self, permission, identity):
        if identity in permission:
            return permission.get(identity).get("email")

    async def get_permission(self, permission, account_id):
        permissions = []
        if identities := permission.get("users"):
            for identity in identities:
                permissions.append(
                    _prefix_user_id(identity.get("user", {}).get("account_id"))
                )

        if identities := permission.get("invitees"):
            for identity in identities:
                if invitee_permission := self.get_email(identity, "invitee"):
                    permissions.append(_prefix_email(invitee_permission))

        if identities := permission.get("groups"):
            for identity in identities:
                if group_permission := self.get_group_id(identity, "group"):
                    permissions.append(_prefix_group(group_permission))

        if (
            not self.include_inherited_users_and_groups
            and account_id not in permissions
        ):
            permissions.append(_prefix_user_id(account_id))
        return permissions

    async def get_folder_permission(self, shared_folder_id, account_id):
        self._logger.debug(f"Fetching permissions for folder: {shared_folder_id}")
        if not shared_folder_id:
            return [account_id]

        async for permission in self.dropbox_client.list_folder_permission(
            shared_folder_id=shared_folder_id
        ):
            return await self.get_permission(
                permission=permission, account_id=account_id
            )

    async def get_file_permission_without_batching(self, file_id, account_id):
        self._logger.debug(f"Fetching permissions for file: {file_id}")
        async for permission in self.dropbox_client.list_file_permission_without_batching(
            file_id=file_id
        ):
            return await self.get_permission(
                permission=permission, account_id=account_id
            )

    async def get_account_details(self):
        self._logger.debug("Fetching account details")
        response = await anext(
            self.dropbox_client.api_call(
                base_url=BASE_URLS["FILES_FOLDERS_BASE_URL"],
                url_name=ENDPOINTS.get(EndpointName.AUTHENTICATED_ADMIN.value),
                data=json.dumps(None),
            )
        )
        account_details = await response.json()
        account_id = account_details.get("admin_profile", {}).get("account_id")
        member_id = account_details.get("admin_profile", {}).get("team_member_id")
        return account_id, member_id

    async def get_permission_list(self, item_type, item, account_id):
        self._logger.debug(f"Fetching permissions for {item_type}")
        if item_type == FOLDER:
            shared_folder_id = item.get("shared_folder_id") or item.get(
                "parent_shared_folder_id"
            )
            return await self.get_folder_permission(
                shared_folder_id=shared_folder_id, account_id=account_id
            )

        return await self.get_file_permission_without_batching(
            file_id=item.get("id"), account_id=account_id
        )

    async def get_team_folder_id(self):
        async for folder_list in self.dropbox_client.get_team_folder_list():
            for folder in folder_list.get("team_folders", []):
                yield folder.get("team_folder_id")

    async def map_permission_with_document(self, batched_document, account_id):
        async for permissions in self.dropbox_client.list_file_permission_with_batching(
            file_ids=batched_document.keys()
        ):
            for permission in permissions:
                file_permission = (
                    await self.get_permission(
                        permission=permission["result"]["members"],
                        account_id=account_id,
                    )
                    if permission.get("result", {}).get("members", {})
                    else []
                )
                file_id = batched_document[permission["file"]]

                yield self._decorate_with_access_control(
                    file_id[0], file_permission
                ), file_id[1]

    def document_tuple(self, document, attachment, folder_id=None):
        if document.get("type") == FILE:
            if document.get("url"):
                return document, partial(
                    self.get_content,
                    attachment=attachment,
                    is_shared=True,
                    folder_id=folder_id,
                )
            else:
                return document, partial(
                    self.get_content, attachment=attachment, folder_id=folder_id
                )
        else:
            return document, None

    async def add_document_to_list(self, func, account_id, folder_id, is_shared=False):
        batched_document = {}
        calling_func = (
            func()
            if is_shared
            else func(path=self.dropbox_client.path, folder_id=folder_id)
        )

        async for document, attachment in calling_func:
            if (
                self.include_inherited_users_and_groups is True
                or document.get("type") == FOLDER
            ):
                permissions = await self.get_permission_list(
                    item_type=document.get("type"),
                    item=attachment,
                    account_id=account_id,
                )
                if permissions is None:
                    permissions = []
                yield self.document_tuple(
                    document=self._decorate_with_access_control(document, permissions),
                    attachment=attachment,
                    folder_id=folder_id,
                )
            else:
                if len(batched_document) == REQUEST_BATCH_SIZE:
                    async for mapped_document, mapped_attachment in self.map_permission_with_document(
                        batched_document=batched_document, account_id=account_id
                    ):
                        yield self.document_tuple(
                            document=mapped_document,
                            attachment=mapped_attachment,
                            folder_id=folder_id,
                        )
                    batched_document = {attachment["id"]: (document, attachment)}
                else:
                    batched_document[attachment["id"]] = (document, attachment)

        if len(batched_document) > 0:
            async for mapped_document, mapped_attachment in self.map_permission_with_document(
                batched_document=batched_document, account_id=account_id
            ):
                yield self.document_tuple(
                    document=mapped_document,
                    attachment=mapped_attachment,
                    folder_id=folder_id,
                )
            batched_document = {}

    async def fetch_file_folders_with_dls(self):
        self._logger.info("Fetching permissions for files and folders")
        account_id, member_id = await self.get_account_details()
        self.dropbox_client.member_id = member_id
        async for folder_id in self.get_team_folder_id():
            self._logger.info(f"Iterating through folder with id '{folder_id}'")
            async for mapped_document in self.add_document_to_list(
                func=self._fetch_files_folders,
                account_id=account_id,
                folder_id=folder_id,
            ):
                yield mapped_document
            async for mapped_document in self.add_document_to_list(
                func=self._fetch_shared_files,
                account_id=account_id,
                folder_id=folder_id,
                is_shared=True,
            ):
                yield mapped_document

    async def get_docs(self, filtering=None):
        """Executes the logic to fetch dropbox objects in async manner

        Args:
            filtering (Filtering): Object of class Filtering

        Yields:
            dictionary: dictionary containing meta-data of the files.
        """

        if self._dls_enabled():
            async for document in self.fetch_file_folders_with_dls():
                yield document

        elif filtering and filtering.has_advanced_rules():
            advanced_rules = filtering.get_advanced_rules()
            self._logger.debug(
                f"Fetching documents using configured advanced sync rules: {advanced_rules}"
            )
            for rule in advanced_rules:
                async for document, attachment in self.advanced_sync(rule=rule):
                    yield self.document_tuple(document=document, attachment=attachment)
        else:
            async for document, attachment in self._fetch_files_folders(
                path=self.dropbox_client.path
            ):
                yield self.document_tuple(document=document, attachment=attachment)

            async for document, attachment in self._fetch_shared_files():
                yield self.document_tuple(document=document, attachment=attachment)
