#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""GitHub source module responsible to fetch documents from GitHub Cloud and Server."""

import json
import time
from enum import Enum
from functools import cached_property, partial

import aiohttp
import fastjsonschema
from aiohttp.client_exceptions import ClientResponseError
from gidgethub import QueryError, RateLimitExceeded, sansio
from gidgethub.abc import (
    BadGraphQLRequest,
    GraphQLAuthorizationFailure,
)
from gidgethub.aiohttp import GitHubAPI
from gidgethub.apps import get_installation_access_token, get_jwt

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
from connectors.source import BaseDataSource, ConfigurableFieldValueError
from connectors.utils import (
    CancellableSleeps,
    RetryStrategy,
    decode_base64_value,
    nested_get_from_dict,
    retryable,
    ssl_context,
)

WILDCARD = "*"
BLOB = "blob"
FILE = "file"
GITHUB_CLOUD = "github_cloud"
GITHUB_SERVER = "github_server"
PERSONAL_ACCESS_TOKEN = "personal_access_token"  # noqa: S105
GITHUB_APP = "github_app"
PULL_REQUEST_OBJECT = "pullRequest"
REPOSITORY_OBJECT = "repository"

RETRIES = 3
RETRY_INTERVAL = 2
FORBIDDEN = 403
UNAUTHORIZED = 401
NODE_SIZE = 100
REVIEWS_COUNT = 45

SUPPORTED_EXTENSION = [".markdown", ".md", ".rst"]

FILE_SCHEMA = {
    "name": "name",
    "size": "size",
    "type": "type",
    "path": "path",
    "mode": "mode",
    "extension": "extension",
    "_timestamp": "_timestamp",
}
PATH_SCHEMA = {
    "name": "name",
    "size": "size",
    "type": "type",
    "path": "path",
    "extension": "extension",
    "_timestamp": "_timestamp",
}


def _prefix_email(email):
    return prefix_identity("email", email)


def _prefix_username(user):
    return prefix_identity("username", user)


def _prefix_user_id(user_id):
    return prefix_identity("user_id", user_id)


class GithubQuery(Enum):
    USER_QUERY = """
        query {
        viewer {
            login
        }
    }
    """
    REPOS_QUERY = f"""
    query ($login: String!, $cursor: String) {{
    user(login: $login) {{
        repositories(first: {NODE_SIZE}, after: $cursor) {{
        pageInfo {{
            hasNextPage
            endCursor
        }}
        nodes {{
            id
            updatedAt
            name
            nameWithOwner
            url
            description
            visibility
            primaryLanguage {{
            name
            }}
            defaultBranchRef {{
            name
            }}
            isFork
            stargazerCount
            watchers {{
            totalCount
            }}
            forkCount
            createdAt
            isArchived
        }}
        }}
    }}
    }}
    """
    ORG_REPOS_QUERY = f"""
    query ($orgName: String!, $cursor: String) {{
    organization(login: $orgName) {{
        repositories(first: {NODE_SIZE}, after: $cursor) {{
        pageInfo {{
            hasNextPage
            endCursor
        }}
        nodes {{
            id
            updatedAt
            name
            nameWithOwner
            url
            description
            visibility
            primaryLanguage {{
            name
            }}
            defaultBranchRef {{
            name
            }}
            isFork
            stargazerCount
            watchers {{
            totalCount
            }}
            forkCount
            createdAt
            isArchived
        }}
        }}
    }}
    }}
    """
    REPO_QUERY = """
    query ($owner: String!, $repositoryName: String!) {
    repository(owner: $owner, name: $repositoryName) {
        id
        updatedAt
        name
        nameWithOwner
        url
        description
        visibility
        primaryLanguage {
        name
        }
        defaultBranchRef {
        name
        }
        isFork
        stargazerCount
        watchers {
        totalCount
        }
        forkCount
        createdAt
        isArchived
    }
    }
    """
    PULL_REQUEST_QUERY = f"""
    query ($owner: String!, $name: String!, $cursor: String) {{
    repository(owner: $owner, name: $name) {{
        pullRequests(first: {NODE_SIZE}, after: $cursor) {{
        pageInfo {{
            hasNextPage
            endCursor
        }}
        nodes {{
            id
            updatedAt
            number
            url
            createdAt
            closedAt
            title
            body
            state
            mergedAt
            author {{
            login
            }}
            assignees(first: {NODE_SIZE}) {{
            pageInfo {{
                hasNextPage
                endCursor
            }}
            nodes {{
                login
            }}
            }}
            labels(first: {NODE_SIZE}) {{
            pageInfo {{
                hasNextPage
                endCursor
            }}
            nodes {{
                name
                description
            }}
            }}
            reviewRequests(first: {NODE_SIZE}) {{
            pageInfo {{
                hasNextPage
                endCursor
            }}
            nodes {{
                requestedReviewer {{
                ... on User {{
                    login
                }}
                }}
            }}
            }}
            comments(first: {NODE_SIZE}) {{
            pageInfo {{
                hasNextPage
                endCursor
            }}
            nodes {{
                author {{
                login
                }}
                body
            }}
            }}
            reviews(first: {REVIEWS_COUNT}) {{
            pageInfo {{
                hasNextPage
                endCursor
            }}
            nodes {{
                id
                author {{
                login
                }}
                state
                body
                comments(first: {NODE_SIZE}) {{
                pageInfo {{
                    hasNextPage
                    endCursor
                }}
                nodes {{
                    body
                }}
                }}
            }}
            }}
        }}
        }}
    }}
    }}
    """
    ISSUE_QUERY = f"""
    query ($owner: String!, $name: String!, $cursor: String) {{
    repository(owner: $owner, name: $name) {{
        issues(first: {NODE_SIZE}, after: $cursor) {{
        pageInfo {{
            hasNextPage
            endCursor
        }}
        nodes {{
            id
            updatedAt
            number
            url
            createdAt
            closedAt
            title
            body
            state
            author {{
            login
            }}
            assignees(first: {NODE_SIZE}) {{
            pageInfo {{
                hasNextPage
                endCursor
            }}
            nodes {{
                login
            }}
            }}
            labels(first: {NODE_SIZE}) {{
            pageInfo {{
                hasNextPage
                endCursor
            }}
            nodes {{
                name
                description
            }}
            }}
            comments(first: {NODE_SIZE}) {{
            pageInfo {{
                hasNextPage
                endCursor
            }}
            nodes {{
                author {{
                login
                }}
                body
            }}
            }}
        }}
        }}
    }}
    }}
    """
    COMMENT_QUERY = """
    query ($owner: String!, $name: String!, $number: Int!, $cursor: String) {{
    repository(owner: $owner, name: $name) {{
        {object_type}(number: $number) {{
        comments(first: {node_size}, after: $cursor) {{
            pageInfo {{
            hasNextPage
            endCursor
            }}
            nodes {{
            author {{
                login
            }}
            body
            }}
        }}
        }}
    }}
    }}
    """
    REVIEW_QUERY = f"""
    query ($owner: String!, $name: String!, $number: Int!, $cursor: String) {{
    repository(owner: $owner, name: $name) {{
        pullRequest(number: $number) {{
        reviews(first: {NODE_SIZE}, after: $cursor) {{
            pageInfo {{
            hasNextPage
            endCursor
            }}
            nodes {{
            id
            author {{
                login
            }}
            state
            body
            comments(first: {NODE_SIZE}) {{
                pageInfo {{
                hasNextPage
                endCursor
                }}
                nodes {{
                body
                }}
            }}
            }}
        }}
        }}
    }}
    }}
    """
    REVIEWERS_QUERY = f"""
    query ($owner: String!, $name: String!, $number: Int!, $cursor: String) {{
    repository(owner: $owner, name: $name) {{
        pullRequest(number: $number) {{
        reviewRequests(first: {NODE_SIZE}, after: $cursor) {{
            pageInfo {{
            hasNextPage
            endCursor
            }}
            nodes {{
                requestedReviewer {{
                    ... on User {{
                    login
                }}
            }}
            }}
        }}
        }}
    }}
    }}
    """
    LABELS_QUERY = """
    query ($owner: String!, $name: String!, $number: Int!, $cursor: String) {{
    repository(owner: $owner, name: $name) {{
        {object_type}(number: $number) {{
        labels(first: {node_size}, after: $cursor) {{
            pageInfo {{
            hasNextPage
            endCursor
            }}
            nodes {{
            name
            }}
        }}
        }}
    }}
    }}
    """
    ASSIGNEES_QUERY = """
    query ($owner: String!, $name: String!, $number: Int!, $cursor: String) {{
    repository(owner: $owner, name: $name) {{
        {object_type}(number: $number) {{
        assignees(first: {node_size}, after: $cursor) {{
            pageInfo {{
            hasNextPage
            endCursor
            }}
            nodes {{
            login
            }}
        }}
        }}
    }}
    }}
    """
    SEARCH_QUERY = f"""
    query ($filter_query: String!, $cursor: String) {{
    search(query: $filter_query, type: ISSUE, first: {NODE_SIZE}, after: $cursor) {{
        pageInfo {{
        hasNextPage
        endCursor
        }}
        nodes {{
        ... on Issue {{
            id
            updatedAt
            number
            url
            createdAt
            closedAt
            title
            body
            state
            assignees(first: {NODE_SIZE}) {{
            pageInfo {{
                hasNextPage
                endCursor
            }}
            nodes {{
                login
            }}
            }}
            labels(first: {NODE_SIZE}) {{
            pageInfo {{
                hasNextPage
                endCursor
            }}
            nodes {{
                name
                description
            }}
            }}
            comments(first: {NODE_SIZE}) {{
            pageInfo {{
                hasNextPage
                endCursor
            }}
            nodes {{
                author {{
                login
                }}
                body
            }}
            }}
        }}
        ... on PullRequest {{
            id
            updatedAt
            number
            url
            createdAt
            closedAt
            title
            body
            state
            mergedAt
            assignees(first: {NODE_SIZE}) {{
            pageInfo {{
                hasNextPage
                endCursor
            }}
            nodes {{
                login
            }}
            }}
            labels(first: {NODE_SIZE}) {{
            pageInfo {{
                hasNextPage
                endCursor
            }}
            nodes {{
                name
                description
            }}
            }}
            reviewRequests(first: {NODE_SIZE}) {{
            pageInfo {{
                hasNextPage
                endCursor
            }}
            nodes {{
                requestedReviewer {{
                ... on User {{
                    login
                }}
                }}
            }}
            }}
            comments(first: {NODE_SIZE}) {{
            pageInfo {{
                hasNextPage
                endCursor
            }}
            nodes {{
                author {{
                login
                }}
                body
            }}
            }}
            reviews(first: {REVIEWS_COUNT}) {{
            pageInfo {{
                hasNextPage
                endCursor
            }}
            nodes {{
                id
                author {{
                login
                }}
                state
                body
                comments(first: {NODE_SIZE}) {{
                pageInfo {{
                    hasNextPage
                    endCursor
                }}
                nodes {{
                    body
                }}
                }}
            }}
            }}
        }}
        }}
    }}
    }}
    """
    ORG_MEMBERS_QUERY = f"""
    query ($orgName: String!, $cursor: String) {{
    organization(login: $orgName) {{
        membersWithRole(first: {NODE_SIZE}, after: $cursor) {{
        pageInfo {{
            hasNextPage
            endCursor
        }}
        edges {{
            node {{
                id
                login
                name
                email
                updatedAt
            }}
        }}
        }}
    }}
    }}
    """
    COLLABORATORS_QUERY = f"""
    query ($orgName: String!, $repoName: String!, $cursor: String) {{
    repository(owner: $orgName, name: $repoName) {{
        collaborators(first: {NODE_SIZE}, after: $cursor) {{
        pageInfo {{
            hasNextPage
            endCursor
        }}
        edges {{
            node {{
                id
                login
                email
            }}
        }}
        }}
    }}
    }}
    """


class ObjectType(Enum):
    REPOSITORY = "Repository"
    ISSUE = "Issue"
    PULL_REQUEST = "Pull request"
    PR = "pr"
    BRANCH = "branch"
    PATH = "path"


class UnauthorizedException(Exception):
    pass


class NoInstallationAccessTokenException(Exception):
    pass


class ForbiddenException(Exception):
    pass


class GitHubClient:
    def __init__(
        self, auth_method, base_url, app_id, private_key, token, ssl_enabled, ssl_ca
    ):
        self._sleeps = CancellableSleeps()
        self._logger = logger
        self.auth_method = auth_method
        self.base_url = base_url
        self.app_id = app_id if self.auth_method == GITHUB_APP else None
        self.private_key = private_key if self.auth_method == GITHUB_APP else None
        self._personal_access_token = (
            token if self.auth_method == PERSONAL_ACCESS_TOKEN else None
        )
        self._installation_access_token = None

        if self.base_url == "https://api.github.com":
            self.endpoints = {
                "TREE": "/repos/{repo_name}/git/trees/{default_branch}?recursive=1",
                "COMMITS": "/repos/{repo_name}/commits?path={path}",
                "PATH": "/repos/{repo_name}/contents/{path}",
            }
        else:
            self.endpoints = {
                "TREE": "/api/v3/repos/{repo_name}/git/trees/{default_branch}?recursive=1",
                "COMMITS": "api/v3/repos/{repo_name}/commits?path={path}",
                "PATH": "api/v3/repos/{repo_name}/contents/{path}",
            }
        if ssl_enabled and ssl_ca:
            self.ssl_ctx = ssl_context(certificate=ssl_ca)
        else:
            self.ssl_ctx = False

        # a variable to hold the current installation id, used to refresh the access token
        self._installation_id = None

    def set_logger(self, logger_):
        self._logger = logger_

    def get_rate_limit_encountered(self, status_code, rate_limit_remaining):
        return status_code == FORBIDDEN and not int(rate_limit_remaining)

    async def _get_retry_after(self, resource_type):
        current_time = time.time()
        response = await self.get_github_item("/rate_limit")
        reset = nested_get_from_dict(
            response, ["resources", resource_type, "reset"], default=current_time
        )
        # Adding a 5 second delay to account for server delays
        return (reset - current_time) + 5  # pyright: ignore

    async def _put_to_sleep(self, resource_type):
        retry_after = await self._get_retry_after(resource_type=resource_type)
        self._logger.debug(
            f"Rate limit exceeded. Retry after {retry_after} seconds. Resource type: {resource_type}"
        )
        await self._sleeps.sleep(retry_after)
        msg = "Rate limit exceeded."
        raise Exception(msg)

    def _access_token(self):
        if self.auth_method == PERSONAL_ACCESS_TOKEN:
            return self._personal_access_token
        if not self._installation_access_token:
            raise NoInstallationAccessTokenException
        return self._installation_access_token

    # update the current installation id and re-generate access token
    async def update_installation_id(self, installation_id):
        self._logger.debug(
            f"Updating installation id - new ID: {installation_id}, original ID: {self._installation_id}"
        )
        self._installation_id = installation_id
        await self._update_installation_access_token()

    @retryable(
        retries=RETRIES,
        interval=RETRY_INTERVAL,
        strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
    )
    async def _update_installation_access_token(self):
        try:
            access_token_response = await get_installation_access_token(
                gh=self._get_client,
                installation_id=self._installation_id,
                app_id=self.app_id,
                private_key=self.private_key,
            )
            self._installation_access_token = access_token_response["token"]
        except RateLimitExceeded:
            await self._put_to_sleep("core")
        except Exception:
            self._logger.exception(
                f"Failed to get access token for installation {self._installation_id}.",
                exc_info=True,
            )
            raise

    @cached_property
    def _get_session(self):
        connector = aiohttp.TCPConnector(ssl=self.ssl_ctx)
        timeout = aiohttp.ClientTimeout(total=None)
        return aiohttp.ClientSession(
            timeout=timeout,
            raise_for_status=True,
            connector=connector,
        )

    @cached_property
    def _get_client(self):
        return GitHubAPI(
            session=self._get_session,
            requester="",
            base_url=self.base_url,
        )

    @retryable(
        retries=RETRIES,
        interval=RETRY_INTERVAL,
        strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
        skipped_exceptions=UnauthorizedException,
    )
    async def graphql(self, query, variables=None):
        """Invoke GraphQL request to fetch repositories, pull requests, and issues.

        Args:
            query: Dictionary comprising of query for the GraphQL request.
            variables: Dictionary comprising of query for the GraphQL request.

        Raises:
            UnauthorizedException: Unauthorized exception
            exception: An instance of an exception class.

        Yields:
            dictionary: Client response
        """
        url = f"{self.base_url}/graphql"
        self._logger.debug(
            f"Sending POST to {url} with query: '{json.dumps(query)}' and variables: '{json.dumps(variables)}'"
        )
        try:
            self._get_client.oauth_token = self._access_token()
            return await self._get_client.graphql(
                query=query, endpoint=url, **variables or {}
            )
        except GraphQLAuthorizationFailure as exception:
            if self.auth_method == GITHUB_APP:
                self._logger.debug(
                    f"The access token for installation #{self._installation_id} expired, Regenerating a new token."
                )
                await self._update_installation_access_token()
                raise
            msg = "Your Github token is either expired or revoked. Please check again."
            raise UnauthorizedException(msg) from exception
        except BadGraphQLRequest as exception:
            if exception.status_code == FORBIDDEN:
                msg = f"Provided GitHub token does not have the necessary permissions to perform the request for the URL: {url} and query: {query}."
                raise ForbiddenException(msg) from exception
            else:
                raise
        except QueryError as exception:
            for error in exception.response.get("errors"):
                if (
                    error.get("type").lower() == "rate_limited"
                    and "api rate limit exceeded" in error.get("message").lower()
                ):
                    await self._put_to_sleep(resource_type="graphql")
            msg = f"Error while executing query. Exception: {exception.response.get('errors')}"
            raise Exception(msg) from exception
        except Exception as e:
            self._logger.debug(
                f"An unexpected error occurred while executing GraphQL query: {query}. Error: {e}"
            )
            raise

    @retryable(
        retries=RETRIES,
        interval=RETRY_INTERVAL,
        strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
        skipped_exceptions=UnauthorizedException,
    )
    async def get_github_item(self, resource):
        """Execute request using getitem method of GitHubAPI which is using REST API.
        Using Rest API for fetching files and folder along with content.

        Args:
            resource (str): API to get the response

        Returns:
            dict/list: Response of the request
        """
        self._logger.debug(f"Getting github item: {resource}")
        try:
            return await self._get_client.getitem(
                url=resource, oauth_token=self._access_token()
            )
        except ClientResponseError as exception:
            if exception.status == UNAUTHORIZED:
                if self.auth_method == GITHUB_APP:
                    self._logger.debug(
                        f"The access token for installation #{self._installation_id} expired, Regenerating a new token."
                    )
                    await self._update_installation_access_token()
                    raise
                msg = "Your Github token is either expired or revoked. Please check again."
                raise UnauthorizedException(msg) from exception
            elif self.get_rate_limit_encountered(
                exception.status, exception.headers.get("X-RateLimit-Remaining")
            ):
                await self._put_to_sleep(resource_type="core")
            elif exception.status == FORBIDDEN:
                msg = f"Provided GitHub token does not have the necessary permissions to perform the request for the URL: {resource}."
                raise ForbiddenException(msg) from exception
            else:
                raise
        except RateLimitExceeded:
            await self._put_to_sleep("core")
        except Exception as e:
            self._logger.debug(
                f"An unexpected error occurred while getting GitHub item: {resource}. Error: {e}"
            )
            raise

    async def paginated_api_call(self, query, variables, keys):
        """Make a paginated API call for fetching GitHub objects.

        Args:
            query (string): Graphql Query
            variables (dict): Variables for Graphql API
            keys (list): List of fields to get pageInfo

        Yields:
            dict: dictionary containing response of GitHub.
        """
        while True:
            response = await self.graphql(query=query, variables=variables)
            yield response

            page_info = nested_get_from_dict(response, keys + ["pageInfo"], default={})
            if not page_info.get("hasNextPage"):
                break
            variables["cursor"] = page_info["endCursor"]  # pyright: ignore

    def get_repo_details(self, repo_name):
        return repo_name.split("/")

    @retryable(
        retries=RETRIES,
        interval=RETRY_INTERVAL,
        strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
        skipped_exceptions=UnauthorizedException,
    )
    async def get_personal_access_token_scopes(self):
        try:
            request_headers = sansio.create_headers(
                self._get_client.requester,
                accept=sansio.accept_format(),
                oauth_token=self._access_token(),
            )
            url = f"{self.base_url}/graphql"
            _, headers, _ = await self._get_client._request(
                "HEAD", url, request_headers
            )
            scopes = headers.get("X-OAuth-Scopes")
            if not scopes or not scopes.strip():
                self._logger.warning(
                    f"Couldn't find 'X-OAuth-Scopes' in headers {headers}"
                )
                return set()
            return {scope.strip() for scope in scopes.split(",")}
        except ClientResponseError as exception:
            if exception.status == FORBIDDEN:
                if self.get_rate_limit_encountered(
                    exception.status, exception.headers.get("X-RateLimit-Remaining")
                ):
                    await self._put_to_sleep("graphql")
                else:
                    msg = f"Provided GitHub token does not have the necessary permissions to perform the request for the URL: {self.base_url}."
                    raise ForbiddenException(msg) from exception
            elif exception.status == UNAUTHORIZED:
                msg = "Your Github token is either expired or revoked. Please check again."
                raise UnauthorizedException(msg) from exception
            else:
                raise

    @retryable(
        retries=RETRIES,
        interval=RETRY_INTERVAL,
        strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
    )
    async def _github_app_get(self, url):
        self._logger.debug(f"Making a get request to GitHub: {url}")
        try:
            return await self._get_client._make_request(
                "GET",
                url,
                {},
                b"",
                sansio.accept_format(),
                get_jwt(app_id=self.app_id, private_key=self.private_key),
            )
        # we don't expect any 401 error as the jwt is freshly generated
        except RateLimitExceeded:
            await self._put_to_sleep("core")
        except Exception:
            raise

    async def _github_app_paginated_get(self, url):
        data, more = await self._github_app_get(url)  # pyright: ignore
        if data:
            for item in data:
                yield item
        if more:
            async for item in self._github_app_paginated_get(more):  # pyright: ignore
                yield item

    async def get_installations(self):
        async for installation in self._github_app_paginated_get(
            url="/app/installations"
        ):
            if installation["suspended_at"]:
                self._logger.debug(
                    f"Skip installation '{installation['id']}' because it's suspended."
                )
                continue
            yield installation

    async def get_org_repos(self, org_name):
        repo_variables = {
            "orgName": org_name,
            "cursor": None,
        }
        async for response in self.paginated_api_call(
            query=GithubQuery.ORG_REPOS_QUERY.value,
            variables=repo_variables,
            keys=["organization", "repositories"],
        ):
            for repo in nested_get_from_dict(  # pyright: ignore
                response, ["organization", "repositories", "nodes"], default=[]
            ):
                yield repo

    async def get_user_repos(self, user):
        repo_variables = {
            "login": user,
            "cursor": None,
        }
        async for response in self.paginated_api_call(
            query=GithubQuery.REPOS_QUERY.value,
            variables=repo_variables,
            keys=["user", "repositories"],
        ):
            for repo in nested_get_from_dict(  # pyright: ignore
                response, ["user", "repositories", "nodes"], default=[]
            ):
                yield repo

    async def get_foreign_repo(self, repo_name):
        owner, repo = self.get_repo_details(repo_name=repo_name)
        repo_variables = {"owner": owner, "repositoryName": repo}
        data = await self.graphql(
            query=GithubQuery.REPO_QUERY.value, variables=repo_variables
        )
        return data.get(REPOSITORY_OBJECT)

    async def _fetch_all_members(self, org_name):
        org_variables = {
            "orgName": org_name,
            "cursor": None,
        }
        async for response in self.paginated_api_call(
            query=GithubQuery.ORG_MEMBERS_QUERY.value,
            variables=org_variables,
            keys=["organization", "membersWithRole"],
        ):
            for repo in nested_get_from_dict(  # pyright: ignore
                response,
                ["organization", "membersWithRole", "edges"],
                default=[],
            ):
                yield repo.get("node")

    async def get_logged_in_user(self):
        data = await self.graphql(query=GithubQuery.USER_QUERY.value)
        return nested_get_from_dict(data, ["viewer", "login"])

    async def ping(self):
        if self.auth_method == GITHUB_APP:
            await self._github_app_get(url="/app")
        else:
            await self.get_logged_in_user()

    async def close(self):
        self._sleeps.cancel()
        await self._get_session.close()
        del self._get_session

    def bifurcate_repos(self, repos, owner):
        foreign_repos, configured_repos = [], []
        for repo_name in repos:
            if repo_name not in ["", None]:
                if "/" in repo_name:
                    foreign_repos.append(repo_name)
                else:
                    configured_repos.append(f"{owner}/{repo_name}")
        return foreign_repos, configured_repos


class GitHubAdvancedRulesValidator(AdvancedRulesValidator):
    RULES_OBJECT_SCHEMA_DEFINITION = {
        "type": "object",
        "properties": {
            "repository": {"type": "string", "minLength": 1},
            "filter": {
                "type": "object",
                "properties": {
                    ObjectType.ISSUE.value.lower(): {"type": "string", "minLength": 1},
                    ObjectType.PR.value: {"type": "string", "minLength": 1},
                    ObjectType.BRANCH.value: {"type": "string", "minLength": 1},
                    ObjectType.PATH.value: {"type": "string", "minLength": 1},
                },
                "minProperties": 1,
                "additionalProperties": False,
            },
        },
        "required": ["repository", "filter"],
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

    async def _remote_validation(self, advanced_rules):
        try:
            GitHubAdvancedRulesValidator.SCHEMA(advanced_rules)
        except fastjsonschema.JsonSchemaValueException as e:
            return SyncRuleValidationResult(
                rule_id=SyncRuleValidationResult.ADVANCED_RULES,
                is_valid=False,
                validation_message=e.message,
            )

        self.source.configured_repos = {rule["repository"] for rule in advanced_rules}
        invalid_repos = await self.source.get_invalid_repos()

        if len(invalid_repos) > 0:
            return SyncRuleValidationResult(
                SyncRuleValidationResult.ADVANCED_RULES,
                is_valid=False,
                validation_message=f"Inaccessible repositories '{', '.join(invalid_repos)}'.",
            )

        return SyncRuleValidationResult.valid_result(
            SyncRuleValidationResult.ADVANCED_RULES
        )


class GitHubDataSource(BaseDataSource):
    """GitHub"""

    name = "GitHub"
    service_type = "github"
    advanced_rules_enabled = True
    dls_enabled = True
    incremental_sync_enabled = True

    def __init__(self, configuration):
        """Setup the connection to the GitHub instance.

        Args:
            configuration (DataSourceConfiguration): Instance of DataSourceConfiguration class.
        """
        super().__init__(configuration=configuration)
        self.github_client = GitHubClient(
            auth_method=self.configuration["auth_method"],
            base_url="https://api.github.com"
            if self.configuration["data_source"] == GITHUB_CLOUD
            else f"{self.configuration['host'].rstrip('/')}/api",
            app_id=self.configuration["app_id"],
            private_key=self.configuration["private_key"],
            token=self.configuration["token"],
            ssl_enabled=self.configuration["ssl_enabled"],
            ssl_ca=self.configuration["ssl_ca"],
        )
        self.configured_repos = self.configuration["repositories"]
        self.user_repos = {}
        self.org_repos = {}
        self.foreign_repos = {}
        self.prev_repos = []
        self.members = {}
        self._user = None
        # A dict caches GitHub App installation info, where the key is the org name/user login,
        # and the value is the installation id
        self._installations = {}

    def _set_internal_logger(self):
        self.github_client.set_logger(self._logger)

    def advanced_rules_validators(self):
        return [GitHubAdvancedRulesValidator(self)]

    @classmethod
    def get_default_configuration(cls):
        """Get the default configuration for GitHub.

        Returns:
            dict: Default configuration.
        """
        return {
            "data_source": {
                "display": "dropdown",
                "label": "Data source",
                "options": [
                    {"label": "GitHub Cloud", "value": GITHUB_CLOUD},
                    {"label": "GitHub Server", "value": GITHUB_SERVER},
                ],
                "order": 1,
                "type": "str",
                "value": GITHUB_SERVER,
            },
            "host": {
                "depends_on": [{"field": "data_source", "value": GITHUB_SERVER}],
                "label": "Server URL",
                "order": 2,
                "type": "str",
            },
            "auth_method": {
                "display": "dropdown",
                "label": "Authentication method",
                "options": [
                    {"label": "Personal access token", "value": PERSONAL_ACCESS_TOKEN},
                    {"label": "GitHub App", "value": GITHUB_APP},
                ],
                "order": 3,
                "type": "str",
                "value": PERSONAL_ACCESS_TOKEN,
            },
            "token": {
                "depends_on": [
                    {"field": "auth_method", "value": PERSONAL_ACCESS_TOKEN}
                ],
                "label": "Token",
                "order": 4,
                "sensitive": True,
                "type": "str",
            },
            "repo_type": {
                "display": "dropdown",
                "label": "Repository Type",
                "options": [
                    {"label": "Organization", "value": "organization"},
                    {"label": "Other", "value": "other"},
                ],
                "order": 5,
                "tooltip": "The Document Level Security feature is not available for the Other Repository Type",
                "type": "str",
                "value": "other",
            },
            "org_name": {
                "depends_on": [
                    {"field": "auth_method", "value": PERSONAL_ACCESS_TOKEN},
                    {"field": "repo_type", "value": "organization"},
                ],
                "label": "Organization Name",
                "order": 6,
                "type": "str",
            },
            "app_id": {
                "depends_on": [{"field": "auth_method", "value": GITHUB_APP}],
                "display": "numeric",
                "label": "App ID",
                "order": 7,
                "type": "int",
            },
            "private_key": {
                "depends_on": [{"field": "auth_method", "value": GITHUB_APP}],
                "display": "textarea",
                "label": "App private key",
                "order": 8,
                "sensitive": True,
                "type": "str",
            },
            "repositories": {
                "display": "textarea",
                "label": "List of repositories",
                "order": 9,
                "tooltip": "This configurable field is ignored when Advanced Sync Rules are used.",
                "type": "list",
            },
            "ssl_enabled": {
                "display": "toggle",
                "label": "Enable SSL",
                "order": 10,
                "type": "bool",
                "value": False,
            },
            "ssl_ca": {
                "depends_on": [{"field": "ssl_enabled", "value": True}],
                "label": "SSL certificate",
                "order": 11,
                "type": "str",
            },
            "retry_count": {
                "display_value": RETRIES,
                "display": "numeric",
                "label": "Maximum retries per request",
                "order": 12,
                "required": False,
                "type": "int",
                "ui_restrictions": ["advanced"],
                "value": RETRIES,
            },
            "use_text_extraction_service": {
                "display": "toggle",
                "label": "Use text extraction service",
                "order": 13,
                "tooltip": "Requires a separate deployment of the Elastic Text Extraction Service. Requires that pipeline settings disable text extraction.",
                "type": "bool",
                "ui_restrictions": ["advanced"],
                "value": False,
            },
            "use_document_level_security": {
                "display": "toggle",
                "depends_on": [{"field": "repo_type", "value": "organization"}],
                "label": "Enable document level security",
                "order": 14,
                "tooltip": "Document level security ensures identities and permissions set in GitHub are maintained in Elasticsearch. This enables you to restrict and personalize read-access users and groups have to documents in this index. Access control syncs ensure this metadata is kept up to date in your Elasticsearch documents.",
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

        return (
            self.configuration["repo_type"] == "organization"
            and self.configuration["use_document_level_security"]
        )

    async def _logged_in_user(self):
        if self.configuration["auth_method"] != PERSONAL_ACCESS_TOKEN:
            return None
        if self._user:
            return self._user
        self._user = await self.github_client.get_logged_in_user()
        return self._user

    async def get_invalid_repos(self):
        self._logger.debug(
            "Checking if there are any inaccessible repositories configured"
        )
        if self.configuration["auth_method"] == GITHUB_APP:
            return await self._get_invalid_repos_for_github_app()
        else:
            return await self._get_invalid_repos_for_personal_access_token()

    async def _get_invalid_repos_for_github_app(self):
        # A github app can be installed on multiple orgs/personal accounts,
        # so the repo must be configured in the format of 'OWNER/REPO', any other format will be rejected
        invalid_repos = set(
            filter(
                lambda repo: repo
                and repo.strip()
                and len(repo.strip().split("/")) != 2,
                self.configured_repos,
            )
        )
        for full_repo_name in self.configured_repos:
            if full_repo_name in invalid_repos:
                continue
            owner, repo_name = self.github_client.get_repo_details(
                repo_name=full_repo_name
            )
            if await self._get_repo_object_for_github_app(owner, repo_name) is None:
                invalid_repos.add(full_repo_name)

        return list(invalid_repos)

    async def _get_repo_object_for_github_app(self, owner, repo_name):
        await self._fetch_installations()
        full_repo_name = f"{owner}/{repo_name}"
        if owner not in self._installations:
            self._logger.debug(
                f"Invalid repo {full_repo_name} as the github app is not installed on {owner}"
            )
            return None
        await self.github_client.update_installation_id(self._installations[owner])

        if self.configuration["repo_type"] == "organization":
            if owner not in self.org_repos:
                # get repos for an org and cached it in self.org_repos
                async for _ in self._get_org_repos(owner):
                    pass

            cached_repo = self.org_repos
        else:
            if owner not in self.user_repos:
                # get repos for a user can cached it in self.user_repos
                async for _ in self._get_personal_repos(owner):
                    pass

            cached_repo = self.user_repos

        if full_repo_name not in cached_repo[owner]:
            self._logger.debug(
                f"Invalid repo {full_repo_name} as it's either non-existing or not accessible"
            )
            return None

        return cached_repo[owner][full_repo_name]

    async def _get_invalid_repos_for_personal_access_token(self):
        try:
            if self.configuration["repo_type"] == "other":
                logged_in_user = await self._logged_in_user()
                foreign_repos, configured_repos = self.github_client.bifurcate_repos(
                    repos=self.configured_repos,
                    owner=logged_in_user,
                )
                if logged_in_user not in self.user_repos:
                    self.user_repos[logged_in_user] = {}
                async for repo in self.github_client.get_user_repos(logged_in_user):
                    self.user_repos[logged_in_user][repo["nameWithOwner"]] = repo
                invalid_repos = list(
                    set(configured_repos) - set(self.user_repos[logged_in_user].keys())
                )

                for repo_name in foreign_repos:
                    try:
                        self.foreign_repos[
                            repo_name
                        ] = await self.github_client.get_foreign_repo(
                            repo_name=repo_name
                        )
                    except Exception:
                        self._logger.debug(f"Detected invalid repository: {repo_name}.")
                        invalid_repos.append(repo_name)
            else:
                foreign_repos, configured_repos = self.github_client.bifurcate_repos(
                    repos=self.configured_repos,
                    owner=self.configuration["org_name"],
                )
                configured_repos.extend(foreign_repos)
                if self.configuration["org_name"] not in self.org_repos:
                    self.org_repos[self.configuration["org_name"]] = {}
                async for repo in self.github_client.get_org_repos(
                    self.configuration["org_name"]
                ):
                    self.org_repos[self.configuration["org_name"]][
                        repo["nameWithOwner"]
                    ] = repo
                invalid_repos = list(
                    set(configured_repos)
                    - set(self.org_repos[self.configuration["org_name"]].keys())
                )
            return invalid_repos
        except Exception as exception:
            self._logger.exception(
                f"Error while checking for inaccessible repositories. Exception: {exception}.",
                exc_info=True,
            )
            raise

    async def _user_access_control_doc(self, user):
        user_id = user.get("id", "")
        user_name = user.get("login", "")
        user_email = user.get("email", "")

        _prefixed_user_id = _prefix_user_id(user_id=user_id)
        _prefixed_user_name = _prefix_username(user=user_name)
        _prefixed_email = _prefix_email(email=user_email)
        return {
            "_id": user_id,
            "identity": {
                "user_id": _prefixed_user_id,
                "user_name": _prefixed_user_name,
                "email": _prefixed_email,
            },
            "created_at": user.get("updatedAt"),
        } | es_access_control_query(
            access_control=[_prefixed_user_id, _prefixed_user_name, _prefixed_email]
        )

    async def get_access_control(self):
        if not self._dls_enabled():
            self._logger.warning("DLS is not enabled. Skipping")
            return

        async for org_name in self._get_owners():
            async for user in self.github_client._fetch_all_members(org_name):
                yield await self._user_access_control_doc(user=user)

    async def _get_owners(self):
        """Yields organization or personal accounts based on the configured repo_type"""
        if self.configuration["auth_method"] == GITHUB_APP:
            await self._fetch_installations()
            for owner, installation_id in self._installations.items():
                await self.github_client.update_installation_id(installation_id)
                yield owner
        else:
            if self.configuration["repo_type"] == "organization":
                yield self.configuration["org_name"]
            else:
                yield await self._logged_in_user()

    async def _remote_validation(self):
        """Validate scope of the configured personal access token and accessibility of repositories

        Raises:
            ConfigurableFieldValueError: Insufficient privileges error.
        """

        await self._validate_personal_access_token_scopes()
        await self._validate_configured_repos()

    async def _validate_personal_access_token_scopes(self):
        if self.configuration["auth_method"] != PERSONAL_ACCESS_TOKEN:
            return

        scopes = await self.github_client.get_personal_access_token_scopes() or set()
        required_scopes = {"repo", "user", "read:org"}

        for scope in ["write:org", "admin:org"]:
            if scope in scopes:
                scopes.add("read:org")

        if required_scopes.issubset(scopes):
            extra_scopes = scopes - required_scopes
            if extra_scopes:
                self._logger.warning(
                    "The provided token has higher privileges than required. It is advisable to run the connector with least privielged token. Required scopes are 'repo', 'user', and 'read:org'."
                )
        else:
            msg = "Configured token does not have required rights to fetch the content. Required scopes are 'repo', 'user', and 'read:org'."
            raise ConfigurableFieldValueError(msg)

    async def _validate_configured_repos(self):
        if WILDCARD in self.configured_repos:
            return

        invalid_repos = await self.get_invalid_repos()
        if invalid_repos:
            msg = f"Inaccessible repositories '{', '.join(invalid_repos)}'."
            raise ConfigurableFieldValueError(msg)

    async def validate_config(self):
        """Validates whether user input is empty or not for configuration fields
        Also validate, if user configured repositories are accessible or not and scope of the token
        """
        await super().validate_config()
        await self._remote_validation()

    async def close(self):
        await self.github_client.close()

    async def ping(self):
        try:
            await self.github_client.ping()
            self._logger.debug("Successfully connected to GitHub.")
        except Exception:
            self._logger.exception("Error while connecting to GitHub.")
            raise

    def adapt_gh_doc_to_es_doc(self, github_document, schema):
        return {
            es_field: github_document[github_field]
            for es_field, github_field in schema.items()
        }

    def _prepare_pull_request_doc(self, pull_request, reviews):
        return {
            "_id": pull_request.pop("id"),
            "_timestamp": pull_request.pop("updatedAt"),
            "type": ObjectType.PULL_REQUEST.value,
            "issue_comments": pull_request.get("comments", {}).get("nodes"),
            "reviews_comments": reviews,
            "labels_field": pull_request.get("labels", {}).get("nodes"),
            "assignees_list": pull_request.get("assignees", {}).get("nodes"),
            "requested_reviewers": pull_request.get("reviewRequests", {}).get("nodes"),
        }

    def _prepare_issue_doc(self, issue):
        return {
            "_id": issue.pop("id"),
            "type": ObjectType.ISSUE.value,
            "_timestamp": issue.pop("updatedAt"),
            "issue_comments": issue.get("comments", {}).get("nodes"),
            "labels_field": issue.get("labels", {}).get("nodes"),
            "assignees_list": issue.get("assignees", {}).get("nodes"),
        }

    def _prepare_review_doc(self, review):
        # review.author can be None if the user was deleted, so need to be extra null-safe
        author = review.get("author", {}) or {}

        return {
            "author": author.get("login"),
            "body": review.get("body"),
            "state": review.get("state"),
            "comments": review.get("comments", {}).get("nodes"),
        }

    async def _fetch_installations(self):
        """Fetches GitHub App installations, and populates instance variable self._installations
        Only populates Organization installations when repo_type is organization, and only populates User installations when repo_type is other
        """
        if self.configuration["auth_method"] != GITHUB_APP:
            return {}
        if self._installations:
            return self._installations

        async for installation in self.github_client.get_installations():
            if (
                self.configuration["repo_type"] == "organization"
                and installation["account"]["type"] == "Organization"
            ) or (
                self.configuration["repo_type"] == "other"
                and installation["account"]["type"] == "User"
            ):
                self._installations[installation["account"]["login"]] = installation[
                    "id"
                ]

        return self._installations

    async def _get_personal_repos(self, user):
        self._logger.info(f"Fetching personal repos {user}")
        if user in self.user_repos:
            for repo_object in self.user_repos[user]:
                yield repo_object
        else:
            self.user_repos[user] = {}
            async for repo_object in self.github_client.get_user_repos(user):
                self.user_repos[user][repo_object["nameWithOwner"]] = repo_object
                yield repo_object

    async def _get_org_repos(self, org_name):
        self._logger.info(f"Fetching org repos for {org_name}")
        if org_name in self.org_repos:
            for repo_object in self.org_repos[org_name]:
                yield repo_object
        else:
            self.org_repos[org_name] = {}
            async for repo_object in self.github_client.get_org_repos(org_name):
                self.org_repos[org_name][repo_object["nameWithOwner"]] = repo_object
                yield repo_object

    async def _get_configured_repos(self, configured_repos):
        self._logger.info(f"Fetching configured repos: '{configured_repos}'")
        for repo_name in configured_repos:
            self._logger.info(f"Fetching repo: '{repo_name}'")
            if repo_name in ["", None]:
                continue

            if self.configuration["auth_method"] == PERSONAL_ACCESS_TOKEN:
                # Converting the local repository names to username/repo_name format.
                logged_in_user = await self._logged_in_user()
                if "/" not in repo_name:
                    owner = (
                        logged_in_user
                        if self.configuration["repo_type"] == "other"
                        else self.configuration["org_name"]
                    )
                    repo_name = f"{owner}/{repo_name}"
                repo_object = self.foreign_repos.get(repo_name) or self.user_repos.get(
                    logged_in_user, {}
                ).get(repo_name)

                if not repo_object:
                    owner, repo = self.github_client.get_repo_details(
                        repo_name=repo_name
                    )
                    variables = {"owner": owner, "repositoryName": repo}
                    data = await self.github_client.graphql(
                        query=GithubQuery.REPO_QUERY.value, variables=variables
                    )
                    repo_object = data.get(REPOSITORY_OBJECT)
            else:
                owner, repo = self.github_client.get_repo_details(repo_name=repo_name)
                repo_object = await self._get_repo_object_for_github_app(owner, repo)
                # update installation and re-generate installation access token
                await self.github_client.update_installation_id(
                    self._installations[owner]
                )
            yield self._convert_repo_object_to_doc(repo_object)

    async def _fetch_repos(self):
        self._logger.info("Fetching repos")
        try:
            if (
                WILDCARD in self.configured_repos
                and self.configuration["repo_type"] == "other"
            ):
                async for user in self._get_owners():
                    async for repo_object in self._get_personal_repos(user):
                        yield self._convert_repo_object_to_doc(repo_object)
            elif (
                WILDCARD in self.configured_repos
                and self.configuration["repo_type"] == "organization"
            ):
                async for org in self._get_owners():
                    async for repo_object in self._get_org_repos(org):
                        yield self._convert_repo_object_to_doc(repo_object)
            else:
                async for repo_object in self._get_configured_repos(
                    configured_repos=self.configured_repos
                ):
                    yield repo_object
        except UnauthorizedException:
            raise
        except ForbiddenException:
            raise
        except Exception as exception:
            self._logger.warning(
                f"Something went wrong while fetching the repository. Exception: {exception}",
                exc_info=True,
            )

    def _convert_repo_object_to_doc(self, repo_object):
        repo_object = repo_object.copy()
        repo_object.update(
            {
                "_id": repo_object.pop("id"),
                "_timestamp": repo_object.pop("updatedAt"),
                "type": ObjectType.REPOSITORY.value,
            }
        )
        return repo_object

    async def _fetch_remaining_data(
        self, variables, object_type, query, field_type, keys
    ):
        async for response in self.github_client.paginated_api_call(
            query=query, variables=variables, keys=keys
        ):
            yield nested_get_from_dict(
                response, [REPOSITORY_OBJECT, object_type, field_type, "nodes"]
            )

    async def _fetch_remaining_fields(
        self, type_obj, object_type, owner, repo, field_type
    ):
        sample_dict = {
            "reviews": {
                "query": GithubQuery.REVIEW_QUERY.value,
                "es_field": "reviews_comments",
            },
            "reviewRequests": {
                "query": GithubQuery.REVIEWERS_QUERY.value,
                "es_field": "requested_reviewers",
            },
            "comments": {
                "query": GithubQuery.COMMENT_QUERY.value.format(
                    object_type=object_type, node_size=NODE_SIZE
                ),
                "es_field": "issue_comments",
            },
            "labels": {
                "query": GithubQuery.LABELS_QUERY.value.format(
                    object_type=object_type, node_size=NODE_SIZE
                ),
                "es_field": "labels_field",
            },
            "assignees": {
                "query": GithubQuery.ASSIGNEES_QUERY.value.format(
                    object_type=object_type, node_size=NODE_SIZE
                ),
                "es_field": "assignees_list",
            },
        }
        page_info = type_obj.get(field_type, {}).get("pageInfo", {})
        if page_info.get("hasNextPage"):
            variables = {
                "owner": owner,
                "name": repo,
                "number": type_obj.get("number"),
                "cursor": page_info.get("endCursor"),
            }
            async for response in self._fetch_remaining_data(
                variables=variables,
                object_type=object_type,
                query=sample_dict[field_type]["query"],
                field_type=field_type,
                keys=[REPOSITORY_OBJECT, object_type, field_type],
            ):
                if field_type == "reviews":
                    for review in response:  # pyright: ignore
                        type_obj["reviews_comments"].append(
                            self._prepare_review_doc(review=review)
                        )
                else:
                    type_obj[sample_dict[field_type]["es_field"]].extend(response)

    async def _extract_pull_request(self, pull_request, owner, repo):
        reviews = [
            self._prepare_review_doc(review=review)
            for review in pull_request.get("reviews", {}).get("nodes")
        ]
        pull_request.update(
            self._prepare_pull_request_doc(pull_request=pull_request, reviews=reviews)
        )
        for field in ["comments", "reviewRequests", "labels", "assignees", "reviews"]:
            await self._fetch_remaining_fields(
                type_obj=pull_request,
                object_type=PULL_REQUEST_OBJECT,
                owner=owner,
                repo=repo,
                field_type=field,
            )
            pull_request.pop(field)
        yield pull_request

    async def _fetch_pull_requests(
        self,
        repo_name,
        response_key,
        filter_query=None,
    ):
        self._logger.info(
            f"Fetching pull requests from '{repo_name}' with response_key '{response_key}' and filter query: '{filter_query}'"
        )
        try:
            query = (
                GithubQuery.SEARCH_QUERY.value
                if filter_query
                else GithubQuery.PULL_REQUEST_QUERY.value
            )
            owner, repo = self.github_client.get_repo_details(repo_name=repo_name)
            pull_request_variables = {
                "owner": owner,
                "name": repo,
                "cursor": None,
                "filter_query": filter_query,
            }
            async for response in self.github_client.paginated_api_call(
                query=query,
                variables=pull_request_variables,
                keys=response_key,
            ):
                for pull_request in nested_get_from_dict(  # pyright: ignore
                    response, response_key + ["nodes"], default=[]
                ):
                    async for pull_request_doc in self._extract_pull_request(
                        pull_request=pull_request, owner=owner, repo=repo
                    ):
                        yield pull_request_doc
        except UnauthorizedException:
            raise
        except ForbiddenException:
            raise
        except Exception as exception:
            self._logger.warning(
                f"Something went wrong while fetching the pull requests. Exception: {exception}",
                exc_info=True,
            )

    async def _extract_issues(self, response, owner, repo, response_key):
        for issue in nested_get_from_dict(  # pyright: ignore
            response, response_key + ["nodes"], default=[]
        ):
            issue.update(self._prepare_issue_doc(issue=issue))
            for field in ["comments", "labels", "assignees"]:
                await self._fetch_remaining_fields(
                    type_obj=issue,
                    object_type=ObjectType.ISSUE.value.lower(),
                    owner=owner,
                    repo=repo,
                    field_type=field,
                )
                issue.pop(field)
            yield issue

    async def _fetch_issues(
        self,
        repo_name,
        response_key,
        filter_query=None,
    ):
        self._logger.info(
            f"Fetching issues from repo: {repo_name} with response_key: '{response_key}' and filter_query: '{filter_query}'"
        )
        try:
            query = (
                GithubQuery.SEARCH_QUERY.value
                if filter_query
                else GithubQuery.ISSUE_QUERY.value
            )
            owner, repo = self.github_client.get_repo_details(repo_name=repo_name)
            issue_variables = {
                "owner": owner,
                "name": repo,
                "cursor": None,
                "filter_query": filter_query,
            }
            async for response in self.github_client.paginated_api_call(
                query=query,
                variables=issue_variables,
                keys=response_key,
            ):
                async for issue in self._extract_issues(
                    response=response, owner=owner, repo=repo, response_key=response_key
                ):
                    yield issue
        except UnauthorizedException:
            raise
        except ForbiddenException:
            raise
        except Exception as exception:
            self._logger.warning(
                f"Something went wrong while fetching the issues. Exception: {exception}",
                exc_info=True,
            )

    async def _fetch_last_commit_timestamp(self, repo_name, path):
        commit, *_ = await self.github_client.get_github_item(  # pyright: ignore
            resource=self.github_client.endpoints["COMMITS"].format(
                repo_name=repo_name, path=path
            )
        )
        return commit["commit"]["committer"]["date"]

    async def _format_file_document(self, repo_object, repo_name, schema):
        file_name = repo_object["path"].split("/")[-1]
        file_extension = (
            file_name[file_name.rfind(".") :] if "." in file_name else ""  # noqa
        )
        if file_extension.lower() in SUPPORTED_EXTENSION:
            last_commit_timestamp = await self._fetch_last_commit_timestamp(
                repo_name=repo_name, path=repo_object["path"]
            )
            repo_object.update(
                {
                    "_timestamp": last_commit_timestamp,
                    "repo_name": repo_name,
                    "name": file_name,
                    "extension": file_extension,
                }
            )

            document = self.adapt_gh_doc_to_es_doc(
                github_document=repo_object, schema=schema
            )

            document["_id"] = f"{repo_name}/{repo_object['path']}"
            return document, repo_object

    async def _fetch_files(self, repo_name, default_branch):
        self._logger.info(
            f"Fetching files from repo: '{repo_name}' (branch: '{default_branch}')"
        )
        try:
            file_tree = await self.github_client.get_github_item(
                resource=self.github_client.endpoints["TREE"].format(
                    repo_name=repo_name, default_branch=default_branch
                )
            )

            for repo_object in file_tree.get("tree", []):
                if repo_object["type"] == BLOB:
                    if document := await self._format_file_document(
                        repo_object=repo_object, repo_name=repo_name, schema=FILE_SCHEMA
                    ):
                        yield document
        except UnauthorizedException:
            raise
        except ForbiddenException:
            raise
        except Exception as exception:
            self._logger.warning(
                f"Something went wrong while fetching the files of {repo_name}. Exception: {exception}",
                exc_info=True,
            )

    async def _fetch_files_by_path(self, repo_name, path):
        self._logger.info(f"Fetching files from repo: '{repo_name}' (path: '{path}')")
        try:
            for repo_object in await self.github_client.get_github_item(
                resource=self.github_client.endpoints["PATH"].format(
                    repo_name=repo_name, path=path
                )
            ):  # pyright: ignore
                if repo_object["type"] == FILE:
                    if document := await self._format_file_document(
                        repo_object=repo_object,
                        repo_name=repo_name,
                        schema=PATH_SCHEMA,
                    ):
                        yield document
        except UnauthorizedException:
            raise
        except ForbiddenException:
            raise
        except Exception as exception:
            self._logger.warning(
                f"Something went wrong while fetching the files of {repo_name}. Exception: {exception}",
                exc_info=True,
            )

    async def get_content(self, attachment, timestamp=None, doit=False):
        """Extracts the content for Apache TIKA supported file types.

        Args:
            attachment (dictionary): Formatted attachment document.
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
        if not self.can_file_be_downloaded(file_extension, filename, file_size):
            return

        document = {
            "_id": f"{attachment['repo_name']}/{filename}",
            "_timestamp": attachment["_timestamp"],
        }
        return await self.download_and_extract_file(
            document,
            filename,
            file_extension,
            partial(
                self.download_func,
                attachment["url"],
            ),
        )

    async def download_func(self, url):
        file_data = await self.github_client.get_github_item(resource=url)
        if file_data:
            yield decode_base64_value(content=file_data["content"])
        else:
            yield

    def _filter_rule_query(self, repo, query, query_type):
        """
        Filters a query based on the query type.

        Args:
            repo (str): Query repo name.
            query (str): The input query.
            query_type (str): The type of query ("pr" or "issue").

        Returns:
            tuple: A tuple containing a boolean value indicating whether the query should be included or excluded and the modified query.
        """
        if query_type == ObjectType.PR.value:
            if "is:issue" in query:
                return False, query
            elif "is:pr" in query:
                return True, f"repo:{repo} {query}"
            return True, f"repo:{repo} is:pr {query}"
        elif query_type == ObjectType.ISSUE.value.lower():
            if "is:pr" in query:
                return False, query
            elif "is:issue" in query:
                return True, f"repo:{repo} {query}"
            return True, f"repo:{repo} is:issue {query}"
        else:
            return False, query

    def is_previous_repo(self, repo_name):
        if repo_name in self.prev_repos:
            return True
        self.prev_repos.append(repo_name)
        return False

    def _decorate_with_access_control(self, document, access_control):
        if self._dls_enabled():
            document[ACCESS_CONTROL] = list(
                set(document.get(ACCESS_CONTROL, []) + access_control)
            )
        return document

    async def _fetch_access_control(self, repo_name):
        owner, repo = self.github_client.get_repo_details(repo_name)
        collaborator_variables = {
            "orgName": owner,
            "repoName": repo,
            "cursor": None,
        }
        access_control = []
        if owner not in self.members:
            members = set()
            async for user in self.github_client._fetch_all_members(owner):
                members.add(user.get("id"))
            self.members[owner] = members
        async for response in self.github_client.paginated_api_call(
            query=GithubQuery.COLLABORATORS_QUERY.value,
            variables=collaborator_variables,
            keys=["repository", "collaborators"],
        ):
            for user in nested_get_from_dict(  # pyright: ignore
                response, ["repository", "collaborators", "edges"], default=[]
            ):
                user_id = user.get("node", {}).get("id")
                user_name = user.get("node", {}).get("login")
                user_email = user.get("node", {}).get("email")
                if user_id in self.members[owner]:
                    access_control.append(_prefix_user_id(user_id=user_id))
                    if user_name:
                        access_control.append(_prefix_username(user=user_name))
                    if user_email:
                        access_control.append(_prefix_email(email=user_email))
        return access_control

    async def get_docs(self, filtering=None):
        """Executes the logic to fetch GitHub objects in async manner.

        Args:
            filtering (filtering, None): Filtering Rules. Defaults to None.

        Yields:
            dict: Documents from GitHub.
        """
        if filtering and filtering.has_advanced_rules():
            self._logger.info("Using advanced rules")
            advanced_rules = filtering.get_advanced_rules()
            for rule in advanced_rules:
                repo = await anext(
                    self._get_configured_repos(configured_repos=[rule["repository"]])
                )
                yield repo, None
                repo_name = repo.get("nameWithOwner")

                if pull_request_query := rule["filter"].get(ObjectType.PR.value):
                    query_status, pull_request_query = self._filter_rule_query(
                        repo=repo_name,
                        query=pull_request_query,
                        query_type=ObjectType.PR.value,
                    )
                    if query_status:
                        async for pull_request in self._fetch_pull_requests(
                            repo_name=repo_name,
                            response_key=["search"],
                            filter_query=pull_request_query,
                        ):
                            yield pull_request, None
                    else:
                        self._logger.warning(
                            f"Skipping pr for rule: {pull_request_query}"
                        )

                if issue_query := rule["filter"].get(ObjectType.ISSUE.value.lower()):
                    query_status, issue_query = self._filter_rule_query(
                        repo=repo_name,
                        query=issue_query,
                        query_type=ObjectType.ISSUE.value.lower(),
                    )
                    if query_status:
                        async for issue in self._fetch_issues(
                            repo_name=repo_name,
                            response_key=["search"],
                            filter_query=issue_query,
                        ):
                            yield issue, None
                    else:
                        self._logger.warning(f"Skipping issue for query: {issue_query}")

                if branch := rule["filter"].get(ObjectType.BRANCH.value):
                    async for file_document, attachment_metadata in self._fetch_files(
                        repo_name=repo_name, default_branch=branch
                    ):
                        if file_document["type"] == BLOB:
                            yield (
                                file_document,
                                partial(
                                    self.get_content, attachment=attachment_metadata
                                ),
                            )
                        else:
                            yield file_document, None

                if path := rule["filter"].get(ObjectType.PATH.value):
                    async for (
                        file_document,
                        attachment_metadata,
                    ) in self._fetch_files_by_path(repo_name=repo_name, path=path):
                        if file_document["type"] == FILE:
                            attachment_metadata["url"] = attachment_metadata["git_url"]
                            yield (
                                file_document,
                                partial(
                                    self.get_content, attachment=attachment_metadata
                                ),
                            )
                        else:
                            yield file_document, None
        else:
            async for repo in self._fetch_repos():
                if self.is_previous_repo(repo["nameWithOwner"]):
                    continue

                access_control = []
                is_public_repo = repo.get("visibility").lower() == "public"
                needs_access_control = self._dls_enabled() and not is_public_repo

                if needs_access_control:
                    access_control = await self._fetch_access_control(
                        repo_name=repo.get("nameWithOwner")
                    )

                if needs_access_control:
                    yield (
                        self._decorate_with_access_control(
                            document=repo, access_control=access_control
                        ),
                        None,
                    )
                else:
                    yield repo, None

                repo_name = repo.get("nameWithOwner")
                default_branch = (
                    repo.get("defaultBranchRef", {}).get("name")
                    if repo.get("defaultBranchRef")
                    else None
                )

                async for pull_request in self._fetch_pull_requests(
                    repo_name=repo_name,
                    response_key=[REPOSITORY_OBJECT, "pullRequests"],
                ):
                    if needs_access_control:
                        yield (
                            self._decorate_with_access_control(
                                document=pull_request, access_control=access_control
                            ),
                            None,
                        )
                    else:
                        yield pull_request, None

                async for issue in self._fetch_issues(
                    repo_name=repo_name, response_key=[REPOSITORY_OBJECT, "issues"]
                ):
                    if needs_access_control:
                        yield (
                            self._decorate_with_access_control(
                                document=issue, access_control=access_control
                            ),
                            None,
                        )
                    else:
                        yield issue, None

                if default_branch:
                    async for file_document, attachment_metadata in self._fetch_files(
                        repo_name=repo_name, default_branch=default_branch
                    ):
                        if file_document["type"] == BLOB:
                            if needs_access_control:
                                yield (
                                    self._decorate_with_access_control(
                                        document=file_document,
                                        access_control=access_control,
                                    ),
                                    partial(
                                        self.get_content, attachment=attachment_metadata
                                    ),
                                )
                            else:
                                yield (
                                    file_document,
                                    partial(
                                        self.get_content, attachment=attachment_metadata
                                    ),
                                )
                        else:
                            if needs_access_control:
                                yield (
                                    self._decorate_with_access_control(
                                        document=file_document,
                                        access_control=access_control,
                                    ),
                                    None,
                                )
                            else:
                                yield file_document, None
