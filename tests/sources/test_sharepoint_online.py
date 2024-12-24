#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import asyncio
import base64
import re
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from functools import partial
from io import BytesIO
from unittest.mock import ANY, AsyncMock, MagicMock, Mock, patch

import aiohttp
import pytest
import pytest_asyncio
from aiohttp.client_exceptions import ClientPayloadError, ClientResponseError
from freezegun import freeze_time

from connectors.logger import logger
from connectors.protocol import Features
from connectors.source import ConfigurableFieldValueError
from connectors.sources.sharepoint_online import (
    ACCESS_CONTROL,
    DEFAULT_BACKOFF_MULTIPLIER,
    DEFAULT_RETRY_SECONDS,
    WILDCARD,
    BadRequestError,
    DriveItemsPage,
    GraphAPIToken,
    InternalServerError,
    InvalidSharepointTenant,
    MicrosoftAPISession,
    MicrosoftSecurityToken,
    NotFound,
    PermissionsMissing,
    SharepointOnlineAdvancedRulesValidator,
    SharepointOnlineClient,
    SharepointOnlineDataSource,
    SharepointRestAPIToken,
    SyncCursorEmpty,
    ThrottledError,
    TokenFetchFailed,
    _get_login_name,
    _prefix_email,
    _prefix_group,
    _prefix_user,
    _prefix_user_id,
)
from connectors.utils import iso_utc
from tests.commons import AsyncIterator
from tests.sources.support import create_source

SITE_LIST_ONE_NAME = "site-list-one-name"

SITE_LIST_ONE_ID = "1"

TIMESTAMP_FORMAT_PATCHED = "%Y-%m-%dT%H:%M:%SZ"

ONLY_USERNAMES = True
USERNAMES_AND_EMAILS = False
WITH_PREFIX = True
WITHOUT_PREFIX = False
IDENTITY_MAIL = "mail@spo.com"
IDENTITY_USER_PRINCIPAL_NAME = "some identity"
IDENTITY_WITH_MAIL_AND_PRINCIPAL_NAME = {
    "mail": IDENTITY_MAIL,
    "userPrincipalName": IDENTITY_USER_PRINCIPAL_NAME,
}


DOMAIN_GROUP_ID = "domain-group-id"

OWNER_TWO_USER_PRINCIPAL_NAME = "some.owner2@spo.com"

OWNER_ONE_EMAIL = "some.owner1@spo.com"

MEMBER_TWO_USER_PRINCIPAL_NAME = "some.member2spo.com"

MEMBER_ONE_EMAIL = "some.member1@spo.com"

GROUP_ONE_ID = "group-id-1"

GROUP_ONE = "Group 1"

GROUP_TWO = "Group 2"

GROUP_TWO_ID = "group-id-2"

USER_ONE_ID = "user-id-1"

USER_ONE_EMAIL = "user1@spo.com"

USER_TWO_EMAIL = "user2@spo.com"

USER_TWO_NAME = "user2"

SITEGROUP_USER_ONE_EMAIL = "sitegroup.user1@spo.com"

SITEGROUP_USER_ONE_ID = "sitegroup.user1"

NUMBER_OF_DEFAULT_GROUPS = 3

ALLOW_ACCESS_CONTROL_PATCHED = "access_control"
DEFAULT_GROUPS_PATCHED = ["some default group"]

READ_BINDING = [
    {
        "BasePermissions": {"High": "176", "Low": "138612833"},
        "Description": "Can view pages and list items and download documents.",
        "Hidden": False,
        "Id": 1073741826,
        "Name": "Read",
        "Order": 128,
        "RoleTypeKind": 2,
    }
]

LIMITED_ACCESS_BINDING = [
    {
        "BasePermissions": {"High": "16", "Low": "134283264"},
        "Description": "Can view specific lists, document libraries, list items, folders, or documents when given permissions.",
        "Hidden": True,
        "Id": 1073741825,
        "Name": "Limited Access",
        "Order": 160,
        "RoleTypeKind": 1,
    }
]

WEB_ONLY_BINDING = [
    {
        "BasePermissions": {"High": "560", "Low": "134221824"},
        "Description": "Can only view the web when given permissions.",
        "Hidden": True,
        "Id": 1073741833,
        "Name": "Web-Only Limited Access",
        "Order": 176,
        "RoleTypeKind": 9,
    }
]

SAMPLE_DRIVE_PERMISSIONS = [
    {
        "id": "3",
        "grantedToV2": {
            "user": {
                "id": USER_ONE_ID,
            },
            "group": {
                "id": GROUP_ONE_ID,
            },
        },
    },
    {
        "id": "4",
        "grantedToV2": {
            "user": {
                "id": USER_ONE_ID,
            },
            "group": {
                "id": GROUP_ONE_ID,
            },
        },
    },
    {
        "id": "5",
        "grantedToV2": {
            "user": {
                "id": USER_ONE_ID,
            },
            "group": {
                "id": GROUP_ONE_ID,
            },
        },
    },
    {
        "id": "6",
        "grantedToV2": {
            "user": {
                "id": USER_ONE_ID,
            },
            "group": {
                "id": GROUP_ONE_ID,
            },
            "siteGroup": {"id": "2", "displayName": "Some site group"},
        },
    },
]

SAMPLE_SITE_GROUP_USERS = [
    {
        "UserPrincipalName": SITEGROUP_USER_ONE_ID,
        "Email": SITEGROUP_USER_ONE_EMAIL,
    }
]

BATCH_THROTTLED_RESPONSE = {
    "responses": [
        {
            "id": "014DQHDVKA25ZKRFXRQBDKQS2ZMJ3C442M",
            "status": 429,
            "headers": {
                "Link": '<https://developer.microsoft-tst.com/en-us/graph/changes?$filterby=v1.0,Removal&from=2021-09-01&to=2021-10-01>;rel="deprecation";type="text/html",<https://developer.microsoft-tst.com/en-us/graph/changes?$filterby=v1.0,Removal&from=2021-09-01&to=2021-10-01>;rel="deprecation";type="text/html"',
                "Deprecation": "Fri, 03 Sep 2021 23:59:59 GMT",
                "Sunset": "Sun, 01 Oct 2023 23:59:59 GMT",
                "Retry-After": "28",
                "Cache-Control": "private",
                "Content-Type": "application/json",
            },
            "body": {
                "error": {
                    "code": "activityLimitReached",
                    "message": "The request has been throttled",
                    "innerError": {
                        "code": "throttledRequest",
                        "innerError": {"code": "quota"},
                        "date": "2023-09-29T14:31:03",
                        "request-id": "b4e31937-edce-43cc-a32b-dcf664ddc754",
                        "client-request-id": "b4e31937-edce-43cc-a32b-dcf664ddc754",
                    },
                }
            },
        }
    ]
}


@asynccontextmanager
async def create_spo_source(
    tenant_name="test",
    site_collections=WILDCARD,
    use_document_level_security=False,
    use_text_extraction_service=False,
    fetch_drive_item_permissions=True,
    fetch_unique_list_permissions=True,
    enumerate_all_sites=False,
):
    async with create_source(
        SharepointOnlineDataSource,
        tenant_id="1",
        client_id="2",
        secret_value="3",
        tenant_name=tenant_name,
        site_collections=site_collections,
        use_document_level_security=use_document_level_security,
        use_text_extraction_service=use_text_extraction_service,
        fetch_drive_item_permissions=fetch_drive_item_permissions,
        fetch_unique_list_permissions=fetch_unique_list_permissions,
        enumerate_all_sites=enumerate_all_sites,
    ) as source:
        source.set_features(
            Features(
                {"document_level_security": {"enabled": use_document_level_security}}
            )
        )
        yield source


def dls_feature_flag_enabled(value):
    return value


def dls_enabled_config_value(value):
    return value


def dls_enabled(value):
    return value


def access_control_matches(actual, expected):
    return all(access_control in expected for access_control in actual)


def access_control_is_equal(actual, expected):
    return set(actual) == set(expected)


class TestMicrosoftSecurityToken:
    class StubMicrosoftSecurityToken(MicrosoftSecurityToken):
        def __init__(self, bearer, expires_in):
            super().__init__(None, None, None, None, None)
            self.bearer = bearer
            self.expires_in = expires_in

        async def _fetch_token(self):
            return (self.bearer, self.expires_in)

    class StubMicrosoftSecurityTokenWrongConfig(MicrosoftSecurityToken):
        def __init__(self, error_code, message=None):
            super().__init__(None, None, None, None, None)
            self.error_code = error_code
            self.message = message

        async def _fetch_token(self):
            error = ClientResponseError(None, None)
            error.status = self.error_code
            error.message = self.message

            raise error

    @pytest.mark.asyncio
    async def test_fetch_token_raises_not_implemented_error(self):
        with pytest.raises(NotImplementedError) as e:
            mst = MicrosoftSecurityToken(None, None, None, None, None)

            await mst._fetch_token()

        assert e is not None

    @pytest.mark.asyncio
    async def test_get_returns_results_from_fetch_token(self):
        bearer = "something"
        expires_in = 0.1

        token = TestMicrosoftSecurityToken.StubMicrosoftSecurityToken(
            bearer, expires_in
        )

        actual = await token.get()

        assert actual == bearer

    @pytest.mark.asyncio
    async def test_get_returns_cached_value_when_token_did_not_expire(self):
        original_bearer = "something"
        updated_bearer = "another"
        expires_in = 1

        token = TestMicrosoftSecurityToken.StubMicrosoftSecurityToken(
            original_bearer, expires_in
        )

        first_bearer = await token.get()
        token.bearer = updated_bearer

        second_bearer = await token.get()

        assert first_bearer == second_bearer
        assert second_bearer == original_bearer

    @pytest.mark.asyncio
    async def test_get_returns_new_value_when_token_expired(self):
        original_bearer = "something"
        updated_bearer = "another"
        expires_in = 0.01

        token = TestMicrosoftSecurityToken.StubMicrosoftSecurityToken(
            original_bearer, expires_in
        )

        first_bearer = await token.get()
        token.bearer = updated_bearer

        await asyncio.sleep(expires_in + 0.01)

        second_bearer = await token.get()

        assert first_bearer != second_bearer
        assert second_bearer == updated_bearer

    @pytest.mark.asyncio
    async def test_get_raises_correct_exception_when_400(self):
        token = TestMicrosoftSecurityToken.StubMicrosoftSecurityTokenWrongConfig(400)

        with pytest.raises(TokenFetchFailed) as e:
            await token.get()

        # Assert that message has field names in UI
        assert e.match("Tenant Id")
        assert e.match("Tenant Name")
        assert e.match("Client ID")

    @pytest.mark.asyncio
    async def test_get_raises_correct_exception_when_401(self):
        token = TestMicrosoftSecurityToken.StubMicrosoftSecurityTokenWrongConfig(401)

        with pytest.raises(TokenFetchFailed) as e:
            await token.get()

        # Assert that message has field names in UI
        assert e.match("Secret Value")

    @pytest.mark.asyncio
    async def test_get_raises_correct_exception_when_any_other_status(self):
        message = "Internal server error"
        token = TestMicrosoftSecurityToken.StubMicrosoftSecurityTokenWrongConfig(
            500, message
        )

        with pytest.raises(TokenFetchFailed) as e:
            await token.get()

        # Assert that message has field names in UI
        assert e.match(message)


class TestGraphAPIToken:
    @pytest_asyncio.fixture
    async def token(self):
        session = aiohttp.ClientSession()

        yield GraphAPIToken(session, None, None, None, None)

        await session.close()

    @pytest.mark.asyncio
    async def test_fetch_token(self, token, mock_responses):
        bearer = "hello"
        expires_in = 15

        mock_responses.post(
            re.compile(".*"),
            payload={"access_token": bearer, "expires_in": str(expires_in)},
        )

        actual_token, actual_expires_in = await token._fetch_token()

        assert actual_token == bearer
        assert actual_expires_in == expires_in

    @pytest.mark.asyncio
    async def test_fetch_token_retries(self, token, mock_responses, patch_sleep):
        bearer = "hello"
        expires_in = 15

        first_request_error = ClientResponseError(None, None)
        first_request_error.status = 500
        first_request_error.message = "Something went wrong"

        mock_responses.post(re.compile(".*"), exception=first_request_error)

        mock_responses.post(
            re.compile(".*"),
            payload={"access_token": bearer, "expires_in": str(expires_in)},
        )

        actual_token, actual_expires_in = await token._fetch_token()

        assert actual_token == bearer
        assert actual_expires_in == expires_in


class TestSharepointRestAPIToken:
    @pytest_asyncio.fixture
    async def token(self):
        session = aiohttp.ClientSession()

        yield SharepointRestAPIToken(session, None, None, None, None)

        await session.close()

    @pytest.mark.asyncio
    async def test_fetch_token(self, token, mock_responses):
        bearer = "hello"
        expires_in = 15

        mock_responses.post(
            re.compile(".*"),
            payload={"access_token": bearer, "expires_in": str(expires_in)},
        )

        actual_token, actual_expires_in = await token._fetch_token()

        assert actual_token == bearer
        assert actual_expires_in == expires_in

    # This test is a duplicate of test for TestGraphAPIToken.
    # When we introduce reusable retryable function instead of a wrapper
    # Then this test can be removed
    @pytest.mark.asyncio
    async def test_fetch_token_retries(self, token, mock_responses, patch_sleep):
        bearer = "hello"
        expires_in = 15

        first_request_error = ClientResponseError(None, None)
        first_request_error.status = 500
        first_request_error.message = "Something went wrong"

        mock_responses.post(re.compile(".*"), exception=first_request_error)

        mock_responses.post(
            re.compile(".*"),
            payload={"access_token": bearer, "expires_in": str(expires_in)},
        )

        actual_token, actual_expires_in = await token._fetch_token()

        assert actual_token == bearer
        assert actual_expires_in == expires_in


class TestMicrosoftAPISession:
    class StubAPIToken:
        async def get(self):
            return "something"

    @pytest_asyncio.fixture
    async def microsoft_api_session(self):
        session = aiohttp.ClientSession()
        yield MicrosoftAPISession(
            session,
            TestMicrosoftAPISession.StubAPIToken(),
            self.scroll_field,
            logger,
        )
        await session.close()

    @property
    def scroll_field(self):
        return "next_link"

    @pytest.mark.asyncio
    async def test_fetch(self, microsoft_api_session, mock_responses):
        url = "http://localhost:1234/url"
        payload = {"test": "hello world"}

        mock_responses.get(url, payload=payload)

        response = await microsoft_api_session.fetch(url)

        assert response == payload

    @pytest.mark.asyncio
    async def test_post(self, microsoft_api_session, mock_responses):
        url = "http://localhost:1234/url"
        expected_response = {"test": "hello world"}
        payload = {"key": "value"}

        mock_responses.post(url, payload=expected_response)

        response = await microsoft_api_session.post(url, payload)

        assert response == expected_response

    @pytest.mark.asyncio
    async def test_post_with_batch_failures(
        self, microsoft_api_session, mock_responses, patch_cancellable_sleeps
    ):
        url = "https://graph.microsoft.com/v1.0/$batch"
        first_response = BATCH_THROTTLED_RESPONSE
        second_response = {"responses": [{"key": "value"}]}
        payload = {"key": "value"}

        mock_responses.post(url, payload=first_response)
        mock_responses.post(url, payload=second_response)

        response = await microsoft_api_session.post(url, payload)

        assert response == second_response

    @pytest.mark.asyncio
    async def test_post_with_consecutive_batch_failures(
        self, microsoft_api_session, mock_responses, patch_cancellable_sleeps
    ):
        url = "https://graph.microsoft.com/v1.0/$batch"
        payload = {"key": "value"}

        mock_responses.post(url, payload=BATCH_THROTTLED_RESPONSE)
        mock_responses.post(url, payload=BATCH_THROTTLED_RESPONSE)
        mock_responses.post(url, payload=BATCH_THROTTLED_RESPONSE)
        mock_responses.post(url, payload=BATCH_THROTTLED_RESPONSE)
        mock_responses.post(url, payload=BATCH_THROTTLED_RESPONSE)

        with pytest.raises(ThrottledError):
            await microsoft_api_session.post(url, payload)

    @pytest.mark.asyncio
    async def test_fetch_with_client_payload_error(
        self, microsoft_api_session, patch_sleep
    ):
        url = "http://localhost:1234/url"
        response_mock = AsyncMock()
        response_mock.status = 400
        response_mock.ok = False
        response_mock.read = AsyncMock(return_value="Failed to read body")
        response_mock.json = AsyncMock(
            # return_value = {}
            side_effect=[aiohttp.client_exceptions.ClientPayloadError("something")]
        )

        async_response = AsyncMock()
        async_response.__aenter__ = AsyncMock(return_value=response_mock)

        with patch("aiohttp.client.ClientSession.get", return_value=async_response):
            await microsoft_api_session.fetch(url)

    @pytest.mark.asyncio
    async def test_fetch_with_retry(
        self, microsoft_api_session, mock_responses, patch_sleep
    ):
        url = "http://localhost:1234/url"
        payload = {"test": "hello world"}

        first_request_error = ClientResponseError(None, None)
        first_request_error.status = 500
        first_request_error.message = "Something went wrong"

        # First error out, then on request to same resource return good payload
        mock_responses.get(url, exception=first_request_error)
        mock_responses.get(url, payload=payload)

        response = await microsoft_api_session.fetch(url)

        assert response == payload

    @pytest.mark.asyncio
    async def test_scroll(self, microsoft_api_session, mock_responses):
        url = "http://localhost:1234/url"
        first_page = ["1", "2", "3"]

        next_url = "http://localhost:1234/url/page-two"
        second_page = ["4", "5", "6"]

        first_payload = {"value": first_page, self.scroll_field: next_url}
        second_payload = {"value": second_page}

        mock_responses.get(url, payload=first_payload)
        mock_responses.get(next_url, payload=second_payload)

        pages = []

        async for page in microsoft_api_session.scroll(url):
            pages.append(page)

        assert first_page in pages
        assert second_page in pages

    @pytest.mark.asyncio
    async def test_scroll_delta_url_with_data_link(
        self, microsoft_api_session, mock_responses
    ):
        drive_item = {
            "id": "1",
            "size": 15,
            "lastModifiedDateTime": str(datetime.now(timezone.utc)),
            "parentReference": {"driveId": "drive-1"},
            "_tempfile_suffix": ".txt",
        }

        responses = {
            "page1": {
                "payload": {
                    "value": [drive_item],
                    "@odata.nextLink": "http://fakesharepointonline/page2",  # this makes scroll to just to another link
                },
                "url": "http://fakesharepointonline/page1",
            },
            "page2": {
                "payload": {
                    "value": [drive_item],
                    "@odata.deltaLink": "http://fakesharepointonline/deltaLink",
                },
                "url": "http://fakesharepointonline/page2",
            },
        }

        for response in responses.values():
            mock_responses.get(response["url"], payload=response["payload"])

        pages = []

        async for page in microsoft_api_session.scroll_delta_url(
            url=responses["page1"]["url"]
        ):
            pages.append(page)

        assert len(pages) == len(responses)

    @pytest.mark.asyncio
    async def test_pipe(self, microsoft_api_session, mock_responses):
        class AsyncStream:
            def __init__(self):
                self.stream = BytesIO()

            async def write(self, data):
                self.stream.write(data)

            def read(self):
                return self.stream.getvalue().decode()

        url = "http://localhost:1234/download-some-sample-file"
        file_content = "hello world, this is content of downloaded file"
        stream = AsyncStream()
        mock_responses.get(url, body=file_content)

        await microsoft_api_session.pipe(url, stream)

        assert stream.read() == file_content

    @pytest.mark.asyncio
    async def test_call_api_with_429(
        self,
        microsoft_api_session,
        mock_responses,
        patch_sleep,
        patch_cancellable_sleeps,
    ):
        url = "http://localhost:1234/download-some-sample-file"
        payload = {"hello": "world"}
        retry_after = 25

        # First throttle, then do not throttle
        first_request_error = ClientResponseError(None, None)
        first_request_error.status = 429
        first_request_error.message = "Something went wrong"
        first_request_error.headers = {"Retry-After": str(retry_after)}

        mock_responses.get(url, exception=first_request_error)
        mock_responses.get(url, payload=payload)

        async with microsoft_api_session._get(url) as response:
            actual_payload = await response.json()
            assert actual_payload == payload

        patch_cancellable_sleeps.assert_awaited_with(retry_after)

    @pytest.mark.asyncio
    async def test_call_api_with_client_payload_error(
        self,
        microsoft_api_session,
        mock_responses,
        patch_sleep,
        patch_cancellable_sleeps,
    ):
        url = "http://localhost:1234/download-some-sample-file"
        payload = {"hello": "world"}
        retry_after = DEFAULT_RETRY_SECONDS

        # First throttle, then do not throttle
        first_request_error = ClientPayloadError(None, None)

        mock_responses.get(url, exception=first_request_error)
        mock_responses.get(url, payload=payload)

        async with microsoft_api_session._get(url) as response:
            actual_payload = await response.json()
            assert actual_payload == payload

        patch_cancellable_sleeps.assert_awaited_with(retry_after)

    @pytest.mark.asyncio
    async def test_call_api_with_429_without_retry_after(
        self,
        microsoft_api_session,
        mock_responses,
        patch_sleep,
        patch_cancellable_sleeps,
    ):
        url = "http://localhost:1234/download-some-sample-file"
        payload = {"hello": "world"}

        # First throttle, then do not throttle
        first_request_error = ClientResponseError(None, None)
        first_request_error.status = 429
        first_request_error.message = "Something went wrong"

        mock_responses.get(url, exception=first_request_error)
        mock_responses.get(url, payload=payload)

        async with microsoft_api_session._get(url) as response:
            actual_payload = await response.json()
            assert actual_payload == payload

        patch_cancellable_sleeps.assert_awaited_with(DEFAULT_RETRY_SECONDS)

    @pytest.mark.asyncio
    async def test_call_api_with_429_with_retry_after_and_backoff(
        self,
        microsoft_api_session,
        mock_responses,
        patch_sleep,
        patch_cancellable_sleeps,
    ):
        url = "http://localhost:1234/download-some-sample-file"
        payload = {"hello": "world"}

        # First throttle, then do not throttle
        first_request_error = ClientResponseError(None, None)
        first_request_error.status = 429
        first_request_error.message = "Something went wrong"

        retry_count = 3
        for _i in range(retry_count):
            mock_responses.get(url, exception=first_request_error)

        mock_responses.get(url, payload=payload)

        async with microsoft_api_session._get(url) as response:
            actual_payload = await response.json()
            assert actual_payload == payload

        patch_cancellable_sleeps.assert_awaited_with(
            DEFAULT_RETRY_SECONDS + DEFAULT_BACKOFF_MULTIPLIER * retry_count
        )

    @pytest.mark.asyncio
    async def test_call_api_with_403(
        self,
        microsoft_api_session,
        mock_responses,
        patch_sleep,
        patch_cancellable_sleeps,
    ):
        url = "http://localhost:1234/download-some-sample-file"

        # First throttle, then do not throttle
        unauthorized_error = ClientResponseError(None, None)
        unauthorized_error.status = 403
        unauthorized_error.message = "Something went wrong"

        mock_responses.get(url, exception=unauthorized_error)
        mock_responses.get(url, exception=unauthorized_error)
        mock_responses.get(url, exception=unauthorized_error)
        mock_responses.get(url, exception=unauthorized_error)
        mock_responses.get(url, exception=unauthorized_error)

        with pytest.raises(PermissionsMissing) as e:
            async with microsoft_api_session._get(url) as _:
                pass

        assert e is not None

    @pytest.mark.asyncio
    async def test_call_api_with_404_with_retry_after_header(
        self,
        microsoft_api_session,
        mock_responses,
        patch_sleep,
        patch_cancellable_sleeps,
    ):
        url = "http://localhost:1234/download-some-sample-file"
        payload = {"hello": "world"}
        retry_after = 25

        # First throttle, then do not throttle
        first_request_error = ClientResponseError(None, None)
        first_request_error.status = 404
        first_request_error.message = "Something went wrong"
        first_request_error.headers = {"Retry-After": str(retry_after)}

        mock_responses.get(url, exception=first_request_error)
        mock_responses.get(url, payload=payload)

        with pytest.raises(NotFound) as e:
            async with microsoft_api_session._get(url) as _:
                pass

        assert e is not None

    @pytest.mark.asyncio
    async def test_call_api_with_404_without_retry_after_header(
        self,
        microsoft_api_session,
        mock_responses,
        patch_sleep,
        patch_cancellable_sleeps,
    ):
        url = "http://localhost:1234/download-some-sample-file"

        # First throttle, then do not throttle
        not_found_error = ClientResponseError(None, None)
        not_found_error.status = 404
        not_found_error.message = "Something went wrong"

        mock_responses.get(url, exception=not_found_error)
        mock_responses.get(url, exception=not_found_error)
        mock_responses.get(url, exception=not_found_error)
        mock_responses.get(url, exception=not_found_error)
        mock_responses.get(url, exception=not_found_error)

        with pytest.raises(NotFound) as e:
            async with microsoft_api_session._get(url) as _:
                pass

        assert e is not None

    @pytest.mark.asyncio
    async def test_call_api_with_400_without_retry_after_header(
        self,
        microsoft_api_session,
        mock_responses,
        patch_sleep,
        patch_cancellable_sleeps,
    ):
        url = "http://localhost:1234/download-some-sample-file"

        # First throttle, then do not throttle
        bad_request_error = ClientResponseError(None, None)
        bad_request_error.status = 400
        bad_request_error.message = "You did something wrong"

        mock_responses.get(url, exception=bad_request_error)

        with pytest.raises(BadRequestError) as e:
            async with microsoft_api_session._get(url) as _:
                pass

        assert e is not None

    @pytest.mark.asyncio
    async def test_call_api_with_unhandled_status(
        self,
        microsoft_api_session,
        mock_responses,
        patch_sleep,
        patch_cancellable_sleeps,
    ):
        url = "http://localhost:1234/download-some-sample-file"

        error_message = "Something went wrong"

        # First throttle, then do not throttle
        not_found_error = ClientResponseError(MagicMock(), MagicMock())
        not_found_error.status = 420
        not_found_error.message = error_message

        mock_responses.get(url, exception=not_found_error)
        mock_responses.get(url, exception=not_found_error)
        mock_responses.get(url, exception=not_found_error)
        mock_responses.get(url, exception=not_found_error)
        mock_responses.get(url, exception=not_found_error)

        with pytest.raises(ClientResponseError) as e:
            async with microsoft_api_session._get(url) as _:
                pass

        assert e.match(error_message)


class TestSharepointOnlineClient:
    @property
    def tenant_id(self):
        return "tid"

    @property
    def tenant_name(self):
        return "tname"

    @property
    def client_id(self):
        return "cid"

    @property
    def client_secret(self):
        return "csecret"

    @pytest_asyncio.fixture
    async def client(self):
        # Patch close is passed here to also not do actual closing logic but instead
        # Do nothing when MicrosoftAPISession.close is called
        client = SharepointOnlineClient(
            self.tenant_id, self.tenant_name, self.client_id, self.client_secret
        )

        yield client
        await client.close()

    @pytest_asyncio.fixture
    def patch_fetch(self):
        with patch.object(
            MicrosoftAPISession, "fetch", return_value=AsyncMock()
        ) as fetch:
            yield fetch

    @pytest_asyncio.fixture
    def patch_post(self):
        with patch.object(
            MicrosoftAPISession, "post", return_value=AsyncMock()
        ) as post:
            yield post

    @pytest_asyncio.fixture
    async def patch_scroll(self):
        with patch.object(
            MicrosoftAPISession, "scroll", return_value=AsyncMock()
        ) as scroll:
            yield scroll

    @pytest_asyncio.fixture
    async def patch_scroll_delta_url(self):
        with patch.object(
            MicrosoftAPISession, "scroll_delta_url", return_value=AsyncMock()
        ) as scroll:
            yield scroll

    @pytest_asyncio.fixture
    async def patch_pipe(self):
        with patch.object(
            MicrosoftAPISession, "pipe", return_value=AsyncMock()
        ) as pipe:
            yield pipe

    async def _execute_scrolling_method(self, method, patch_scroll, setup_items, *args):
        half = len(setup_items) // 2
        patch_scroll.return_value = AsyncIterator(
            [setup_items[:half], setup_items[half:]]
        )  # simulate 2 pages

        returned_items = []
        async for item in method(*args):
            returned_items.append(item)

        return returned_items

    async def _test_scrolling_method_not_found(self, method, patch_scroll):
        patch_scroll.side_effect = NotFound()

        returned_items = []
        async for item in method():
            returned_items.append(item)

        assert len(returned_items) == 0

    @pytest.mark.asyncio
    async def test_groups(self, client, patch_scroll):
        actual_items = ["1", "2", "3", "4"]

        returned_items = await self._execute_scrolling_method(
            client.groups, patch_scroll, actual_items
        )

        assert len(returned_items) == len(actual_items)
        assert returned_items == actual_items

    @pytest.mark.asyncio
    async def test_group_sites(self, client, patch_scroll):
        group_id = "12345"

        actual_items = ["1", "2", "3", "4"]

        returned_items = await self._execute_scrolling_method(
            partial(client.group_sites, group_id), patch_scroll, actual_items
        )

        assert len(returned_items) == len(actual_items)
        assert returned_items == actual_items

    @pytest.mark.asyncio
    async def test_group_sites_not_found(self, client, patch_scroll):
        group_id = "12345"

        await self._test_scrolling_method_not_found(
            partial(client.group_sites, group_id), patch_scroll
        )

    @pytest.mark.asyncio
    async def test_site_collections(self, client, patch_scroll):
        first_site_collection = {
            "@odata.context": "https://graph.microsoft.com/v1.0/$metadata#sites/$entity",
            "createdDateTime": "2023-12-12T12:00:00.000Z",
            "description": "This is the first test site collection",
            "id": "site-id-1",
            "lastModifiedDateTime": "2023-12-12T12:00:00.000Z",
            "name": "fake-site-collection-1",
            "webUrl": "https://example.sharepoint.com",
            "displayName": "Fake Site Collection",
            "root": {},
            "siteCollection": {"hostname": "example.sharepoint.com"},
        }
        second_site_collection = {
            "@odata.context": "https://graph.microsoft.com/v1.0/$metadata#sites/$entity",
            "createdDateTime": "2023-12-12T12:00:00.000Z",
            "description": "This is the second test site collection",
            "id": "site-id-2",
            "lastModifiedDateTime": "2023-12-12T12:00:00.000Z",
            "name": "fake-site-collection-2",
            "webUrl": "https://example.sharepoint.com",
            "displayName": "Fake Site Collection",
            "root": {},
            "siteCollection": {"hostname": "example.sharepoint.com"},
        }

        patch_scroll.side_effect = AsyncIterator(
            [[first_site_collection, second_site_collection]]
        )
        returned_items = []
        async for site_collection in client.site_collections():
            returned_items.append(site_collection)

        assert len(returned_items) == 2
        assert returned_items == [first_site_collection, second_site_collection]

    @pytest.mark.asyncio
    async def test_site_collections_when_permission_missing(
        self, client, patch_fetch, patch_scroll
    ):
        actual_response = {
            "@odata.context": "https://graph.microsoft.com/v1.0/$metadata#sites/$entity",
            "createdDateTime": "2023-12-12T12:00:00.000Z",
            "description": "This is a test site collection",
            "id": "site-id",
            "lastModifiedDateTime": "2023-12-12T12:00:00.000Z",
            "name": "fake-site-collection",
            "webUrl": "https://example.sharepoint.com",
            "displayName": "Fake Site Collection",
            "root": {},
            "siteCollection": {"hostname": "example.sharepoint.com"},
        }

        patch_scroll.side_effect = PermissionsMissing()
        patch_fetch.side_effect = [actual_response]
        async for site_collection in client.site_collections():
            assert site_collection == actual_response

    @pytest.mark.asyncio
    async def test_sites_wildcard(self, client, patch_scroll):
        root_site = "root"
        actual_items = [
            {"name": "First"},
            {"name": "Second"},
            {"name": "Third"},
            {"name": "Fourth"},
        ]

        returned_items = await self._execute_scrolling_method(
            partial(client.sites, root_site, [WILDCARD]), patch_scroll, actual_items
        )

        assert len(returned_items) == len(actual_items)
        assert returned_items == actual_items

    @pytest.mark.asyncio
    async def test_sites_filter(self, client, patch_scroll):
        root_site = "root"
        actual_items = [
            {"name": "First"},
            {"name": "Second"},
            {"name": "Third"},
            {"name": "Fourth"},
        ]
        filter_ = ["First", "Third"]

        returned_items = await self._execute_scrolling_method(
            partial(client.sites, root_site, filter_), patch_scroll, actual_items
        )

        assert len(returned_items) == len(filter_)
        assert actual_items[0] in returned_items
        assert actual_items[2] in returned_items

    @pytest.mark.asyncio
    async def test_sites_filter_individually(self, client, patch_fetch):
        root_site = "root"
        actual_items = [
            {"name": "First"},
            {"name": "Third"},
        ]
        filter_ = ["First", "Third"]
        patch_fetch.side_effect = actual_items

        returned_items = []
        async for site in client.sites(
            root_site, filter_, enumerate_all_sites=False, fetch_subsites=False
        ):
            returned_items.append(site)

        assert len(returned_items) == len(filter_)
        assert actual_items == returned_items

    @pytest.mark.asyncio
    async def test_sites_filter_individually_plus_subsites(
        self, client, patch_fetch, patch_scroll
    ):
        root_site = "root"
        root_item = {
            "name": "First",
            "id": "first",
            "sites": [{"name": "Second"}],
        }
        sub_items = [
            AsyncIterator(
                [[{"name": "Second", "id": "second", "sites": [{"name": "Third"}]}]]
            ),
            AsyncIterator([[{"name": "Third", "id": "third"}]]),
        ]
        expected_items = [
            {"name": "First", "id": "first"},
            {"name": "Second", "id": "second"},
            {"name": "Third", "id": "third"},
        ]
        filter_ = ["First"]
        patch_fetch.side_effect = [root_item]
        patch_scroll.side_effect = sub_items

        returned_items = []
        async for site in client.sites(
            root_site, filter_, enumerate_all_sites=False, fetch_subsites=True
        ):
            returned_items.append(site)

        assert len(returned_items) == 3
        assert returned_items == expected_items

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "exception, raises",
        [
            (
                PermissionsMissing(),
                PermissionsMissing,
            ),
            (
                ClientResponseError(
                    status=400,
                    request_info=aiohttp.RequestInfo(
                        real_url="", method=None, headers=None, url=""
                    ),
                    history=None,
                ),
                ClientResponseError,
            ),
        ],
    )
    async def test_all_sites_with_error(self, client, patch_scroll, exception, raises):
        sharepoint_host = "example.sharepoint.com"
        patch_scroll.side_effect = exception
        with pytest.raises(raises):
            await anext(client._all_sites(sharepoint_host, []))

    @pytest.mark.asyncio
    async def test_site_drives(self, client, patch_scroll):
        site_id = "12345"

        actual_items = ["1", "2", "3", "4"]

        returned_items = await self._execute_scrolling_method(
            partial(client.site_drives, site_id), patch_scroll, actual_items
        )

        assert len(returned_items) == len(actual_items)
        assert returned_items == actual_items

    @pytest.mark.asyncio
    async def test_drive_items_delta(self, client, patch_fetch, patch_scroll_delta_url):
        delta_url_input = "https://sharepoint.com/delta-link-lalal"
        delta_url_next_page = "https://sharepoint.com/delta-link-lalal/page-2"
        delta_url_next_sync = "https://sharepoint.com/delta-link-lalal/next-sync"

        items_page_1 = ["1", "2"]
        items_page_2 = ["3", "4"]

        patch_scroll_delta_url.return_value = AsyncIterator(
            [
                {"@odata.nextLink": delta_url_next_page, "value": items_page_1},
                {"@odata.deltaLink": delta_url_next_sync, "value": items_page_2},
            ]
        )

        returned_drive_items_pages = []
        async for page in client.drive_items_delta(delta_url_input):
            returned_drive_items_pages.append(page)

        returned_drive_items = [
            item
            for drive_item_page in returned_drive_items_pages
            for item in drive_item_page
        ]

        assert len(returned_drive_items) == len(items_page_1) + len(items_page_2)
        assert returned_drive_items == items_page_1 + items_page_2

    @pytest.mark.asyncio
    async def test_drive_items(self, client, patch_fetch):
        drive_id = "12345"
        delta_url_next_sync = "https://sharepoint.com/delta-link-lalal/page-2"
        items_page_1 = ["1", "2"]
        items_page_2 = ["3", "4"]

        pages = AsyncIterator(
            [
                DriveItemsPage(items_page_1, delta_url_next_sync),
                DriveItemsPage(items_page_2, delta_url_next_sync),
            ]
        )
        returned_items = []

        with patch.object(client, "drive_items_delta", return_value=pages):
            async for page in client.drive_items(drive_id):
                for item in page:
                    returned_items.append(item)
                assert page.delta_link() == delta_url_next_sync

        assert len(returned_items) == len(items_page_1) + len(items_page_2)
        assert returned_items == items_page_1 + items_page_2

    @pytest.mark.asyncio
    async def test_download_drive_item(self, client, patch_pipe):
        """Basic setup for the test - no recursion through directories"""
        drive_id = "1"
        item_id = "2"
        async_buffer = MagicMock()

        await client.download_drive_item(drive_id, item_id, async_buffer)

        patch_pipe.assert_awaited_once_with(ANY, async_buffer)

    @pytest.mark.asyncio
    async def test_site_lists(self, client, patch_scroll):
        site_id = "12345"

        actual_items = ["1", "2", "3", "4"]

        returned_items = await self._execute_scrolling_method(
            partial(client.site_lists, site_id), patch_scroll, actual_items
        )

        assert len(returned_items) == len(actual_items)
        assert returned_items == actual_items

    @pytest.mark.asyncio
    async def test_site_list_items(self, client, patch_scroll):
        site_id = "12345"
        list_id = "54321"

        actual_items = ["1", "2", "3", "4"]

        returned_items = await self._execute_scrolling_method(
            partial(client.site_list_items, site_id, list_id),
            patch_scroll,
            actual_items,
        )

        assert len(returned_items) == len(actual_items)
        assert returned_items == actual_items

    @pytest.mark.asyncio
    async def test_site_list_item_attachments(self, client, patch_fetch):
        site_web_url = f"https://{self.tenant_name}.sharepoint.com"
        list_title = "Summer Vacation Notes"
        list_item_id = "1"

        actual_attachments = ["file.txt", "o-file.txt", "third.txt", "hll.wrd"]

        patch_fetch.return_value = {"AttachmentFiles": actual_attachments}

        returned_items = []
        async for attachment in client.site_list_item_attachments(
            site_web_url, list_title, list_item_id
        ):
            returned_items.append(attachment)

        assert len(returned_items) == len(actual_attachments)
        assert returned_items == actual_attachments

    @pytest.mark.asyncio
    async def test_site_list_item_attachments_not_found(self, client, patch_fetch):
        site_web_url = f"https://{self.tenant_name}.sharepoint.com"
        list_title = "Summer Vacation Notes"
        list_item_id = "1"

        patch_fetch.side_effect = NotFound()

        returned_items = []
        async for attachment in client.site_list_item_attachments(
            site_web_url, list_title, list_item_id
        ):
            returned_items.append(attachment)

        assert len(returned_items) == 0

    @pytest.mark.asyncio
    async def test_site_list_item_attachments_wrong_tenant(self, client):
        invalid_tenant_name = "something"
        site_web_url = f"https://{invalid_tenant_name}.sharepoint.com"
        list_title = "Summer Vacation Notes"
        list_item_id = "1"

        with pytest.raises(InvalidSharepointTenant) as e:
            async for _ in client.site_list_item_attachments(
                site_web_url, list_title, list_item_id
            ):
                pass

        # Assert error message contains both invalid and valid tenant name
        # cause that's what's important
        assert e.match(invalid_tenant_name)
        assert e.match(self.tenant_name)

    @pytest.mark.asyncio
    async def test_download_attachment(self, client, patch_pipe):
        attachment_path = f"https://{self.tenant_name}.sharepoint.com/thats/a/made/up/attachment/path.jpg"
        async_buffer = MagicMock()

        await client.download_attachment(attachment_path, async_buffer)

        patch_pipe.assert_awaited_once_with(ANY, async_buffer)

    @pytest.mark.asyncio
    async def test_download_attachment_wrong_tenant(self, client, patch_pipe):
        invalid_tenant_name = "something"
        attachment_path = f"https://{invalid_tenant_name}.sharepoint.com/thats/a/made/up/attachment/path.jpg"
        async_buffer = MagicMock()

        with pytest.raises(InvalidSharepointTenant) as e:
            await client.download_attachment(attachment_path, async_buffer)

        # Assert error message contains both invalid and valid tenant name
        # cause that's what's important
        assert e.match(invalid_tenant_name)
        assert e.match(self.tenant_name)

    @pytest.mark.asyncio
    async def test_site_pages(self, client, patch_scroll):
        page_url_path = f"https://{self.tenant_name}.sharepoint.com/random/totally/made/up/page.aspx"
        actual_items = [{"Id": "1"}, {"Id": "2"}, {"Id": "3"}, {"Id": "4"}]

        returned_items = await self._execute_scrolling_method(
            partial(client.site_pages, page_url_path), patch_scroll, actual_items
        )

        assert len(returned_items) == len(actual_items)
        assert [{"Id": i["Id"]} for i in returned_items] == actual_items

    @pytest.mark.asyncio
    async def test_site_pages_not_found(self, client, patch_scroll):
        page_url_path = f"https://{self.tenant_name}.sharepoint.com/random/totally/made/up/page.aspx"

        patch_scroll.side_effect = NotFound()

        returned_items = []
        async for site_page in client.site_pages(page_url_path):
            returned_items.append(site_page)

        assert len(returned_items) == 0

    @pytest.mark.asyncio
    async def test_site_page_has_unique_role_assignments(self, client, patch_fetch):
        url = f"https://{self.tenant_name}.sharepoint.com"
        site_page_id = 1

        patch_fetch.return_value = {"value": True}

        assert await client.site_page_has_unique_role_assignments(url, site_page_id)

    @pytest.mark.asyncio
    async def test_site_page_has_unique_role_assignments_not_found(
        self, client, patch_fetch
    ):
        url = f"https://{self.tenant_name}.sharepoint.com"
        site_page_id = 1

        patch_fetch.side_effect = NotFound()

        assert not await client.site_page_has_unique_role_assignments(url, site_page_id)

    @pytest.mark.asyncio
    async def test_site_pages_wrong_tenant(self, client, patch_scroll):
        invalid_tenant_name = "something"
        page_url_path = f"https://{invalid_tenant_name}.sharepoint.com/random/totally/made/up/page.aspx"

        with pytest.raises(InvalidSharepointTenant) as e:
            async for _ in client.site_pages(page_url_path):
                pass

        # Assert error message contains both invalid and valid tenant name
        # cause that's what's important
        assert e.match(invalid_tenant_name)
        assert e.match(self.tenant_name)

    @pytest.mark.asyncio
    async def test_tenant_details(self, client, patch_fetch):
        http_call_result = {"hello": "world"}

        patch_fetch.return_value = http_call_result

        actual_result = await client.tenant_details()

        assert http_call_result == actual_result

    @pytest.mark.asyncio
    async def test_site_group_users(self, client, patch_scroll):
        site_group_id = 42
        site_groups_url = f"https://{self.tenant_name}.sharepoint.com/_api/web/sitegroups/getbyid({site_group_id})/users"
        users = [
            {
                "odata.type": "SP.User",
                "UserPrincipalName": SITEGROUP_USER_ONE_ID,
                "Email": SITEGROUP_USER_ONE_EMAIL,
            }
        ]

        patch_scroll.return_value = users

        actual_group = await self._execute_scrolling_method(
            client.site_groups_users,
            patch_scroll,
            users,
            site_groups_url,
            site_group_id,
        )

        assert actual_group == users

    @pytest.mark.asyncio
    async def test_site_group_not_found(self, client, patch_scroll):
        site_group_id = 42
        site_groups_url = f"https://{self.tenant_name}.sharepoint.com/_api/web/sitegroups/getbyid({site_group_id})/users"

        patch_scroll.side_effect = NotFound()

        returned_items = []
        async for item in client.site_groups_users(site_groups_url, site_group_id):
            returned_items.append(item)

        assert len(returned_items) == 0

    @pytest.mark.asyncio
    async def test_site_users(self, client, patch_scroll):
        site_users_url = f"https://{self.tenant_name}.sharepoint.com/random/totally/made/up/siteusers"
        users = ["user1", "user2"]

        actual_users = await self._execute_scrolling_method(
            client.site_users, patch_scroll, users, site_users_url
        )

        assert actual_users == users

    @pytest.mark.asyncio
    async def test_site_users_not_found(self, client, patch_scroll):
        site_users_url = f"https://{self.tenant_name}.sharepoint.com/random/totally/made/up/siteusers"
        patch_scroll.side_effect = NotFound()

        returned_items = []
        async for item in client.site_users(site_users_url):
            returned_items.append(item)

        assert len(returned_items) == 0

    @pytest.mark.asyncio
    async def test_drive_items_permissions_batch(self, client, patch_post):
        drive_id = 1
        drive_item_ids = [1, 2, 3]
        batch_response = {
            "responses": [{"id": drive_item_id} for drive_item_id in drive_item_ids]
        }
        expected_batch_request = {
            "requests": [
                {"id": drive_item_id, "method": ANY, "url": ANY}
                for drive_item_id in drive_item_ids
            ]
        }

        patch_post.return_value = batch_response

        response_ids = set()
        async for response in client.drive_items_permissions_batch(
            drive_id, drive_item_ids
        ):
            response_ids.add(response.get("id"))

        assert response_ids == set(drive_item_ids)
        client._graph_api_client.post.assert_awaited_with(ANY, expected_batch_request)

    @pytest.mark.asyncio
    async def test_drive_items_permissions_batch_not_found(self, client, patch_post):
        drive_id = 1
        drive_item_ids = [1, 2, 3]

        patch_post.side_effect = NotFound()

        responses = []
        async for response in client.drive_items_permissions_batch(
            drive_id, drive_item_ids
        ):
            responses.append(response)

        assert len(responses) == 0

    @pytest.mark.asyncio
    async def test_drive_items_permissions_batch_empty(self, client, patch_post):
        drive_id = 1
        drive_item_ids = []

        async for _response in client.drive_items_permissions_batch(
            drive_id, drive_item_ids
        ):
            msg = "we shouldn't get here"
            raise Exception(msg)

    @pytest.mark.asyncio
    async def test_site_role_assignments(self, client, patch_scroll):
        site_role_assignments_url = (
            f"https://{self.tenant_name}.sharepoint.com/sites/test"
        )

        role_assignments = [{"value": ["role"]}]

        actual_role_assignments = await self._execute_scrolling_method(
            partial(
                client.site_role_assignments,
                site_role_assignments_url,
            ),
            patch_scroll,
            role_assignments,
        )

        assert actual_role_assignments == role_assignments

    @pytest.mark.asyncio
    async def test_site_list_has_unique_role_assignments(self, client, patch_fetch):
        site_list_role_assignments_url = f"https://{self.tenant_name}.sharepoint.com/random/totally/made/up/roleassignments"
        site_list_name = "site_list"

        patch_fetch.return_value = {"value": True}

        assert await client.site_page_has_unique_role_assignments(
            site_list_role_assignments_url, site_list_name
        )

    @pytest.mark.asyncio
    async def test_site_list_has_unique_role_assignments_not_found(
        self, client, patch_fetch
    ):
        site_list_role_assignments_url = f"https://{self.tenant_name}.sharepoint.com/random/totally/made/up/roleassignments"
        site_list_name = "site_list"

        patch_fetch.side_effect = NotFound()

        assert not await client.site_page_has_unique_role_assignments(
            site_list_role_assignments_url, site_list_name
        )

    @pytest.mark.asyncio
    async def test_site_list_role_assignments(self, client, patch_scroll):
        site_list_role_assignments_url = f"https://{self.tenant_name}.sharepoint.com/random/totally/made/up/roleassignments"
        site_list_name = "site_list"

        role_assignments = [{"value": ["role"]}]

        actual_role_assignments = await self._execute_scrolling_method(
            partial(
                client.site_page_role_assignments,
                site_list_role_assignments_url,
                site_list_name,
            ),
            patch_scroll,
            role_assignments,
        )

        assert actual_role_assignments == role_assignments

    @pytest.mark.asyncio
    async def test_site_list_role_assignments_not_found(self, client, patch_scroll):
        site_list_role_assignments_url = f"https://{self.tenant_name}.sharepoint.com/random/totally/made/up/roleassignments"
        site_list_name = "site_list"

        patch_scroll.side_effect = NotFound

        role_assignments = []
        async for role_assignment in client.site_list_role_assignments(
            site_list_role_assignments_url, site_list_name
        ):
            role_assignments.append(role_assignment)

        assert len(role_assignments) == 0

    @pytest.mark.asyncio
    async def test_site_list_item_has_unique_role_assignments(
        self, client, patch_fetch
    ):
        site_list_item_role_assignments_url = f"https://{self.tenant_name}.sharepoint.com/random/totally/made/up/roleassignments"
        list_title = "list_title"
        list_item_id = 1

        patch_fetch.return_value = {"value": True}

        assert await client.site_list_item_has_unique_role_assignments(
            site_list_item_role_assignments_url, list_title, list_item_id
        )

    @pytest.mark.asyncio
    async def test_site_list_item_has_unique_role_assignments_not_found(
        self, client, patch_fetch
    ):
        site_list_item_role_assignments_url = f"https://{self.tenant_name}.sharepoint.com/random/totally/made/up/roleassignments"
        list_title = "list_title"
        list_item_id = 1

        patch_fetch.side_effect = NotFound()

        assert not await client.site_list_item_has_unique_role_assignments(
            site_list_item_role_assignments_url, list_title, list_item_id
        )

    @pytest.mark.asyncio
    async def test_site_list_item_has_unique_role_assignments_bad_request(
        self, client, patch_fetch
    ):
        site_list_item_role_assignments_url = f"https://{self.tenant_name}.sharepoint.com/random/totally/made/up/roleassignments"
        list_title = "list_title"
        list_item_id = 1

        patch_fetch.side_effect = BadRequestError()

        assert not await client.site_list_item_has_unique_role_assignments(
            site_list_item_role_assignments_url, list_title, list_item_id
        )

    @pytest.mark.asyncio
    async def test_site_list_item_role_assignments(self, client, patch_scroll):
        site_list_item_role_assignments_url = f"https://{self.tenant_name}.sharepoint.com/random/totally/made/up/roleassignments"
        list_title = "list_title"
        list_item_id = 1

        role_assignments = [{"value": ["role"]}]

        actual_role_assignments = await self._execute_scrolling_method(
            partial(
                client.site_list_item_role_assignments,
                site_list_item_role_assignments_url,
                list_title,
                list_item_id,
            ),
            patch_scroll,
            role_assignments,
        )

        assert actual_role_assignments == role_assignments

    @pytest.mark.asyncio
    async def test_site_list_item_role_assignments_not_found(
        self, client, patch_scroll
    ):
        site_list_item_role_assignments_url = f"https://{self.tenant_name}.sharepoint.com/random/totally/made/up/roleassignments"
        list_title = "list_title"
        list_item_id = 1

        patch_scroll.side_effect = NotFound

        role_assignments = []
        async for role_assignment in client.site_list_item_role_assignments(
            site_list_item_role_assignments_url, list_title, list_item_id
        ):
            role_assignments.append(role_assignment)

        assert len(role_assignments) == 0

    @pytest.mark.asyncio
    async def test_site_page_role_assignments(self, client, patch_scroll):
        site_web_url = f"https://{self.tenant_name}.sharepoint.com/random/totally/made/up/roleassignments"
        site_page_id = 1
        role_assignments = [{"value": ["role"]}]

        actual_role_assignments = await self._execute_scrolling_method(
            partial(client.site_page_role_assignments, site_web_url, site_page_id),
            patch_scroll,
            role_assignments,
        )

        assert actual_role_assignments == role_assignments

    @pytest.mark.asyncio
    async def test_site_page_role_assignments_not_found(self, client, patch_scroll):
        site_page_role_assignments_url = f"https://{self.tenant_name}.sharepoint.com/random/totally/made/up/roleassignments"
        site_page_id = 1

        patch_scroll.side_effect = NotFound

        returned_items = []
        async for item in client.site_page_role_assignments(
            site_page_role_assignments_url, site_page_id
        ):
            returned_items.append(item)

        assert len(returned_items) == 0

    @pytest.mark.asyncio
    async def test_users_and_groups_for_role_assignment(self, client, patch_fetch):
        users_by_id_url = (
            f"https://{self.tenant_name}.sharepoint.com/random/totally/made/up/users"
        )
        role_assignment = {"name": "role", "PrincipalId": 1}
        users_and_groups = ["user", "group"]

        patch_fetch.return_value = users_and_groups

        actual_users_and_groups = await client.users_and_groups_for_role_assignment(
            users_by_id_url, role_assignment
        )

        assert actual_users_and_groups == users_and_groups

    @pytest.mark.asyncio
    async def test_users_and_groups_for_role_assignment_not_found(
        self, client, patch_fetch
    ):
        users_by_id_url = (
            f"https://{self.tenant_name}.sharepoint.com/random/totally/made/up/users"
        )
        role_assignment = {"name": "role", "PrincipalId": 1}

        patch_fetch.side_effect = NotFound

        users_and_groups = await client.users_and_groups_for_role_assignment(
            users_by_id_url, role_assignment
        )

        assert len(users_and_groups) == 0

    @pytest.mark.asyncio
    async def test_users_and_groups_for_role_assignment_internal_server_error(
        self, client, patch_fetch
    ):
        users_by_id_url = (
            f"https://{self.tenant_name}.sharepoint.com/random/totally/made/up/users"
        )
        role_assignment = {"name": "role", "PrincipalId": 1}

        patch_fetch.side_effect = InternalServerError

        users_and_groups = await client.users_and_groups_for_role_assignment(
            users_by_id_url, role_assignment
        )

        assert len(users_and_groups) == 0

    @pytest.mark.asyncio
    async def test_users_and_groups_for_role_assignment_missing_principal_id(
        self, client, patch_fetch
    ):
        users_by_id_url = (
            f"https://{self.tenant_name}.sharepoint.com/random/totally/made/up/users"
        )

        # missing principal id
        role_assignment = {"name": "role"}
        users_and_groups = ["user", "group"]

        patch_fetch.return_value = users_and_groups

        actual_users_and_groups = await client.users_and_groups_for_role_assignment(
            users_by_id_url, role_assignment
        )

        assert len(actual_users_and_groups) == 0

    @pytest.mark.asyncio
    async def test_groups_user_transitive_member_of(self, client, patch_scroll):
        user_id = "12345"
        group_one = {"name": "some group"}
        group_two = {"name": "some other group"}

        expected_groups = [group_one, group_two]

        actual_groups = await self._execute_scrolling_method(
            partial(client.groups_user_transitive_member_of, user_id),
            patch_scroll,
            expected_groups,
        )

        assert len(actual_groups) == len(expected_groups)
        assert actual_groups == expected_groups

    @pytest.mark.asyncio
    async def test_groups_user_transitive_member_of_with_not_found_raised(
        self, client, patch_scroll
    ):
        user_id = "12345"
        patch_scroll.side_effect = NotFound()

        returned_groups = []
        async for groups in client.groups_user_transitive_member_of(user_id):
            returned_groups.append(groups)

        assert len(returned_groups) == 0

    @pytest.mark.asyncio
    async def test_group_members(self, client, patch_scroll):
        group_id = "12345"
        member_one = {"name": "some member"}
        member_two = {"name": "some other member"}

        expected_members = [member_one, member_two]

        actual_members = await self._execute_scrolling_method(
            partial(client.group_members, group_id),
            patch_scroll,
            expected_members,
        )

        assert len(actual_members) == len(expected_members)
        assert actual_members == expected_members

    @pytest.mark.asyncio
    async def test_group_members_with_not_found_raised(self, client, patch_scroll):
        group_id = "12345"
        patch_scroll.side_effect = NotFound()

        returned_members = []
        async for member in client.group_members(group_id):
            returned_members.append(member)

        assert len(returned_members) == 0

    @pytest.mark.asyncio
    async def test_group_owners(self, client, patch_scroll):
        group_id = "12345"
        owner_one = {"name": "some owner"}
        owner_two = {"name": "some other owner"}

        expected_owners = [owner_one, owner_two]

        actual_owners = await self._execute_scrolling_method(
            partial(client.group_owners, group_id),
            patch_scroll,
            expected_owners,
        )

        assert len(actual_owners) == len(expected_owners)
        assert actual_owners == expected_owners

    @pytest.mark.asyncio
    async def test_group_owners_with_not_found_raised(self, client, patch_scroll):
        group_id = "12345"
        patch_scroll.side_effect = NotFound()

        returned_owners = []
        async for owner in client.group_owners(group_id):
            returned_owners.append(owner)

        assert len(returned_owners) == 0


class TestSharepointOnlineAdvancedRulesValidator:
    @pytest_asyncio.fixture
    def validator(self):
        return SharepointOnlineAdvancedRulesValidator()

    @pytest.mark.asyncio
    async def test_validate(self, validator):
        valid_rules = {"skipExtractingDriveItemsOlderThan": 15}

        result = await validator.validate(valid_rules)

        assert result.is_valid

    @pytest.mark.asyncio
    async def test_validate_invalid_rule(self, validator):
        invalid_rules = {"skipExtractingDriveItemsOlderThan": "why is this a string"}

        result = await validator.validate(invalid_rules)

        assert not result.is_valid


class TestSharepointOnlineDataSource:
    @property
    def today(self):
        return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    @property
    def day_ago(self):
        return (datetime.now(timezone.utc) - timedelta(days=1)).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )

    @property
    def month_ago(self):
        return (datetime.now(timezone.utc) - timedelta(days=30)).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )

    @property
    def two_months_ago(selfself):
        return (datetime.now(timezone.utc) - timedelta(days=60)).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )

    @property
    def site_collections(self):
        return [
            {
                "siteCollection": {"hostname": "test.sharepoint.com"},
                "webUrl": "https://test.sharepoint.com",
                "lastModifiedDateTime": self.day_ago,
            }
        ]

    @property
    def sites(self):
        return [
            {
                "id": "1",
                "webUrl": "https://test.sharepoint.com/sites/site_1",
                "name": "site-1",
                "siteCollection": self.site_collections[0]["siteCollection"],
                "lastModifiedDateTime": self.day_ago,
            }
        ]

    @property
    def site_drives(self):
        return [{"id": "2", "lastModifiedDateTime": self.day_ago}]

    @property
    def drive_items(self):
        return [
            DriveItemsPage(
                items=[
                    {
                        "id": "3",
                        "name": "third.txt",
                        "lastModifiedDateTime": self.month_ago,
                    },
                    {
                        "id": "4",
                        "name": "fourth.txt",
                        "lastModifiedDateTime": self.day_ago,
                    },
                ],
                delta_link="deltalinksample",
            )
        ]

    @property
    def site_lists(self):
        return [
            {
                "id": SITE_LIST_ONE_ID,
                "name": SITE_LIST_ONE_NAME,
                "lastModifiedDateTime": self.day_ago,
            }
        ]

    @property
    def site_list_has_unique_role_assignments(self):
        return True

    @property
    def site_list_items(self):
        return [
            {
                "id": "6",
                "contentType": {"name": "Item"},
                "fields": {"Attachments": ""},
                "lastModifiedDateTime": self.month_ago,
            },
            {
                "id": "7",
                "contentType": {"name": "Web Template Extensions"},
                "fields": {},
                "lastModifiedDateTime": self.day_ago,
            },  # Will be ignored!!!
            {
                "id": "8",
                "contentType": {"name": "Something without attachments"},
                "fields": {},
                "lastModifiedDateTime": self.two_months_ago,
            },
        ]

    @property
    def site_list_item_attachments(self):
        return [
            {"odata.id": "9", "name": "attachment 1.txt"},
            {"odata.id": "10", "name": "attachment 2.txt"},
        ]

    @property
    def site_pages(self):
        return [
            {
                "Id": "4",
                "odata.id": "11",
                "GUID": "thats-not-a-guid",
                "Modified": self.day_ago,
            }
        ]

    @property
    def site_role_assignments(self):
        return [
            {
                "Member": {
                    "odata.type": "SP.Group",
                    "Users": [
                        {
                            "odata.type": "SP.User",
                            "UserPrincipalName": USER_ONE_EMAIL,
                        },
                        {
                            "odata.type": "SP.User",
                            "Email": USER_TWO_EMAIL,
                        },
                        {
                            "odata.type": "SP.User",
                            "LoginName": f"c:0o.c|federateddirectoryclaimprovider|{GROUP_ONE_ID}",
                            "Title": GROUP_ONE,
                            "Email": f"{GROUP_ONE}@foobar.onmicrosoft.com",
                        },
                        {
                            "odata.type": "SP.User",
                            "LoginName": f"c:0o.c|federateddirectoryclaimprovider|{GROUP_TWO_ID}",
                            "Title": GROUP_TWO,
                            "Email": f"{GROUP_TWO}@foobar.onmicrosoft.com",
                        },
                    ],
                    "LoginName": "Site Members",
                    "Title": "Site Members",
                },
                "RoleDefinitionBindings": READ_BINDING,
            }
        ]

    @property
    def site_admins(self):
        return [
            {
                "LoginName": "c:0o.c|federateddirectoryclaimprovider|97d055cf-5cdf-4e5e-b383-f01ed3a8844d_o",
                "Email": "Sean'sTeamSite@enterprisesearch.onmicrosoft.com",
            },
            {
                "LoginName": "c:0t.c|tenant|78b2fb13-4ef2-4132-96c6-84c1a58e2bdf",
            },
        ]

    @property
    def group_members(self):
        return [
            {
                "mail": MEMBER_ONE_EMAIL,
            },
            {"userPrincipalName": MEMBER_TWO_USER_PRINCIPAL_NAME},
        ]

    @property
    def group_owners(self):
        return [
            {"mail": OWNER_ONE_EMAIL},
            {"userPrincipalName": OWNER_TWO_USER_PRINCIPAL_NAME},
        ]

    @property
    def group(self):
        return {"id": GROUP_ONE_ID}

    @property
    def site_users(self):
        return [
            {"UserPrincipalName": USER_ONE_EMAIL},
            {"UserPrincipalName": USER_TWO_EMAIL},
            {},
            {"UserPrincipalName": None},
        ]

    @property
    def site_list_role_assignments(self):
        return {"value": ["role"]}

    @property
    def site_group_users(self):
        return SAMPLE_SITE_GROUP_USERS

    @property
    def users_and_groups_for_role_assignments(self):
        return [USER_ONE_EMAIL, GROUP_ONE]

    @property
    def site_list_item_role_assignments(self):
        return {"value": ["role"]}

    @property
    def site_list_item_has_unique_role_assignments(self):
        return True

    @property
    def site_page_has_unique_role_assignments(self):
        return True

    @property
    def site_page_role_assignments(self):
        return {"value": ["role"]}

    @property
    def graph_api_token(self):
        return "graph bearer"

    @property
    def rest_api_token(self):
        return "rest bearer"

    @property
    def valid_tenant(self):
        return {"NameSpaceType": "VALID"}

    @property
    def drive_items_delta(self):
        return [
            DriveItemsPage(
                items=[
                    {
                        "id": "3",
                        "name": "third",
                        "lastModifiedDateTime": self.month_ago,
                    },
                    {"id": "4", "name": "fourth", "lastModifiedDateTime": self.day_ago},
                    {"id": "5", "name": "fifth", "lastModifiedDateTime": self.day_ago},
                    {"id": "6", "name": "sixth", "deleted": {"state": "deleted"}},
                ],
                delta_link="deltalinksample",
            )
        ]

    @property
    def drive_item_permissions(self):
        return SAMPLE_DRIVE_PERMISSIONS

    @property
    def drive_items_permissions_batch(self):
        return [
            {
                "id": drive_item_permission["id"],
                "body": {"value": [drive_item_permission]},
            }
            for drive_item_permission in self.drive_item_permissions
        ]

    @pytest_asyncio.fixture
    async def patch_sharepoint_client(self):
        client = AsyncMock()

        with patch(
            "connectors.sources.sharepoint_online.SharepointOnlineClient",
            return_value=AsyncMock(),
        ) as new_mock:
            client = new_mock.return_value
            client.site_collections = AsyncIterator(self.site_collections)
            client.sites = AsyncIterator(self.sites)
            client.site_group_users = AsyncIterator(self.site_group_users)
            client.site_role_assignments = AsyncIterator(self.site_role_assignments)
            client.site_admins = AsyncIterator(self.site_admins)
            client.group = AsyncMock(return_value=self.group)
            client.group_members = AsyncIterator(self.group_members)
            client.group_owners = AsyncIterator(self.group_owners)
            client.site_users = AsyncIterator(self.site_users)
            client.drive_items_permissions_batch = AsyncIterator(
                self.drive_items_permissions_batch
            )
            client.site_list_role_assignments = AsyncIterator(
                [self.site_list_role_assignments]
            )
            client.site_list_item_role_assignments = AsyncIterator(
                [self.site_list_item_role_assignments]
            )
            client.site_list_item_has_unique_role_assignments = AsyncMock(
                return_value=self.site_list_item_has_unique_role_assignments
            )
            client.site_page_has_unique_role_assignments = AsyncMock(
                return_value=self.site_page_has_unique_role_assignments
            )
            client.site_page_role_assignments = AsyncIterator(
                [self.site_page_role_assignments]
            )
            client.users_and_groups_for_role_assignment = AsyncMock(
                return_value=self.users_and_groups_for_role_assignments
            )
            client.site_drives = AsyncIterator(self.site_drives)
            client.drive_items = self.drive_items_func
            client.site_lists = AsyncIterator(self.site_lists)
            client.site_list_has_unique_role_assignments = AsyncMock(
                return_value=self.site_list_has_unique_role_assignments
            )
            client.site_list_items = AsyncIterator(self.site_list_items)
            client.site_list_item_attachments = AsyncIterator(
                self.site_list_item_attachments
            )
            client.site_pages = AsyncIterator(self.site_pages)

            client.graph_api_token = AsyncMock()
            client.graph_api_token.get.return_value = self.graph_api_token
            client.rest_api_token = AsyncMock()
            client.rest_api_token.get.return_value = self.rest_api_token

            client.tenant_details = AsyncMock(return_value=self.valid_tenant)
            client.drive_items_delta = AsyncIterator(self.drive_items_delta)

            yield client

    def drive_items_func(self, drive_id, url=None):
        if not url:
            return AsyncIterator(self.drive_items)
        else:
            return AsyncIterator(self.drive_items_delta)

    @pytest.mark.asyncio
    async def test_get_docs_without_access_control(self, patch_sharepoint_client):
        async with create_spo_source() as source:
            source._dls_enabled = Mock(return_value=False)

            results = []
            downloads = []
            async for doc, download_func in source.get_docs():
                results.append(doc)

                if download_func:
                    downloads.append(download_func)

            assert len(results) == 11
            assert len(
                [i for i in results if i["object_type"] == "site_collection"]
            ) == len(self.site_collections)
            assert len([i for i in results if i["object_type"] == "site"]) == len(
                self.sites
            )
            assert len([i for i in results if i["object_type"] == "site_drive"]) == len(
                self.site_drives
            )
            assert len([i for i in results if i["object_type"] == "drive_item"]) == sum(
                len(j) for j in self.drive_items
            )
            assert len([i for i in results if i["object_type"] == "site_list"]) == len(
                self.site_lists
            )
            assert (
                len([i for i in results if i["object_type"] == "list_item"])
                == len(self.site_list_items) - 1
            )  # -1 because one of them is ignored!
            assert len(
                [i for i in results if i["object_type"] == "list_item_attachment"]
            ) == len(self.site_list_item_attachments)
            assert len([i for i in results if i["object_type"] == "site_page"]) == len(
                self.site_pages
            )

            for item in results:
                assert ACCESS_CONTROL not in item

    @pytest.mark.asyncio
    @patch(
        "connectors.sources.sharepoint_online.ACCESS_CONTROL",
        ALLOW_ACCESS_CONTROL_PATCHED,
    )
    @freeze_time(iso_utc())
    async def test_get_docs_with_access_control(self, patch_sharepoint_client):
        group = "group"
        email = "email"
        user = "user"
        expected_access_control = [group, email, user]

        async with create_spo_source(use_document_level_security=True) as source:
            source._site_access_control = AsyncMock(
                return_value=(expected_access_control, [])
            )

            results = []
            async for doc, _download_func in source.get_docs():
                results.append(doc)

            site_collections = [
                i for i in results if i["object_type"] == "site_collection"
            ]
            sites = [i for i in results if i["object_type"] == "site"]
            site_drives = [i for i in results if i["object_type"] == "site_drive"]
            drive_items = [i for i in results if i["object_type"] == "drive_item"]
            site_lists = [i for i in results if i["object_type"] == "site_list"]
            list_items = [i for i in results if i["object_type"] == "list_item"]
            list_item_attachments = [
                i for i in results if i["object_type"] == "list_item_attachment"
            ]
            site_pages = [i for i in results if i["object_type"] == "site_page"]

            assert len(results) == 11

            assert len(site_collections) == len(self.site_collections)
            assert len(sites) == len(self.sites)
            assert all(
                access_control_matches(
                    site[ALLOW_ACCESS_CONTROL_PATCHED], expected_access_control
                )
                for site in sites
            )

            assert len(site_drives) == len(self.site_drives)
            assert all(
                access_control_matches(
                    site_drive[ALLOW_ACCESS_CONTROL_PATCHED],
                    expected_access_control,
                )
                for site_drive in site_drives
            )

            assert len(drive_items) == sum(len(j) for j in self.drive_items)

            expected_drive_item_access_control = [
                _prefix_user_id(USER_ONE_ID),
                _prefix_group(GROUP_ONE_ID),
                *expected_access_control,
            ]
            assert all(
                access_control_matches(
                    drive_item[ALLOW_ACCESS_CONTROL_PATCHED],
                    expected_drive_item_access_control,
                )
                for drive_item in drive_items
            )

            assert len(site_lists) == len(self.site_lists)
            assert all(
                access_control_matches(
                    site_list[ALLOW_ACCESS_CONTROL_PATCHED], expected_access_control
                )
                for site_list in site_lists
            )

            assert (
                len(list_items) == len(self.site_list_items) - 1
            )  # -1 because one of them is ignored!
            assert all(
                access_control_matches(
                    list_item[ALLOW_ACCESS_CONTROL_PATCHED], expected_access_control
                )
                for list_item in list_items
            )

            assert len(list_item_attachments) == len(self.site_list_item_attachments)
            assert all(
                access_control_matches(
                    list_item_attachment[ALLOW_ACCESS_CONTROL_PATCHED],
                    expected_access_control,
                )
                for list_item_attachment in list_item_attachments
            )

            assert len(site_pages) == len(self.site_pages)
            assert all(
                access_control_matches(
                    site_page[ALLOW_ACCESS_CONTROL_PATCHED], expected_access_control
                )
                for site_page in site_pages
            )

            assert source.sync_cursor()["cursor_timestamp"] == self.today

    @pytest.mark.asyncio
    @pytest.mark.parametrize("sync_cursor", [None, {}])
    async def test_get_docs_incrementally_with_empty_cursor(
        self, patch_sharepoint_client, sync_cursor
    ):
        async with create_spo_source() as source:
            with pytest.raises(SyncCursorEmpty):
                async for (
                    _doc,
                    _download_func,
                    _operation,
                ) in source.get_docs_incrementally(sync_cursor=sync_cursor):
                    pass

    @pytest.mark.asyncio
    @freeze_time(iso_utc())
    async def test_get_docs_incrementally(self, patch_sharepoint_client):
        async with create_spo_source() as source:
            source._site_access_control = AsyncMock(return_value=([], []))
            # mock cache lookup
            source.site_group_users = AsyncMock(return_value=self.site_group_users)

        sync_cursor = {"site_drives": {}, "cursor_timestamp": self.month_ago}
        for site_drive in self.site_drives:
            sync_cursor["site_drives"][site_drive["id"]] = (
                "http://fakesharepoint.com/deltalink"
            )

        deleted = 0
        for page in self.drive_items_delta:
            deleted += len(list(filter(lambda item: "deleted" in item, page)))

        docs = []
        downloads = []
        operations = {"index": 0, "delete": 0}

        async for doc, download_func, operation in source.get_docs_incrementally(
            sync_cursor=sync_cursor
        ):
            docs.append(doc)

            if download_func:
                downloads.append(download_func)

            operations[operation] += 1

        assert len(self.site_collections) == len(
            [doc for doc in docs if doc["object_type"] == "site_collection"]
        )
        assert len(self.site_drives) == len(
            [doc for doc in docs if doc["object_type"] == "site_drive"]
        )
        assert len([doc for doc in docs if doc["object_type"] == "drive_item"]) == sum(
            len(i) for i in self.drive_items_delta
        )
        assert len([doc for doc in docs if doc["object_type"] == "site_page"]) == len(
            self.site_pages
        )
        assert len([doc for doc in docs if doc["object_type"] == "site_list"]) == len(
            self.site_lists
        )
        # -2 because one item is too old (2 months ago), one item is Web Template Extensions and is always ignored
        assert (
            len([doc for doc in docs if doc["object_type"] == "list_item"])
            == len(self.site_list_items) - 2
        )
        assert len(
            [doc for doc in docs if doc["object_type"] == "list_item_attachment"]
        ) == len(self.site_list_item_attachments)
        assert sync_cursor["cursor_timestamp"] == self.today  # cursor was updated

        assert (operations["delete"]) == deleted

    @pytest.mark.asyncio
    async def test_site_lists(self, patch_sharepoint_client):
        async with create_spo_source(
            use_document_level_security=True, fetch_unique_list_permissions=False
        ) as source:
            site = {"id": "1", "webUrl": "some url"}
            site_access_control = ["some site specific access control"]
            site_lists = []

            async for site_list in source.site_lists(site, site_access_control):
                site_lists.append(site_list)

            assert len(site_lists) == len(self.site_lists)
            assert all(
                access_control_matches(site_list[ACCESS_CONTROL], site_access_control)
                for site_list in site_lists
            )
            patch_sharepoint_client.site_list_role_assignments.assert_not_called()

    @pytest.mark.asyncio
    async def test_site_lists_with_unique_role_assignments(
        self, patch_sharepoint_client
    ):
        async with create_spo_source(use_document_level_security=True) as source:
            site = {"id": "1", "webUrl": "some url"}
            site_access_control = ["some site specific access control"]
            site_list_access_control = [
                {
                    "Member": {
                        "odata.type": "SP.User",
                        "UserPrincipalName": USER_TWO_NAME,
                    },
                }
            ]
            expected_access_control = _prefix_user(USER_TWO_NAME)

            patch_sharepoint_client.site_list_role_assignments = AsyncIterator(
                site_list_access_control
            )
            patch_sharepoint_client.has_unique_role_assignments.return_value = True

            actual_site_lists = []

            async for site_list in source.site_lists(site, site_access_control):
                actual_site_lists.append(site_list)

            assert len(actual_site_lists) == len(self.site_lists)
            assert all(
                access_control_matches(
                    site_list[ACCESS_CONTROL], expected_access_control
                )
                for site_list in actual_site_lists
            )
            patch_sharepoint_client.site_list_role_assignments.assert_called_once()

    @pytest.mark.asyncio
    async def test_download_function_for_folder(self):
        async with create_spo_source() as source:
            drive_item = {
                "name": "folder",
                "folder": {},
            }

            download_result = source.download_function(drive_item, None)

            assert download_result is None

    @pytest.mark.parametrize(
        "drive_item_permissions, site_group_users, expected_access_control",
        [
            (
                SAMPLE_DRIVE_PERMISSIONS,
                SAMPLE_SITE_GROUP_USERS,
                [
                    "user_id:user-id-1",
                    "email:sitegroup.user1@spo.com",
                    "group:group-id-1",
                    "user:sitegroup.user1",
                ],
            ),
            (
                # grantedToIdentitiesV2 gets parsed
                [
                    {
                        "grantedToIdentitiesV2": [
                            {
                                "user": {
                                    "displayName": "Generic User",
                                    "id": "75e1766a-f83d-48f8-aac3-8422f5cea411",
                                },
                                "siteUser": {
                                    "displayName": "Generic User",
                                    "loginName": "i:0#.f|membership|generic.user@enterprisesearch.onmicrosoft.com",
                                },
                            }
                        ],
                    }
                ],
                [],
                [
                    "user:generic.user@enterprisesearch.onmicrosoft.com",
                    "user_id:75e1766a-f83d-48f8-aac3-8422f5cea411",
                ],
            ),
            (
                # grantedToIdentitiesV2 parsed alongside grantedToV2
                [
                    {
                        "grantedToIdentitiesV2": [
                            {
                                "user": {
                                    "displayName": "Generic User",
                                    "id": "75e1766a-f83d-48f8-aac3-8422f5cea411",
                                },
                                "siteUser": {
                                    "displayName": "Generic User",
                                    "loginName": "i:0#.f|membership|generic.user@enterprisesearch.onmicrosoft.com",
                                },
                            }
                        ],
                        "grantedToV2": {
                            "user": {
                                "displayName": "Enterprise Search",
                                "email": "demo@enterprisesearch.onmicrosoft.com",
                                "id": "baa37bda-0dd1-4799-ae22-f3476c2cf58d",
                            },
                            "siteUser": {
                                "displayName": "Enterprise Search",
                                "email": "demo@enterprisesearch.onmicrosoft.com",
                                "id": "6",
                                "loginName": "i:0#.f|membership|demo@enterprisesearch.onmicrosoft.com",
                            },
                        },
                    }
                ],
                [],
                [
                    "user:generic.user@enterprisesearch.onmicrosoft.com",
                    "user_id:75e1766a-f83d-48f8-aac3-8422f5cea411",
                    "email:demo@enterprisesearch.onmicrosoft.com",
                    "user_id:baa37bda-0dd1-4799-ae22-f3476c2cf58d",
                    "user:demo@enterprisesearch.onmicrosoft.com",
                ],
            ),
            (
                # site groups get expanded
                [
                    {
                        "grantedToV2": {"siteGroup": {"id": "3"}},
                    }
                ],
                SAMPLE_SITE_GROUP_USERS,
                [f"user:{SITEGROUP_USER_ONE_ID}", f"email:{SITEGROUP_USER_ONE_EMAIL}"],
            ),
            (
                # groups of groups
                [{"grantedToV2": {"siteGroup": {"id": "3"}}}],
                [
                    {
                        "LoginName": "c:0o.c|federateddirectoryclaimprovider|abc123",
                        "Title": "Nested group",
                    }
                ],
                ["group:abc123"],
            ),
        ],
    )
    @pytest.mark.asyncio
    async def test_with_drive_item_permissions(
        self, drive_item_permissions, site_group_users, expected_access_control
    ):
        async with create_spo_source(use_document_level_security=True) as source:
            drive_item = {"id": 1}

            # mock cache lookup
            source.site_group_users = AsyncMock(return_value=site_group_users)

            drive_item_with_access_control = await source._with_drive_item_permissions(
                drive_item, drive_item_permissions, "dummy_site_web_url"
            )
            drive_item_access_control = drive_item_with_access_control[ACCESS_CONTROL]

            assert set(drive_item_access_control) == set(expected_access_control)

    @pytest.mark.asyncio
    async def test_drive_items_batch_with_permissions_when_fetch_drive_item_permissions_enabled(
        self, patch_sharepoint_client
    ):
        async with create_spo_source(use_document_level_security=True) as source:
            drive_id = 1
            drive_item_ids = ["1", "2"]
            drive_items_batch = [
                {"id": drive_item_id} for drive_item_id in drive_item_ids
            ]

            permissions_responses = [
                {
                    "id": drive_item_id,
                    "body": {
                        "value": [{"grantedToV2": {"user": {"id": "some user id"}}}]
                    },
                }
                for drive_item_id in drive_item_ids
            ]

            patch_sharepoint_client.drive_items_permissions_batch = AsyncIterator(
                permissions_responses
            )

            drive_items_with_permissions = []

            async for (
                drive_item_with_permission
            ) in source._drive_items_batch_with_permissions(
                drive_id, drive_items_batch, "dummy_site_web_url"
            ):
                drive_items_with_permissions.append(drive_item_with_permission)

            assert len(drive_items_with_permissions) == len(drive_item_ids)
            assert all(
                ACCESS_CONTROL in drive_item
                for drive_item in drive_items_with_permissions
            )

    @pytest.mark.asyncio
    async def test_drive_items_batch_with_permissions_when_fetch_drive_item_permissions_disabled(
        self,
    ):
        async with create_spo_source(fetch_drive_item_permissions=False) as source:
            drive_id = 1
            drive_items_batch = [{"id": "1"}, {"id": "2"}]

            drive_items_without_permissions = []

            async for (
                drive_item_without_permissions
            ) in source._drive_items_batch_with_permissions(
                drive_id, drive_items_batch, "dummy_site_web_url"
            ):
                drive_items_without_permissions.append(drive_item_without_permissions)

            assert len(drive_items_without_permissions) == len(drive_items_batch)
            assert not any(
                ACCESS_CONTROL in drive_item
                for drive_item in drive_items_without_permissions
            )

    @pytest.mark.asyncio
    async def test_drive_items_batch_with_permissions_when_dls_disabled(
        self,
    ):
        async with create_spo_source(use_document_level_security=False) as source:
            drive_id = 1
            drive_items_batch = [{"id": "1"}, {"id": "2"}]

            drive_items_without_permissions = []

            async for (
                drive_item_without_permissions
            ) in source._drive_items_batch_with_permissions(
                drive_id, drive_items_batch, "dummy_site_web_url"
            ):
                drive_items_without_permissions.append(drive_item_without_permissions)

            assert len(drive_items_without_permissions) == len(drive_items_batch)
            assert not any(
                ACCESS_CONTROL in drive_item
                for drive_item in drive_items_without_permissions
            )

    @pytest.mark.asyncio
    async def test_drive_items_batch_with_permissions_for_delta_delete_operation(
        self, patch_sharepoint_client
    ):
        async with create_spo_source(use_document_level_security=True) as source:
            drive_id = 1
            drive_item_ids = ["1", "2"]
            drive_items_batch = [
                {"id": "1", "deleted": {"state": "deleted"}},
                {"id": "2"},
            ]

            permissions_responses = [
                {
                    "id": drive_item_id,
                    "body": {
                        "value": [{"grantedToV2": {"user": {"id": "some user id"}}}]
                    },
                }
                for drive_item_id in drive_item_ids
            ]

            patch_sharepoint_client.drive_items_permissions_batch = AsyncIterator(
                permissions_responses
            )

            drive_items_with_permissions = []

            async for (
                drive_item_with_permission
            ) in source._drive_items_batch_with_permissions(
                drive_id, drive_items_batch, "dummy_site_web_url"
            ):
                drive_items_with_permissions.append(drive_item_with_permission)

            assert len(drive_items_with_permissions) == len(drive_item_ids)
            # Item with id 1 was deleted, so not trying to fetch permissions
            assert ACCESS_CONTROL not in drive_items_with_permissions[0]
            assert ACCESS_CONTROL in drive_items_with_permissions[1]

    @pytest.mark.asyncio
    @patch(
        "connectors.sources.sharepoint_online.ACCESS_CONTROL",
        ALLOW_ACCESS_CONTROL_PATCHED,
    )
    async def test_drive_items_permissions_when_fetch_drive_item_permissions_enabled(
        self, patch_sharepoint_client
    ):
        group = _prefix_group("do-not-inherit-me")
        email = _prefix_email("should-not@be-inherited.com")
        user = _prefix_user("sorry-no-access-here")
        site_access_controls = [group, email, user]

        async with create_spo_source(use_document_level_security=True) as source:
            source._site_access_control = AsyncMock(
                return_value=(site_access_controls, [])
            )

            results = []
            async for doc, _ in source.get_docs():
                results.append(doc)

            drive_items = [i for i in results if i["object_type"] == "drive_item"]

            expected_drive_item_access_control = [
                _prefix_user_id(USER_ONE_ID),
                _prefix_group(GROUP_ONE_ID),
            ]

            drive_item_access_control_with_ac_inhertiance = [
                *expected_drive_item_access_control,
                *site_access_controls,
            ]

            assert all(
                access_control_is_equal(
                    drive_item[ALLOW_ACCESS_CONTROL_PATCHED],
                    expected_drive_item_access_control,
                )
                for drive_item in drive_items
            )

            assert all(
                not access_control_is_equal(
                    drive_item[ALLOW_ACCESS_CONTROL_PATCHED],
                    drive_item_access_control_with_ac_inhertiance,
                )
                for drive_item in drive_items
            )

    @pytest.mark.asyncio
    @patch(
        "connectors.sources.sharepoint_online.ACCESS_CONTROL",
        ALLOW_ACCESS_CONTROL_PATCHED,
    )
    async def test_site_page_permissions_when_fetch_drive_item_permissions_enabled(
        self, patch_sharepoint_client
    ):
        admin_email = _prefix_email("hello@iam-admin.com")
        admin_user = _prefix_user("admin-so-i-can-access-your-data")
        admin_site_access_controls = [admin_email, admin_user]

        async with create_spo_source(use_document_level_security=True) as source:
            source._site_access_control = AsyncMock(
                return_value=([], admin_site_access_controls)
            )

            results = []
            async for doc, _download_func in source.get_docs():
                results.append(doc)

            site_pages = [i for i in results if i["object_type"] == "site_page"]

            assert all(
                access_control_is_equal(
                    site_page[ALLOW_ACCESS_CONTROL_PATCHED],
                    admin_site_access_controls,
                )
                for site_page in site_pages
            )

    @pytest.mark.asyncio
    async def test_download_function_for_deleted_item(self):
        async with create_spo_source() as source:
            # deleted items don't have `name` property
            drive_item = {"id": "testid", "deleted": {"state": "deleted"}}

            download_result = source.download_function(drive_item, None)

            assert download_result is None

    @pytest.mark.asyncio
    async def test_download_function_for_unsupported_file(self):
        async with create_spo_source() as source:
            drive_item = {
                "id": "testid",
                "name": "filename.randomextention",
                "@microsoft.graph.downloadUrl": "http://localhost/filename",
                "lastModifiedDateTime": "2023-07-10T22:12:56Z",
                "size": 10,
            }

            download_result = source.download_function(drive_item, None)

            assert download_result is None

    @pytest.mark.asyncio
    async def test_download_function_with_filtering_rule(self):
        async with create_spo_source() as source:
            max_drive_item_age = 15
            drive_item = {
                "name": "test",
                "lastModifiedDateTime": str(
                    datetime.utcnow() - timedelta(days=max_drive_item_age + 1)
                ),
            }

            download_result = source.download_function(drive_item, max_drive_item_age)

            assert download_result is None

    def test_get_default_configuration(self):
        config = SharepointOnlineDataSource.get_default_configuration()

        assert config is not None

    @pytest.mark.asyncio
    async def test_validate_config_empty_config(self, patch_sharepoint_client):
        async with create_source(SharepointOnlineDataSource) as source:
            with pytest.raises(ConfigurableFieldValueError) as e:
                await source.validate_config()

            assert e.match("Tenant ID")
            assert e.match("Tenant name")
            assert e.match("Client ID")
            assert e.match("Secret value")

    @pytest.mark.asyncio
    async def test_validate_config(self, patch_sharepoint_client):
        async with create_spo_source() as source:
            await source.validate_config()

            # Assert that tokens are awaited
            # They raise human-readable errors if something goes wrong
            # Therefore it's important
            patch_sharepoint_client.graph_api_token.get.assert_awaited()
            patch_sharepoint_client.rest_api_token.get.assert_awaited()
            patch_sharepoint_client.site_collections.assert_not_called()

    @pytest.mark.asyncio
    async def test_validate_config_when_invalid_tenant(self, patch_sharepoint_client):
        invalid_tenant_name = "wat"

        async with create_spo_source(
            tenant_name=invalid_tenant_name,
        ) as source:
            patch_sharepoint_client.tenant_details.return_value = {
                "NameSpaceType": "Unknown"
            }

            with pytest.raises(Exception) as e:
                await source.validate_config()

            assert e.match(invalid_tenant_name)

    @pytest.mark.asyncio
    async def test_validate_config_non_existing_collection(
        self, patch_sharepoint_client
    ):
        non_existing_site = "something"
        another_non_existing_site = "something-something"

        async with create_spo_source(
            site_collections=[non_existing_site, another_non_existing_site],
        ) as source:
            with pytest.raises(Exception) as e:
                await source.validate_config()

            # Says which site does not exist
            assert e.match(non_existing_site)
            assert e.match(another_non_existing_site)

    @pytest.mark.asyncio
    async def test_validate_config_with_existing_collection_fetching_all_sites(
        self, patch_sharepoint_client
    ):
        existing_site = "site-1"

        async with create_spo_source(
            site_collections=[existing_site], enumerate_all_sites=True
        ) as source:
            await source.validate_config()

    @pytest.mark.asyncio
    async def test_validate_config_with_existing_collection_fetching_individual_sites(
        self, patch_sharepoint_client
    ):
        existing_site = "site_1"

        async with create_spo_source(
            site_collections=[existing_site], enumerate_all_sites=False
        ) as source:
            await source.validate_config()

    @pytest.mark.asyncio
    async def test_get_attachment_content(self, patch_sharepoint_client):
        attachment = {"odata.id": "1", "_original_filename": "file.ppt"}
        message = b"This is content of attachment"

        async def download_func(attachment_id, async_buffer):
            await async_buffer.write(message)

        patch_sharepoint_client.download_attachment = download_func
        async with create_spo_source() as source:
            download_result = await source.get_attachment_content(attachment, doit=True)

            assert download_result["_attachment"] == base64.b64encode(message).decode()
            assert "body" not in download_result

    @pytest.mark.asyncio
    async def test_get_attachment_content_unsupported_file_type(
        self, patch_sharepoint_client
    ):
        filename = "file.unsupported_extention"
        attachment = {
            "odata.id": "1",
            "_original_filename": filename,
        }
        message = b"This is content of attachment"

        async def download_func(attachment_id, async_buffer):
            await async_buffer.write(message)

        patch_sharepoint_client.download_attachment = download_func
        async with create_spo_source() as source:
            with patch.object(source._logger, "debug") as mock_method:
                download_result = await source.get_attachment_content(
                    attachment, doit=True
                )

            assert download_result is None
            mock_method.assert_called_once_with(
                f"Not downloading attachment {filename}: file type is not supported"
            )

    @pytest.mark.asyncio
    @patch(
        "connectors.content_extraction.ContentExtraction._check_configured",
        lambda *_: True,
    )
    async def test_get_attachment_with_text_extraction_enabled_adds_body(
        self, patch_sharepoint_client
    ):
        attachment = {"odata.id": "1", "_original_filename": "file.ppt"}
        message = "This is the text content of drive item"

        with (
            patch(
                "connectors.content_extraction.ContentExtraction.extract_text",
                return_value=message,
            ) as extraction_service_mock,
            patch(
                "connectors.content_extraction.ContentExtraction.get_extraction_config",
                return_value={"host": "http://localhost:8090"},
            ),
        ):

            async def download_func(attachment_id, async_buffer):
                await async_buffer.write(bytes(message, "utf-8"))

            patch_sharepoint_client.download_attachment = download_func
            async with create_spo_source(use_text_extraction_service=True) as source:
                download_result = await source.get_attachment_content(
                    attachment, doit=True
                )

                extraction_service_mock.assert_called_once()
                assert download_result["body"] == message
                assert "_attachment" not in download_result

    @pytest.mark.asyncio
    @patch(
        "connectors.content_extraction.ContentExtraction._check_configured",
        lambda *_: False,
    )
    async def test_get_attachment_with_text_extraction_enabled_but_not_configured_adds_empty_string(
        self, patch_sharepoint_client
    ):
        attachment = {"odata.id": "1", "_original_filename": "file.ppt"}
        message = "This is the text content of drive item"

        with (
            patch(
                "connectors.content_extraction.ContentExtraction.extract_text",
                return_value=message,
            ) as extraction_service_mock,
            patch(
                "connectors.content_extraction.ContentExtraction.get_extraction_config",
                return_value={"host": "http://localhost:8090"},
            ),
        ):

            async def download_func(attachment_id, async_buffer):
                await async_buffer.write(bytes(message, "utf-8"))

            patch_sharepoint_client.download_attachment = download_func
            async with create_spo_source(use_text_extraction_service=True) as source:
                download_result = await source.get_attachment_content(
                    attachment, doit=True
                )

                extraction_service_mock.assert_not_called()
                assert download_result["body"] == ""
                assert "_attachment" not in download_result

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "filesize, expect_download", [(15, True), (10485761, False)]
    )
    @patch(
        "connectors.content_extraction.ContentExtraction._check_configured",
        lambda *_: True,
    )
    async def test_get_drive_item_content(
        self, patch_sharepoint_client, filesize, expect_download
    ):
        drive_item = {
            "id": "1",
            "size": filesize,
            "lastModifiedDateTime": datetime.now(timezone.utc),
            "parentReference": {"driveId": "drive-1"},
            "_original_filename": "file.txt",
        }
        message = b"This is content of drive item"

        async def download_func(drive_id, drive_item_id, async_buffer):
            await async_buffer.write(message)

        patch_sharepoint_client.download_drive_item = download_func
        async with create_spo_source() as source:
            download_result = await source.get_drive_item_content(drive_item, doit=True)

            if expect_download:
                assert (
                    download_result["_attachment"] == base64.b64encode(message).decode()
                )
                assert "body" not in download_result
            else:
                assert download_result is None

    @pytest.mark.asyncio
    @pytest.mark.parametrize("filesize", [(15), (10485761)])
    @patch(
        "connectors.content_extraction.ContentExtraction._check_configured",
        lambda *_: True,
    )
    async def test_get_content_with_text_extraction_enabled_adds_body(
        self, patch_sharepoint_client, filesize
    ):
        drive_item = {
            "id": "1",
            "size": filesize,
            "lastModifiedDateTime": datetime.now(timezone.utc),
            "parentReference": {"driveId": "drive-1"},
            "_original_filename": "file.txt",
        }
        message = "This is the text content of drive item"

        with (
            patch(
                "connectors.content_extraction.ContentExtraction.extract_text",
                return_value=message,
            ) as extraction_service_mock,
            patch(
                "connectors.content_extraction.ContentExtraction.get_extraction_config",
                return_value={"host": "http://localhost:8090"},
            ),
        ):

            async def download_func(drive_id, drive_item_id, async_buffer):
                await async_buffer.write(bytes(message, "utf-8"))

            patch_sharepoint_client.download_drive_item = download_func
            async with create_spo_source(use_text_extraction_service=True) as source:
                download_result = await source.get_drive_item_content(
                    drive_item, doit=True
                )

                extraction_service_mock.assert_called_once()
                assert download_result["body"] == message
                assert "_attachment" not in download_result

    @pytest.mark.asyncio
    @pytest.mark.parametrize("filesize", [(15), (10485761)])
    @patch(
        "connectors.content_extraction.ContentExtraction._check_configured",
        lambda *_: False,
    )
    async def test_get_content_with_text_extraction_enabled_but_not_configured_adds_empty_string(
        self, patch_sharepoint_client, filesize
    ):
        drive_item = {
            "id": "1",
            "size": filesize,
            "lastModifiedDateTime": datetime.now(timezone.utc),
            "parentReference": {"driveId": "drive-1"},
            "_original_filename": "file.txt",
        }
        message = "This is the text content of drive item"

        with (
            patch(
                "connectors.content_extraction.ContentExtraction.extract_text",
                return_value=message,
            ) as extraction_service_mock,
            patch(
                "connectors.content_extraction.ContentExtraction.get_extraction_config",
                return_value={"host": "http://localhost:8090"},
            ),
        ):

            async def download_func(drive_id, drive_item_id, async_buffer):
                await async_buffer.write(bytes(message, "utf-8"))

            patch_sharepoint_client.download_drive_item = download_func
            async with create_spo_source(use_text_extraction_service=True) as source:
                download_result = await source.get_drive_item_content(
                    drive_item, doit=True
                )

                extraction_service_mock.assert_not_called()
                assert download_result["body"] == ""
                assert "_attachment" not in download_result

    @pytest.mark.asyncio
    async def test_site_access_control(self, patch_sharepoint_client):
        async with create_spo_source(use_document_level_security=True) as source:
            patch_sharepoint_client._validate_sharepoint_rest_url = Mock()

            site = {"id": 1, "webUrl": "some url"}

            access_control, _ = await source._site_access_control(site)

            two_users = 2
            two_groups = 2

            assert len(access_control) == two_groups + two_users

            assert _prefix_group(GROUP_ONE_ID) in access_control
            assert _prefix_group(GROUP_TWO_ID) in access_control

            assert _prefix_user(USER_ONE_EMAIL) in access_control
            assert _prefix_email(USER_TWO_EMAIL) in access_control

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "_dls_enabled, document, access_control, expected_decorated_document",
        [
            (
                False,
                {},
                [USER_ONE_EMAIL],
                {},
            ),
            (
                True,
                {},
                [USER_ONE_EMAIL],
                {
                    ALLOW_ACCESS_CONTROL_PATCHED: [
                        USER_ONE_EMAIL,
                        *DEFAULT_GROUPS_PATCHED,
                    ]
                },
            ),
            (True, {}, [], {ALLOW_ACCESS_CONTROL_PATCHED: DEFAULT_GROUPS_PATCHED}),
            (
                True,
                {ALLOW_ACCESS_CONTROL_PATCHED: [USER_ONE_EMAIL]},
                [USER_TWO_EMAIL],
                {
                    ALLOW_ACCESS_CONTROL_PATCHED: [
                        USER_ONE_EMAIL,
                        USER_TWO_EMAIL,
                        *DEFAULT_GROUPS_PATCHED,
                    ]
                },
            ),
            (
                True,
                {ALLOW_ACCESS_CONTROL_PATCHED: [USER_ONE_EMAIL]},
                [],
                {
                    ALLOW_ACCESS_CONTROL_PATCHED: [
                        USER_ONE_EMAIL,
                        *DEFAULT_GROUPS_PATCHED,
                    ]
                },
            ),
        ],
    )
    @patch(
        "connectors.sources.sharepoint_online.ACCESS_CONTROL",
        ALLOW_ACCESS_CONTROL_PATCHED,
    )
    async def test_decorate_with_access_control(
        self, _dls_enabled, document, access_control, expected_decorated_document
    ):
        async with create_spo_source(use_document_level_security=True) as source:
            decorated_document = source._decorate_with_access_control(
                document, access_control
            )

            assert (
                decorated_document.get(ALLOW_ACCESS_CONTROL_PATCHED, []).sort()
                == expected_decorated_document.get(
                    ALLOW_ACCESS_CONTROL_PATCHED, []
                ).sort()
            )

    @pytest.mark.parametrize(
        "dls_feature_flag, dls_config_value, expected_dls_enabled",
        [
            (
                dls_feature_flag_enabled(False),
                dls_enabled_config_value(False),
                dls_enabled(False),
            ),
            (
                dls_feature_flag_enabled(False),
                dls_enabled_config_value(True),
                dls_enabled(False),
            ),
            (
                dls_feature_flag_enabled(True),
                dls_enabled_config_value(False),
                dls_enabled(False),
            ),
            (
                dls_feature_flag_enabled(True),
                dls_enabled_config_value(True),
                dls_enabled(True),
            ),
        ],
    )
    @pytest.mark.asyncio
    async def test_dls_enabled(
        self, dls_feature_flag, dls_config_value, expected_dls_enabled
    ):
        async with create_spo_source() as source:
            source._features = Mock()
            source._features.document_level_security_enabled = Mock(
                return_value=dls_feature_flag
            )
            source.configuration.get_field(
                "use_document_level_security"
            ).value = dls_config_value

            assert source._dls_enabled() == expected_dls_enabled

    @pytest.mark.asyncio
    async def test_dls_disabled_with_features_missing(self):
        async with create_spo_source() as source:
            source._features = None

            assert not source._dls_enabled()

    @pytest.mark.asyncio
    @patch(
        "connectors.sources.sharepoint_online.TIMESTAMP_FORMAT",
        TIMESTAMP_FORMAT_PATCHED,
    )
    async def test_user_access_control_doc(self, patch_sharepoint_client):
        async with create_spo_source() as source:
            created_at = "2023-05-25T13:30:54Z"
            group_one = {"id": "group-one-id"}
            group_two = {"id": "group-two-id"}
            groups = [group_one, group_two]
            patch_sharepoint_client.groups_user_transitive_member_of = AsyncIterator(
                groups
            )

            user_id = "1"
            username = "user"
            email = "user@spo.com"
            user = {
                "id": user_id,
                "UserName": username,
                "EMail": email,
                "createdDateTime": created_at,
            }

            expected_email = f"email:{email}"
            expected_user = f"user:{username}"
            expected_user_id = f"user_id:{user_id}"
            expected_groups = [f"group:{group}" for group in groups]

            user_doc = await source._user_access_control_doc(user)
            access_control = user_doc["query"]["template"]["params"]["access_control"]

            assert user_doc["_id"] == email
            assert user_doc["created_at"] == datetime.strptime(
                user["createdDateTime"], TIMESTAMP_FORMAT_PATCHED
            )
            assert user_doc["identity"]["email"] == expected_email
            assert user_doc["identity"]["username"] == expected_user
            assert user_doc["identity"]["user_id"] == expected_user_id
            assert expected_email in access_control
            assert expected_user in access_control
            all(group in access_control for group in expected_groups)

    @pytest.mark.asyncio
    async def test_get_access_control_with_dls_disabled(self, patch_sharepoint_client):
        async with create_spo_source() as source:
            patch_sharepoint_client.site_collections = AsyncIterator(
                [{"siteCollection": {"hostname": "localhost"}}]
            )
            patch_sharepoint_client.sites = AsyncIterator([{"webUrl": "some url"}])
            patch_sharepoint_client.site_users = AsyncMock(
                return_value={"value": [{"Id": 1}]}
            )
            patch_sharepoint_client.group_for_user = AsyncMock(return_value=["group_1"])

            access_control = []

            async for doc in source.get_access_control():
                access_control.append(doc)

            assert len(access_control) == 0

    @pytest.mark.asyncio
    async def test_get_access_control_with_dls_enabled_and_fetch_all_users(
        self, patch_sharepoint_client
    ):
        async with create_spo_source(use_document_level_security=True) as source:
            group = {"@odata.type": "#microsoft.graph.group", "id": "doop"}
            member_email = "member@acme.co"
            member = {
                "userPrincipalName": "some member",
                "EMail": member_email,
                "transitiveMemberOf": group,
            }
            owner_email = "owner@acme.co"
            owner = {
                "UserName": "some owner",
                "mail": owner_email,
                "transitiveMemberOf": group,
            }

            user_doc_one = {"_id": member_email}
            user_doc_two = {"_id": owner_email}

            patch_sharepoint_client.active_users_with_groups = AsyncIterator(
                [member, owner]
            )
            source._user_access_control_doc = AsyncMock(
                side_effect=[user_doc_one, user_doc_two]
            )

            user_access_control_docs = []

            async for doc in source.get_access_control():
                user_access_control_docs.append(doc)

            assert len(user_access_control_docs) == 2

    def test_prefix_group(self):
        group = "group"

        assert _prefix_group(group) == "group:group"

    def test_prefix_user(self):
        user = "user"

        assert _prefix_user(user) == "user:user"

    def test_prefix_email(self):
        email = "email"

        assert _prefix_email(email) == "email:email"

    def test_prefix_user_id(self):
        user_id = "user id"

        assert _prefix_user_id(user_id) == "user_id:user id"

    @pytest.mark.parametrize(
        "role_assignment, expected_access_control",
        [
            (
                # Group (access control: one user's principal name and one user's login name)
                {
                    "Member": {
                        "odata.type": "SP.Group",
                        "Users": [
                            {
                                "odata.type": "SP.User",
                                "LoginName": None,
                                "UserPrincipalName": USER_ONE_EMAIL,
                            },
                            {
                                "odata.type": "SP.User",
                                "LoginName": f"i:0#.f|membership|{USER_TWO_EMAIL}",
                                "UserPrincipalName": None,
                                "Email": USER_TWO_EMAIL,
                            },
                        ],
                    },
                    "RoleDefinitionBindings": READ_BINDING,
                },
                [
                    _prefix_user(USER_ONE_EMAIL),
                    _prefix_user(USER_TWO_EMAIL),
                    _prefix_email(USER_TWO_EMAIL),
                ],
            ),
            (
                # Group of groups (access control: group Id)
                {
                    "Member": {
                        "odata.type": "SP.Group",
                        "Users": [
                            {
                                "odata.type": "SP.User",
                                "LoginName": "c:0o.c|federateddirectoryclaimprovider|abc123",
                                "Title": "Nested Group",
                            }
                        ],
                    },
                    "RoleDefinitionBindings": READ_BINDING,
                },
                [_prefix_group("abc123")],
            ),
            (
                # User (access control: only principal name)
                {
                    "Member": {
                        "odata.type": "SP.User",
                        "LoginName": None,
                        "UserPrincipalName": USER_ONE_EMAIL,
                    },
                    "RoleDefinitionBindings": READ_BINDING,
                },
                [_prefix_user(USER_ONE_EMAIL)],
            ),
            (
                # User (access control: login name and principal name)
                {
                    "Member": {
                        "odata.type": "SP.User",
                        "LoginName": f"i:0#.f|membership|{USER_TWO_EMAIL}",
                        "UserPrincipalName": USER_TWO_NAME,
                    },
                    "RoleDefinitionBindings": READ_BINDING,
                },
                [_prefix_user(USER_TWO_EMAIL), _prefix_user(USER_TWO_NAME)],
            ),
            (
                # Dynamic group (access control: login name)
                {
                    "Member": {
                        "odata.type": "SP.User",
                        "LoginName": f"c:0o.c|federateddirectoryclaimprovider|{GROUP_ONE_ID}",
                    },
                    "RoleDefinitionBindings": READ_BINDING,
                },
                [_prefix_group(GROUP_ONE_ID)],
            ),
            (
                # Unknown type (access control: nothing)
                {
                    "Member": {
                        "odata.type": "Unknown type",
                        "LoginName": None,
                        "UserPrincipalName": USER_ONE_EMAIL,
                    },
                    "RoleDefinitionBindings": READ_BINDING,
                },
                [],
            ),
            (
                # User with limited access has no ACLs
                {
                    "Member": {
                        "odata.type": "SP.User",
                        "UserPrincipalName": USER_ONE_EMAIL,
                    },
                    "RoleDefinitionBindings": LIMITED_ACCESS_BINDING,
                },
                [],
            ),
            (
                # User with web-only limited access has no ACLs
                {
                    "Member": {
                        "odata.type": "SP.User",
                        "UserPrincipalName": USER_ONE_EMAIL,
                    },
                    "RoleDefinitionBindings": WEB_ONLY_BINDING,
                },
                [],
            ),
            (
                # User with mixed access
                {
                    "Member": {
                        "odata.type": "SP.User",
                        "UserPrincipalName": USER_ONE_EMAIL,
                    },
                    "RoleDefinitionBindings": READ_BINDING + LIMITED_ACCESS_BINDING,
                },
                [_prefix_user(USER_ONE_EMAIL)],
            ),
            (
                # User with empty mask (custom binding) has no ACLs
                {
                    "Member": {
                        "odata.type": "SP.User",
                        "UserPrincipalName": USER_ONE_EMAIL,
                    },
                    "RoleDefinitionBindings": [{"BasePermissions": {"Low": "0"}}],
                },
                [],
            ),
            (
                # User with non-read mask (custom binding) has no ACLs
                {
                    "Member": {
                        "odata.type": "SP.User",
                        "UserPrincipalName": USER_ONE_EMAIL,
                    },
                    "RoleDefinitionBindings": [{"BasePermissions": {"Low": "2"}}],
                },
                [],
            ),
            (
                # User with view item mask (custom binding)
                {
                    "Member": {
                        "odata.type": "SP.User",
                        "UserPrincipalName": USER_ONE_EMAIL,
                    },
                    "RoleDefinitionBindings": [{"BasePermissions": {"Low": "1"}}],
                },
                [_prefix_user(USER_ONE_EMAIL)],
            ),
            (
                # User with view page mask (custom binding)
                {
                    "Member": {
                        "odata.type": "SP.User",
                        "UserPrincipalName": USER_ONE_EMAIL,
                    },
                    "RoleDefinitionBindings": [{"BasePermissions": {"Low": "131072"}}],
                },
                [_prefix_user(USER_ONE_EMAIL)],
            ),
            (
                # User with "None" role type (custom binding) has no ACLs
                {
                    "Member": {
                        "odata.type": "SP.User",
                        "UserPrincipalName": USER_ONE_EMAIL,
                    },
                    "RoleDefinitionBindings": [{"RoleTypeKind": 0}],
                },
                [],
            ),
            (
                # User with "Guest" role type (custom binding) has no ACLs
                {
                    "Member": {
                        "odata.type": "SP.User",
                        "UserPrincipalName": USER_ONE_EMAIL,
                    },
                    "RoleDefinitionBindings": [{"RoleTypeKind": 1}],
                },
                [],
            ),
            (
                # User with "Restricted Guest" role type (custom binding) has no ACLs
                {
                    "Member": {
                        "odata.type": "SP.User",
                        "UserPrincipalName": USER_ONE_EMAIL,
                    },
                    "RoleDefinitionBindings": [{"RoleTypeKind": 9}],
                },
                [],
            ),
            (
                # User with "Reader" role type (custom binding)
                {
                    "Member": {
                        "odata.type": "SP.User",
                        "UserPrincipalName": USER_ONE_EMAIL,
                    },
                    "RoleDefinitionBindings": [{"RoleTypeKind": 2}],
                },
                [_prefix_user(USER_ONE_EMAIL)],
            ),
            (
                # User with "System" role type (custom binding)
                {
                    "Member": {
                        "odata.type": "SP.User",
                        "UserPrincipalName": USER_ONE_EMAIL,
                    },
                    "RoleDefinitionBindings": [{"RoleTypeKind": 255}],
                },
                [_prefix_user(USER_ONE_EMAIL)],
            ),
        ],
    )
    @pytest.mark.asyncio
    async def test_get_access_control_from_role_assignment(
        self, role_assignment, expected_access_control
    ):
        async with create_spo_source() as source:
            access_control = await source._get_access_control_from_role_assignment(
                role_assignment
            )

            assert len(access_control) == len(expected_access_control)
            assert all(
                identity in access_control for identity in expected_access_control
            )

    @pytest.mark.parametrize(
        "raw_login_name, expected_login_name",
        [
            (f"i:0#.f|membership|{USER_ONE_EMAIL}", USER_ONE_EMAIL),
            (f"membership|{USER_ONE_EMAIL}", None),
            (f"c:0o.c|federateddirectoryclaimprovider|{GROUP_ONE_ID}", GROUP_ONE_ID),
            (f"c:0t.c|tenant|{GROUP_ONE_ID}", GROUP_ONE_ID),
            (USER_ONE_EMAIL, None),
            ("", None),
            (None, None),
        ],
    )
    def test_get_login_name(self, raw_login_name, expected_login_name):
        assert _get_login_name(raw_login_name) == expected_login_name
