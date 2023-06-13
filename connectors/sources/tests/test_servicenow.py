#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Tests the ServiceNow source class methods"""
from unittest import mock

import pytest
from aiohttp.client_exceptions import ServerDisconnectedError

from connectors.source import ConfigurableFieldValueError, DataSourceConfiguration
from connectors.sources.servicenow import (
    InvalidResponse,
    ServiceNowClient,
    ServiceNowDataSource,
)
from connectors.sources.tests.support import create_source
from connectors.tests.commons import AsyncIterator

SAMPLE_RESPONSE = b'{"batch_request_id":"1","serviced_requests":[{"id":"1", "body":"eyJyZXN1bHQiOlt7Im5hbWUiOiJzbl9zbV9qb3VybmFsMDAwMiIsImxhYmVsIjoiU2VjcmV0cyBNYW5hZ2VtZW50IEpvdXJuYWwifV19","status_code":200,"status_text":"OK","execution_time":19}],"unserviced_requests":[]}'


class MockResponse:
    """Mock response of aiohttp get method"""

    def __init__(self, res, headers):
        """Setup a response"""
        self._res = res
        self.headers = headers
        self.content = StreamerReader(self._res)

    async def read(self):
        """Method to read response"""
        return self._res

    async def __aenter__(self):
        """Enters an async with block"""
        return self

    async def __aexit__(self, exc_type, exc, tb):
        """Closes an async with block"""
        pass


class StreamerReader:
    """Mock Stream Reader"""

    def __init__(self, res):
        """Setup a response"""
        self._res = res
        self._size = None

    async def iter_chunked(self, size):
        """Method to iterate over content"""
        yield self._res


def test_get_configuration():
    config = DataSourceConfiguration(ServiceNowDataSource.get_default_configuration())

    assert config["services"] == ["*"]
    assert config["username"] == "admin"
    assert config["password"] == "changeme"


@pytest.mark.parametrize("field", ["url", "username", "password", "services"])
@pytest.mark.asyncio
async def test_validate_config_missing_fields_then_raise(field):
    source = create_source(ServiceNowDataSource)
    source.configuration.set_field(name=field, value="")

    with pytest.raises(ConfigurableFieldValueError):
        await source.validate_config()


@pytest.mark.asyncio
async def test_validate_configuration_with_invalid_service_then_raise():
    source = create_source(ServiceNowDataSource)
    source.servicenow_client.services = ["label_1", "label_3"]

    source.servicenow_client.get_table_length = mock.AsyncMock(return_value=2)

    with pytest.raises(
        ConfigurableFieldValueError,
        match="Services 'label_3' are not available. Available services are: 'label_1'",
    ):
        with mock.patch.object(
            ServiceNowClient,
            "get_data",
            return_value=AsyncIterator(
                [
                    [
                        {"name": "name_1", "label": "label_1"},
                        {"name": "name_2", "label": "label_2"},
                    ]
                ]
            ),
        ):
            await source.validate_config()


@pytest.mark.asyncio
async def test_close_with_client_session():
    source = create_source(ServiceNowDataSource)
    source.servicenow_client._get_session

    await source.close()
    assert hasattr(source.servicenow_client.__dict__, "_get_session") is False


@pytest.mark.asyncio
async def test_ping_for_successful_connection():
    source = create_source(ServiceNowDataSource)

    with mock.patch.object(
        ServiceNowClient,
        "get_table_length",
        return_value=mock.AsyncMock(return_value=2),
    ):
        await source.ping()


@pytest.mark.asyncio
async def test_ping_for_unsuccessful_connection_then_raise():
    source = create_source(ServiceNowDataSource)

    with mock.patch.object(
        ServiceNowClient,
        "get_table_length",
        side_effect=Exception("Something went wrong"),
    ):
        with pytest.raises(Exception):
            await source.ping()


def test_tweak_bulk_options():
    source = create_source(ServiceNowDataSource)
    source.concurrent_downloads = 10
    options = {"concurrent_downloads": 5}

    source.tweak_bulk_options(options)
    assert options["concurrent_downloads"] == 10


@pytest.mark.asyncio
async def test_get_data():
    source = create_source(ServiceNowDataSource)
    # session = source.servicenow_client._get_session

    source.servicenow_client._api_call = mock.AsyncMock(
        return_value=MockResponse(
            res=SAMPLE_RESPONSE, headers={"Content-Type": "application/json"}
        )
    )

    response_list = []
    async for response in source.servicenow_client.get_data(batched_apis={"API1"}):
        response_list.append(response)

    assert [
        {"name": "sn_sm_journal0002", "label": "Secrets Management Journal"}
    ] in response_list


@pytest.mark.asyncio
@mock.patch("connectors.utils.apply_retry_strategy")
async def test_get_data_with_retry(mock_apply_retry_strategy):
    source = create_source(ServiceNowDataSource)

    mock_apply_retry_strategy.return_value = mock.Mock()
    source.servicenow_client._api_call = mock.AsyncMock(
        side_effect=ServerDisconnectedError
    )

    with pytest.raises(Exception):
        async for response in source.servicenow_client.get_data(batched_apis={"API1"}):
            pass


@pytest.mark.asyncio
async def test_get_table_length():
    source = create_source(ServiceNowDataSource)

    source.servicenow_client._api_call = mock.AsyncMock(
        return_value=MockResponse(
            res=SAMPLE_RESPONSE,
            headers={"Content-Type": "application/json", "x-total-count": 2},
        )
    )
    response = await source.servicenow_client.get_table_length("Service1")

    assert response == 2


@pytest.mark.asyncio
@mock.patch("connectors.utils.apply_retry_strategy")
async def test_get_table_length_with_retry(mock_apply_retry_strategy):
    source = create_source(ServiceNowDataSource)

    mock_apply_retry_strategy.return_value = mock.Mock()
    source.servicenow_client._api_call = mock.AsyncMock(
        side_effect=ServerDisconnectedError
    )

    with pytest.raises(Exception):
        await source.servicenow_client.get_table_length("Service1")


@pytest.mark.asyncio
@mock.patch("connectors.utils.apply_retry_strategy")
async def test_get_data_with_empty_response(mock_apply_retry_strategy):
    source = create_source(ServiceNowDataSource)

    mock_apply_retry_strategy.return_value = mock.Mock()
    source.servicenow_client._api_call = mock.AsyncMock(
        return_value=MockResponse(
            res=b"",
            headers={"Content-Type": "application/json"},
        )
    )

    with pytest.raises(InvalidResponse):
        async for response in source.servicenow_client.get_data(batched_apis={"API1"}):
            pass


@pytest.mark.asyncio
@mock.patch("connectors.utils.apply_retry_strategy")
async def test_get_data_with_text_response(mock_apply_retry_strategy):
    source = create_source(ServiceNowDataSource)

    mock_apply_retry_strategy.return_value = mock.Mock()
    source.servicenow_client._api_call = mock.AsyncMock(
        return_value=MockResponse(
            res=b"Text",
            headers={"Content-Type": "text/html"},
        )
    )

    with pytest.raises(InvalidResponse):
        async for response in source.servicenow_client.get_data(batched_apis={"API1"}):
            pass


@pytest.mark.asyncio
async def test_filter_services_with_exception():
    source = create_source(ServiceNowDataSource)
    source.servicenow_client.services = ["label_1", "label_3"]

    source.servicenow_client.get_table_length = mock.AsyncMock(return_value=2)
    with mock.patch.object(
        ServiceNowClient, "get_data", side_effect=Exception("Something went wrong")
    ):
        with pytest.raises(Exception):
            await source.servicenow_client.filter_services()


@pytest.mark.asyncio
async def test_get_docs_with_skipping_table_data():
    source = create_source(ServiceNowDataSource)

    source.servicenow_client._api_call = mock.AsyncMock(
        return_value=MockResponse(
            res=SAMPLE_RESPONSE,
            headers={"Content-Type": "application/json", "x-total-count": 2},
        )
    )
    response_list = []
    with mock.patch(
        "connectors.sources.servicenow.DEFAULT_SERVICE_NAMES", ("incident",)
    ):
        with mock.patch.object(
            ServiceNowClient,
            "get_data",
            side_effect=[
                Exception("Something went wrong"),
            ],
        ):
            async for response in source.get_docs():
                response_list.append(response)

    assert response_list == []


@pytest.mark.asyncio
async def test_get_docs_with_skipping_attachment_data():
    source = create_source(ServiceNowDataSource)
    source.servicenow_client._api_call = mock.AsyncMock(
        return_value=MockResponse(
            res=SAMPLE_RESPONSE,
            headers={"Content-Type": "application/json", "x-total-count": 2},
        )
    )

    response_list = []
    with mock.patch(
        "connectors.sources.servicenow.DEFAULT_SERVICE_NAMES", ("incident",)
    ):
        with mock.patch.object(
            ServiceNowClient,
            "get_data",
            side_effect=[
                AsyncIterator(
                    [
                        [
                            {
                                "sys_id": "id_1",
                                "sys_updated_on": "1212-12-12 12:12:12",
                                "sys_class_name": "incident",
                                "sys_user": "admin",
                                "type": "table_record",
                            }
                        ]
                    ]
                ),
                Exception("Something went wrong"),
            ],
        ):
            async for response in source.get_docs():
                response_list.append(response)

    assert (
        {
            "_id": "id_1",
            "_timestamp": "1212-12-12T12:12:12",
            "sys_id": "id_1",
            "sys_updated_on": "1212-12-12 12:12:12",
            "sys_class_name": "incident",
            "sys_user": "admin",
            "type": "table_record",
        },
        None,
    ) in response_list


@pytest.mark.asyncio
async def test_get_docs_with_configured_services():
    source = create_source(ServiceNowDataSource)
    source.servicenow_client.services = ["custom"]
    source.servicenow_client._api_call = mock.AsyncMock(
        return_value=MockResponse(
            res=SAMPLE_RESPONSE,
            headers={"Content-Type": "application/json", "x-total-count": 2},
        )
    )

    response_list = []
    with mock.patch.object(
        ServiceNowClient, "filter_services", return_value=(["custom"], [])
    ):
        with mock.patch.object(
            ServiceNowClient,
            "get_data",
            side_effect=[
                AsyncIterator(
                    [
                        [
                            {
                                "sys_id": "id_1",
                                "sys_updated_on": "1212-12-12 12:12:12",
                                "sys_class_name": "custom",
                                "sys_user": "user1",
                                "type": "table_record",
                            },
                        ]
                    ]
                ),
                AsyncIterator(
                    [
                        [
                            {
                                "sys_id": "id_2",
                                "table_sys_id": "id_1",
                                "sys_updated_on": "1212-12-12 12:12:12",
                                "sys_class_name": "custom",
                                "sys_user": "user1",
                                "type": "attachment_metadata",
                            },
                        ]
                    ]
                ),
            ],
        ):
            async for response in source.get_docs():
                response_list.append(response[0])
    assert [
        {
            "sys_id": "id_1",
            "sys_updated_on": "1212-12-12 12:12:12",
            "sys_class_name": "custom",
            "sys_user": "user1",
            "type": "table_record",
            "_id": "id_1",
            "_timestamp": "1212-12-12T12:12:12",
        },
        {
            "sys_id": "id_2",
            "table_sys_id": "id_1",
            "sys_updated_on": "1212-12-12 12:12:12",
            "sys_class_name": "custom",
            "sys_user": "user1",
            "type": "attachment_metadata",
            "_id": "id_2",
            "_timestamp": "1212-12-12T12:12:12",
        },
    ] == response_list


@pytest.mark.asyncio
async def test_fetch_attachment_content_with_doit():
    source = create_source(ServiceNowDataSource)
    source.servicenow_client._api_call = mock.AsyncMock(
        return_value=MockResponse(res=b"Attachment Content", headers={})
    )

    response = await source.servicenow_client.fetch_attachment_content(
        metadata={
            "id": "id_1",
            "_timestamp": "1212-12-12 12:12:12",
            "file_name": "file_1.txt",
            "size_bytes": "2048",
        },
        doit=True,
    )

    assert response == {
        "_id": "id_1",
        "_timestamp": "1212-12-12 12:12:12",
        "_attachment": "QXR0YWNobWVudCBDb250ZW50",
    }


@pytest.mark.asyncio
async def test_fetch_attachment_content_without_doit():
    source = create_source(ServiceNowDataSource)
    source.servicenow_client._api_call = mock.AsyncMock(
        return_value=MockResponse(res=b"Attachment Content", headers={})
    )

    response = await source.servicenow_client.fetch_attachment_content(
        metadata={
            "id": "id_1",
            "_timestamp": "1212-12-12 12:12:12",
            "file_name": "file_1.txt",
            "size_bytes": "2048",
        }
    )

    assert response is None


@pytest.mark.asyncio
async def test_fetch_attachment_content_with_exception():
    source = create_source(ServiceNowDataSource)
    source.servicenow_client._api_call = mock.AsyncMock(
        side_effect=Exception("Something went wrong")
    )

    response = await source.servicenow_client.fetch_attachment_content(
        metadata={
            "id": "id_1",
            "_timestamp": "1212-12-12 12:12:12",
            "file_name": "file_1.txt",
            "size_bytes": "2048",
        },
        doit=True,
    )

    assert response is None


@pytest.mark.asyncio
async def test_fetch_attachment_content_with_unsupported_extension_then_skip():
    source = create_source(ServiceNowDataSource)
    source.servicenow_client._api_call = mock.AsyncMock(
        return_value=MockResponse(res=b"Attachment Content", headers={})
    )

    response = await source.servicenow_client.fetch_attachment_content(
        metadata={
            "id": "id_1",
            "_timestamp": "1212-12-12 12:12:12",
            "file_name": "file_1.png",
            "size_bytes": "2048",
        },
        doit=True,
    )

    assert response is None


@pytest.mark.asyncio
async def test_fetch_attachment_content_without_extension_then_skip():
    source = create_source(ServiceNowDataSource)
    source.servicenow_client._api_call = mock.AsyncMock(
        return_value=MockResponse(res=b"Attachment Content", headers={})
    )

    response = await source.servicenow_client.fetch_attachment_content(
        metadata={
            "id": "id_1",
            "_timestamp": "1212-12-12 12:12:12",
            "file_name": "file_1",
            "size_bytes": "2048",
        },
        doit=True,
    )

    assert response is None


@pytest.mark.asyncio
async def test_fetch_attachment_content_with_unsupported_file_size_then_skip():
    source = create_source(ServiceNowDataSource)
    source.servicenow_client._api_call = mock.AsyncMock(
        return_value=MockResponse(res=b"Attachment Content", headers={})
    )

    response = await source.servicenow_client.fetch_attachment_content(
        metadata={
            "id": "id_1",
            "_timestamp": "1212-12-12 12:12:12",
            "file_name": "file_1.txt",
            "size_bytes": "10485761",
        },
        doit=True,
    )

    assert response is None
