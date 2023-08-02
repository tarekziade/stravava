#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Tests the Salesforce source class methods"""
import re
import time
from unittest import mock

import pytest
from aiohttp.client_exceptions import ClientConnectionError

from connectors.source import ConfigurableFieldValueError, DataSourceConfiguration
from connectors.sources.salesforce import SalesforceDataSource, SalesforceSoqlBuilder
from tests.commons import AsyncIterator
from tests.sources.support import create_source

TEST_DOMAIN = "fake"
TEST_BASE_URL = f"https://{TEST_DOMAIN}.my.salesforce.com"
TEST_CLIENT_ID = "1234"
TEST_CLIENT_SECRET = "9876"
SECONDS_SINCE_EPOCH = int(time.time())


def test_get_default_configuration():
    config = DataSourceConfiguration(SalesforceDataSource.get_default_configuration())
    expected_fields = ["client_id", "client_secret", "domain"]

    assert all(field in config.to_dict() for field in expected_fields)


@pytest.mark.asyncio
@pytest.mark.parametrize("field", ["client_id", "client_secret", "domain"])
async def test_validate_config_missing_fields_then_raise(field):
    source = create_source(SalesforceDataSource)
    source.configuration.set_field(name=field, value="")

    with pytest.raises(ConfigurableFieldValueError):
        await source.validate_config()


@pytest.mark.asyncio
async def test_ping_with_successful_connection(mock_responses):
    source = create_source(SalesforceDataSource, domain=TEST_DOMAIN)
    mock_responses.head(TEST_BASE_URL, status=200)

    await source.ping()
    await source.close()


@pytest.mark.asyncio
async def test_get_token_with_successful_connection(mock_responses):
    source = create_source(
        SalesforceDataSource,
        domain=TEST_DOMAIN,
        client_id=TEST_CLIENT_ID,
        client_secret=TEST_CLIENT_SECRET,
    )
    response_payload = {
        "access_token": "foo",
        "signature": "bar",
        "instance_url": "https://fake.my.salesforce.com",
        "id": "https://login.salesforce.com/id/1234",
        "token_type": "Bearer",
        "issued_at": SECONDS_SINCE_EPOCH,
    }

    mock_responses.post(
        f"{TEST_BASE_URL}/services/oauth2/token", status=200, payload=response_payload
    )
    await source.salesforce_client.get_token()

    assert source.salesforce_client.token == "foo"
    assert source.salesforce_client.token_issued_at == SECONDS_SINCE_EPOCH

    await source.close()


@pytest.mark.asyncio
@mock.patch("connectors.utils.apply_retry_strategy")
async def test_get_token_with_bad_domain_raises_error(
    apply_retry_strategy, mock_responses
):
    source = create_source(
        SalesforceDataSource,
        domain=TEST_DOMAIN,
        client_id=TEST_CLIENT_ID,
        client_secret=TEST_CLIENT_SECRET,
    )
    apply_retry_strategy.return_value = mock.Mock()

    response_payload = {
        "access_token": "foo",
        "signature": "bar",
        "instance_url": "https://fake.my.salesforce.com",
        "id": "https://login.salesforce.com/id/1234",
        "token_type": "Bearer",
        "issued_at": SECONDS_SINCE_EPOCH,
    }

    mock_responses.post(
        f"{TEST_BASE_URL}/services/oauth2/token", status=400, payload=response_payload
    )
    with pytest.raises(ClientConnectionError):
        await source.salesforce_client.get_token()
    await source.close()


@pytest.mark.asyncio
async def test_get_accounts_when_success(mock_responses):
    expected_record = {
        "attributes": {
            "type": "Account",
            "url": "/services/data/v58.0/sobjects/Account/1234",
        },
        "Id": "1234",
    }
    source = create_source(
        SalesforceDataSource,
        domain=TEST_DOMAIN,
        client_id=TEST_CLIENT_ID,
        client_secret=TEST_CLIENT_SECRET,
    )
    response_payload = {
        "totalSize": "2",
        "done": True,
        "records": [expected_record],
    }

    source.salesforce_client.get_accounts = mock.Mock(
        return_value=AsyncIterator([{**expected_record, "_id": "1234"}])
    )
    mock_responses.get(
        re.compile(f"{TEST_BASE_URL}/services/data/v58.0/query*"),
        status=200,
        payload=response_payload,
    )
    async for account in source.salesforce_client.get_accounts():
        assert account["Id"] == "1234"
        assert account["_id"] == "1234"

    await source.close()


@pytest.mark.asyncio
@mock.patch("connectors.utils.apply_retry_strategy")
async def test_get_accounts_when_invalid_request(apply_retry_strategy, mock_responses):
    source = create_source(
        SalesforceDataSource,
        domain=TEST_DOMAIN,
        client_id=TEST_CLIENT_ID,
        client_secret=TEST_CLIENT_SECRET,
    )
    response_payload = [
        {"message": "Unable to process query.", "errorCode": "INVALID_FIELD"}
    ]

    mock_responses.get(
        re.compile(f"{TEST_BASE_URL}/services/data/v58.0/query*"),
        status=400,
        payload=response_payload,
    )
    with pytest.raises(ClientConnectionError):
        async for _ in source.salesforce_client.get_accounts():
            # TODO confirm error message when error handling is improved
            pass

    await source.close()


@pytest.mark.asyncio
async def test_build_soql_query_with_fields():
    expected_columns = [
        "Id",
        "CreatedDate",
        "LastModifiedDate",
        "FooField",
        "BarField",
    ]

    builder = SalesforceSoqlBuilder("Test")
    builder.with_id()
    builder.with_default_metafields()
    builder.with_fields(["FooField", "BarField"])
    query = builder.build()

    assert query.startswith("SELECT ")
    assert all(col in query for col in expected_columns)
    assert query.endswith("FROM Test")
