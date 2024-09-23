#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import json
import os
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from unittest.mock import ANY, AsyncMock, Mock, patch

import pytest
from elasticsearch import ApiError, ConflictError

from connectors.config import load_config
from connectors.filtering.validation import (
    FilteringValidationResult,
    FilteringValidationState,
    FilterValidationError,
    InvalidFilteringError,
)
from connectors.protocol import (
    IDLE_JOBS_THRESHOLD,
    JOB_NOT_FOUND_ERROR,
    Connector,
    ConnectorIndex,
    DataSourceError,
    Features,
    Filter,
    Filtering,
    JobStatus,
    JobTriggerMethod,
    JobType,
    Pipeline,
    ServiceTypeNotConfiguredError,
    ServiceTypeNotSupportedError,
    Sort,
    Status,
    SyncJob,
    SyncJobIndex,
)
from connectors.protocol.connectors import ProtocolError
from connectors.source import BaseDataSource
from connectors.utils import ACCESS_CONTROL_INDEX_PREFIX, iso_utc
from tests.commons import AsyncIterator

HERE = os.path.dirname(__file__)
FIXTURES_DIR = os.path.abspath(os.path.join(HERE, "..", "fixtures"))

CONFIG = os.path.join(FIXTURES_DIR, "config.yml")

DEFAULT_DOMAIN = "DEFAULT"

DRAFT_ADVANCED_SNIPPET = {"value": {"query": {"options": {}}}}

DRAFT_RULE_ONE_ID = 1
DRAFT_RULE_TWO_ID = 2

ACTIVE_ADVANCED_SNIPPET = {"value": {"find": {"settings": {}}}}

ACTIVE_RULE_ONE_ID = 3
ACTIVE_RULE_TWO_ID = 4

ACTIVE_FILTER_STATE = "active"
DRAFT_FILTER_STATE = "draft"

FILTERING_VALIDATION_VALID = {"state": "valid", "errors": []}
FILTERING_VALIDATION_INVALID = {"state": "invalid", "errors": []}
FILTERING_VALIDATION_EDITED = {"state": "edited", "errors": []}

FILTER_VALIDATION_STATE_VALID = {
    "advanced_snippet": ACTIVE_ADVANCED_SNIPPET,
    "rules": [{"id": DRAFT_RULE_ONE_ID}],
    "validation": FILTERING_VALIDATION_VALID,
}

FILTER_VALIDATION_STATE_INVALID = FILTER_VALIDATION_STATE_VALID | {
    "validation": FILTERING_VALIDATION_INVALID
}
FILTER_VALIDATION_STATE_EDITED = FILTER_VALIDATION_STATE_VALID | {
    "validation": FILTERING_VALIDATION_EDITED
}

DRAFT_FILTERING_DEFAULT_DOMAIN = {
    "advanced_snippet": DRAFT_ADVANCED_SNIPPET,
    "rules": [{"id": DRAFT_RULE_ONE_ID}, {"id": DRAFT_RULE_TWO_ID}],
    "validation": FILTERING_VALIDATION_VALID,
}

DRAFT_FILTERING_DEFAULT_DOMAIN_EDITED = DRAFT_FILTERING_DEFAULT_DOMAIN | {
    "validation": FILTERING_VALIDATION_EDITED
}

ACTIVE_FILTERING_DEFAULT_DOMAIN = {
    "advanced_snippet": ACTIVE_ADVANCED_SNIPPET,
    "rules": [{"id": ACTIVE_RULE_ONE_ID}, {"id": ACTIVE_RULE_TWO_ID}],
    "validation": FILTERING_VALIDATION_VALID,
}

FILTERING_DEFAULT_DOMAIN = {
    "domain": DEFAULT_DOMAIN,
    "draft": DRAFT_FILTERING_DEFAULT_DOMAIN,
    "active": ACTIVE_FILTERING_DEFAULT_DOMAIN,
}

FILTERING_DEFAULT_DOMAIN_EDITED = FILTERING_DEFAULT_DOMAIN | {
    "draft": DRAFT_FILTERING_DEFAULT_DOMAIN_EDITED
}

FILTERING_OTHER_DOMAIN = {
    "domain": "other",
    "draft": {},
    "active": {},
    "validation": {
        "state": "invalid",
        "errors": [{"ids": ["1"], "messages": ["some messages"]}],
    },
}

CONNECTOR_ID = "1"

DOC_SOURCE_FILTERING = [FILTERING_DEFAULT_DOMAIN, FILTERING_OTHER_DOMAIN]
DOC_SOURCE_FILTERING_EDITED = [FILTERING_DEFAULT_DOMAIN_EDITED, FILTERING_OTHER_DOMAIN]

DOC_SOURCE = {
    "_id": CONNECTOR_ID,
    "_seq_no": 1,
    "_primary_term": 2,
    "_source": {
        "configuration": {"key": "value"},
        "description": "description",
        "error": "none",
        "features": {},
        "filtering": DOC_SOURCE_FILTERING,
        "index_name": "search-index",
        "name": "MySQL",
        "pipeline": {},
        "scheduling": {},
        "service_type": "SERVICE",
        "status": "connected",
        "language": "en",
    },
}

DOC_SOURCE_WITH_EDITED_FILTERING = DOC_SOURCE | {
    "_source": {"filtering": DOC_SOURCE_FILTERING_EDITED}
}

OTHER_DOMAIN_ONE = "other-domain-1"
OTHER_DOMAIN_TWO = "other-domain-2"
NON_EXISTING_DOMAIN = "non-existing-domain"

EMPTY_FILTER = Filter()

FILTERING = [
    {
        "domain": Filtering.DEFAULT_DOMAIN,
        "draft": DRAFT_FILTERING_DEFAULT_DOMAIN,
        "active": ACTIVE_FILTERING_DEFAULT_DOMAIN,
        "validation": FILTERING_VALIDATION_VALID,
    },
    {
        "domain": OTHER_DOMAIN_ONE,
        "draft": {},
        "active": {},
        "validation": FILTERING_VALIDATION_VALID,
    },
    {
        "domain": OTHER_DOMAIN_TWO,
        "draft": {},
        "active": {},
        "validation": FILTERING_VALIDATION_VALID,
    },
]

ADVANCED_RULES = {"db": {"table": "SELECT * FROM db.table"}}

ADVANCED_RULES_NON_EMPTY = {"advanced_snippet": ADVANCED_RULES}

RULES = [
    {
        "id": 1,
    }
]
BASIC_RULES_NON_EMPTY = {"rules": RULES}
ADVANCED_AND_BASIC_RULES_NON_EMPTY = {
    "advanced_snippet": {"db": {"table": "SELECT * FROM db.table"}},
    "rules": RULES,
}

SYNC_CURSOR = {"foo": "bar"}

INDEX_NAME = "index_name"


def test_utc():
    # All dates are in ISO 8601 UTC so we can serialize them
    now = datetime.utcnow()
    then = json.loads(json.dumps({"date": iso_utc(when=now)}))["date"]
    assert now.isoformat() == then


mongo = {
    "api_key_id": "",
    "api_key_secret_id": "",
    "configuration": {
        "host": {"value": "mongodb://127.0.0.1:27021", "label": "MongoDB Host"},
        "database": {"value": "sample_airbnb", "label": "MongoDB Database"},
        "collection": {
            "value": "listingsAndReviews",
            "label": "MongoDB Collection",
        },
    },
    "index_name": "search-airbnb",
    "service_type": "mongodb",
    "status": "configured",
    "language": "en",
    "last_sync_status": None,
    "last_sync_error": "",
    "last_synced": "",
    "last_seen": "",
    "created_at": "",
    "updated_at": "",
    "scheduling": {"enabled": True, "interval": "0 * * * *"},
}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "native_service_types, connector_ids, expected_connector_count",
    [
        ([], [], 0),
        (["mongodb"], [], 1),
        ([], ["1"], 1),
        (["mongodb"], ["1"], 1),
    ],
)
async def test_supported_connectors(
    native_service_types, connector_ids, expected_connector_count, mock_responses
):
    config = {"host": "http://nowhere.com:9200", "user": "tarek", "password": "blah"}
    native_connectors_query = {
        "bool": {
            "filter": [
                {"term": {"is_native": True}},
                {"terms": {"service_type": native_service_types}},
            ]
        }
    }

    custom_connectors_query = {
        "bool": {
            "filter": [
                {"term": {"is_native": False}},
                {"terms": {"_id": connector_ids}},
            ]
        }
    }

    if len(native_service_types) > 0 and len(connector_ids) > 0:
        query = {"bool": {"should": [native_connectors_query, custom_connectors_query]}}
    elif len(native_service_types) > 0:
        query = native_connectors_query
    elif len(connector_ids) > 0:
        query = custom_connectors_query
    else:
        query = {}

    headers = {"X-Elastic-Product": "Elasticsearch"}
    mock_responses.post(
        "http://nowhere.com:9200/.elastic-connectors/_refresh", headers=headers
    )
    mock_responses.post(
        "http://nowhere.com:9200/.elastic-connectors/_search?expand_wildcards=hidden",
        body={"query": query},
        payload={
            "hits": {"hits": [{"_id": "1", "_source": mongo}], "total": {"value": 1}}
        },
        headers=headers,
    )

    connector_index = ConnectorIndex(config)
    connectors = [
        connector
        async for connector in connector_index.supported_connectors(
            native_service_types=native_service_types, connector_ids=connector_ids
        )
    ]
    await connector_index.close()

    assert len(connectors) == expected_connector_count
    if expected_connector_count > 0:
        assert connectors[0].service_type == mongo["service_type"]


@pytest.mark.asyncio
async def test_all_connectors(mock_responses):
    config = {"host": "http://nowhere.com:9200", "user": "tarek", "password": "blah"}
    headers = {"X-Elastic-Product": "Elasticsearch"}
    mock_responses.post(
        "http://nowhere.com:9200/.elastic-connectors/_refresh", headers=headers
    )

    mock_responses.post(
        "http://nowhere.com:9200/.elastic-connectors/_search?expand_wildcards=hidden",
        payload={
            "hits": {"hits": [{"_id": "1", "_source": mongo}], "total": {"value": 1}}
        },
        headers=headers,
    )

    connectors = ConnectorIndex(config)
    conns = []
    async for connector in connectors.all_connectors():
        conns.append(connector)
    await connectors.close()

    assert len(conns) == 1


@pytest.mark.asyncio
async def test_connector_properties():
    connector_src = {
        "_id": "test",
        "_source": {
            "api_key_secret_id": "api-key-secret-id",
            "service_type": "test",
            "index_name": "search-some-index",
            "configuration": {},
            "language": "en",
            "scheduling": {
                "access_control": {"enabled": True, "interval": "* * * * *"},
                "full": {"enabled": True, "interval": "* * * * *"},
                "incremental": {"enabled": True, "interval": "* * * * *"},
            },
            "status": "created",
            "last_seen": iso_utc(),
            "last_sync_status": "completed",
            "last_access_control_sync_status": "pending",
            "pipeline": {},
            "last_sync_scheduled_at": iso_utc(),
            "last_access_control_sync_scheduled_at": iso_utc(),
            "sync_cursor": SYNC_CURSOR,
        },
    }

    index = Mock()
    connector = Connector(elastic_index=index, doc_source=connector_src)

    assert connector.id == "test"
    assert connector.status == Status.CREATED
    assert connector.service_type == "test"
    assert connector.configuration.is_empty()
    assert connector.native is False
    assert connector.index_name == "search-some-index"
    assert connector.language == "en"
    assert connector.last_sync_status == JobStatus.COMPLETED
    assert connector.last_access_control_sync_status == JobStatus.PENDING
    assert connector.access_control_sync_scheduling["enabled"]
    assert connector.access_control_sync_scheduling["interval"] == "* * * * *"
    assert connector.full_sync_scheduling["enabled"]
    assert connector.full_sync_scheduling["interval"] == "* * * * *"
    assert connector.incremental_sync_scheduling["enabled"]
    assert connector.incremental_sync_scheduling["interval"] == "* * * * *"
    assert connector.sync_cursor == SYNC_CURSOR
    assert connector.api_key_secret_id == "api-key-secret-id"
    assert isinstance(connector.last_seen, datetime)
    assert isinstance(connector.filtering, Filtering)
    assert isinstance(connector.pipeline, Pipeline)
    assert isinstance(connector.features, Features)
    assert isinstance(connector.last_sync_scheduled_at, datetime)
    assert isinstance(connector.last_access_control_sync_scheduled_at, datetime)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "interval, last_seen, should_send_heartbeat",
    [
        (60, None, True),
        (
            60,
            iso_utc(),
            False,
        ),
        (60, iso_utc(datetime.now(timezone.utc) - timedelta(seconds=70)), True),
    ],
)
async def test_heartbeat(interval, last_seen, should_send_heartbeat):
    source = {
        "_id": "1",
        "_source": {
            "last_seen": last_seen,
        },
    }
    index = Mock()
    index.heartbeat = AsyncMock(return_value=1)

    connector = Connector(elastic_index=index, doc_source=source)
    await connector.heartbeat(interval=interval)

    if should_send_heartbeat:
        index.heartbeat.assert_awaited()
    else:
        index.heartbeat.assert_not_awaited()


@pytest.mark.parametrize(
    "job_type, expected_doc_source_update",
    [
        (
            JobType.FULL,
            {
                "last_sync_status": JobStatus.IN_PROGRESS.value,
                "last_sync_error": None,
                "status": Status.CONNECTED.value,
                "error": None,
            },
        ),
        (
            JobType.INCREMENTAL,
            {
                "last_sync_status": JobStatus.IN_PROGRESS.value,
                "last_sync_error": None,
                "status": Status.CONNECTED.value,
                "error": None,
            },
        ),
        (
            JobType.ACCESS_CONTROL,
            {
                "last_access_control_sync_status": JobStatus.IN_PROGRESS.value,
                "last_access_control_sync_error": None,
                "status": Status.CONNECTED.value,
                "error": None,
            },
        ),
    ],
)
@pytest.mark.asyncio
async def test_sync_starts(job_type, expected_doc_source_update):
    doc_id = "1"
    seq_no = 1
    primary_term = 2
    connector_doc = {"_id": doc_id, "_seq_no": seq_no, "_primary_term": primary_term}
    index = Mock()
    index.update = AsyncMock()
    index.feature_use_connectors_api = False

    connector = Connector(elastic_index=index, doc_source=connector_doc)
    await connector.sync_starts(job_type)
    index.update.assert_called_with(
        doc_id=connector.id,
        doc=expected_doc_source_update,
        if_seq_no=seq_no,
        if_primary_term=primary_term,
    )


@pytest.mark.parametrize(
    "job_type, last_sync_info, status, error",
    [
        (
            JobType.FULL,
            {
                "last_sync_status": JobStatus.IN_PROGRESS.value,
                "last_sync_error": None,
            },
            Status.CONNECTED.value,
            None,
        ),
        (
            JobType.INCREMENTAL,
            {"last_sync_status": JobStatus.IN_PROGRESS.value, "last_sync_error": None},
            Status.CONNECTED.value,
            None,
        ),
        (
            JobType.ACCESS_CONTROL,
            {
                "last_access_control_sync_status": JobStatus.IN_PROGRESS.value,
                "last_access_control_sync_error": None,
            },
            Status.CONNECTED.value,
            None,
        ),
    ],
)
@pytest.mark.asyncio
async def test_sync_starts_with_connector_api(job_type, last_sync_info, status, error):
    doc_id = "1"
    connector_doc = {"_id": doc_id}
    index = Mock()
    index.api.connector_update_error = AsyncMock()
    index.api.connector_update_last_sync_info = AsyncMock()
    index.feature_use_connectors_api = True

    connector = Connector(elastic_index=index, doc_source=connector_doc)
    await connector.sync_starts(job_type)
    index.api.connector_update_last_sync_info.assert_called_with(
        connector_id=connector.id, last_sync_info=last_sync_info
    )
    index.api.connector_update_error.assert_called_with(
        connector_id=connector.id, error=error
    )


@pytest.mark.asyncio
async def test_connector_error():
    connector_doc = {"_id": "1"}
    error = "something wrong"
    index = Mock()
    index.update = AsyncMock(return_value=1)
    index.feature_use_connectors_api = False
    expected_doc_source_update = {
        "status": Status.ERROR.value,
        "error": error,
    }

    connector = Connector(elastic_index=index, doc_source=connector_doc)
    await connector.error(error)
    index.update.assert_called_with(doc_id=connector.id, doc=expected_doc_source_update)


@pytest.mark.asyncio
async def test_connector_error_with_connector_api():
    connector_doc = {"_id": "1"}
    error = "something wrong"
    index = Mock()
    index.api.connector_update_error = AsyncMock()
    index.feature_use_connectors_api = True
    connector = Connector(elastic_index=index, doc_source=connector_doc)
    await connector.error(error)
    index.api.connector_update_error.assert_called_with(
        connector_id=connector.id, error=error
    )


def mock_job(
    status=JobStatus.COMPLETED,
    job_type=JobType.FULL,
    error=None,
    terminated=True,
):
    job = Mock()
    job.status = status
    job.error = error
    job.job_type = job_type
    job.terminated = terminated
    job.indexed_document_count = 0
    job.deleted_document_count = 0
    return job


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "job, expected_doc_source_update",
    [
        (
            None,
            {
                "last_access_control_sync_error": JOB_NOT_FOUND_ERROR,
                "last_access_control_sync_status": JobStatus.ERROR.value,
                "last_sync_error": JOB_NOT_FOUND_ERROR,
                "last_sync_status": JobStatus.ERROR.value,
                "last_synced": ANY,
                "status": Status.ERROR.value,
                "error": JOB_NOT_FOUND_ERROR,
            },
        ),
        (
            mock_job(
                status=JobStatus.ERROR, job_type=JobType.FULL, error="something wrong"
            ),
            {
                "last_sync_status": JobStatus.ERROR.value,
                "last_synced": ANY,
                "last_sync_error": "something wrong",
                "status": Status.ERROR.value,
                "error": "something wrong",
                "last_indexed_document_count": 0,
                "last_deleted_document_count": 0,
            },
        ),
        (
            mock_job(status=JobStatus.CANCELED, job_type=JobType.FULL),
            {
                "last_sync_status": JobStatus.CANCELED.value,
                "last_synced": ANY,
                "last_sync_error": None,
                "status": Status.CONNECTED.value,
                "error": None,
                "last_indexed_document_count": 0,
                "last_deleted_document_count": 0,
            },
        ),
        (
            mock_job(
                status=JobStatus.SUSPENDED, job_type=JobType.FULL, terminated=False
            ),
            {
                "last_sync_status": JobStatus.SUSPENDED.value,
                "last_synced": ANY,
                "last_sync_error": None,
                "status": Status.CONNECTED.value,
                "error": None,
            },
        ),
        (
            mock_job(job_type=JobType.FULL),
            {
                "last_sync_status": JobStatus.COMPLETED.value,
                "last_synced": ANY,
                "last_sync_error": None,
                "status": Status.CONNECTED.value,
                "error": None,
                "sync_cursor": SYNC_CURSOR,
                "last_indexed_document_count": 0,
                "last_deleted_document_count": 0,
            },
        ),
        (
            mock_job(job_type=JobType.FULL),
            {
                "last_sync_status": JobStatus.COMPLETED.value,
                "last_synced": ANY,
                "last_sync_error": None,
                "status": Status.CONNECTED.value,
                "error": None,
                "sync_cursor": SYNC_CURSOR,
                "last_indexed_document_count": 0,
                "last_deleted_document_count": 0,
            },
        ),
        (
            mock_job(job_type=JobType.ACCESS_CONTROL),
            {
                "last_access_control_sync_status": JobStatus.COMPLETED.value,
                "last_synced": ANY,
                "last_access_control_sync_error": None,
                "status": Status.CONNECTED.value,
                "error": None,
                "last_indexed_document_count": 0,
                "last_deleted_document_count": 0,
            },
        ),
        (
            mock_job(
                status=JobStatus.ERROR,
                job_type=JobType.ACCESS_CONTROL,
                error="something wrong",
            ),
            {
                "last_access_control_sync_status": JobStatus.ERROR.value,
                "last_synced": ANY,
                "last_access_control_sync_error": "something wrong",
                "status": Status.ERROR.value,
                "error": "something wrong",
                "last_indexed_document_count": 0,
                "last_deleted_document_count": 0,
            },
        ),
        (
            mock_job(
                status=JobStatus.SUSPENDED,
                job_type=JobType.ACCESS_CONTROL,
                terminated=False,
            ),
            {
                "last_access_control_sync_status": JobStatus.SUSPENDED.value,
                "last_synced": ANY,
                "last_access_control_sync_error": None,
                "status": Status.CONNECTED.value,
                "error": None,
            },
        ),
        (
            mock_job(status=JobStatus.CANCELED, job_type=JobType.ACCESS_CONTROL),
            {
                "last_access_control_sync_status": JobStatus.CANCELED.value,
                "last_synced": ANY,
                "last_access_control_sync_error": None,
                "status": Status.CONNECTED.value,
                "error": None,
                "last_indexed_document_count": 0,
                "last_deleted_document_count": 0,
            },
        ),
    ],
)
async def test_sync_done(job, expected_doc_source_update):
    connector_doc = {"_id": "1"}
    index = Mock()
    index.update = AsyncMock(return_value=1)
    index.feature_use_connectors_api = False

    connector = Connector(elastic_index=index, doc_source=connector_doc)
    await connector.sync_done(job=job, cursor=SYNC_CURSOR)
    index.update.assert_called_with(doc_id=connector.id, doc=expected_doc_source_update)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "job, last_sync_info, error, status",
    [
        (
            None,
            {
                "last_access_control_sync_error": JOB_NOT_FOUND_ERROR,
                "last_access_control_sync_status": JobStatus.ERROR.value,
                "last_sync_error": JOB_NOT_FOUND_ERROR,
                "last_sync_status": JobStatus.ERROR.value,
                "last_synced": ANY,
                "last_indexed_document_count": 0,
                "last_deleted_document_count": 0,
            },
            JOB_NOT_FOUND_ERROR,
            Status.ERROR.value,
        ),
        (
            mock_job(
                status=JobStatus.ERROR, job_type=JobType.FULL, error="something wrong"
            ),
            {
                "last_sync_status": JobStatus.ERROR.value,
                "last_synced": ANY,
                "last_sync_error": "something wrong",
                "last_indexed_document_count": 0,
                "last_deleted_document_count": 0,
            },
            "something wrong",
            Status.ERROR.value,
        ),
        (
            mock_job(status=JobStatus.CANCELED, job_type=JobType.FULL),
            {
                "last_sync_status": JobStatus.CANCELED.value,
                "last_synced": ANY,
                "last_sync_error": None,
                "last_indexed_document_count": 0,
                "last_deleted_document_count": 0,
            },
            None,
            Status.CONNECTED.value,
        ),
        (
            mock_job(
                status=JobStatus.SUSPENDED, job_type=JobType.FULL, terminated=False
            ),
            {
                "last_sync_status": JobStatus.SUSPENDED.value,
                "last_synced": ANY,
                "last_sync_error": None,
            },
            None,
            Status.CONNECTED.value,
        ),
        (
            mock_job(job_type=JobType.FULL),
            {
                "last_sync_status": JobStatus.COMPLETED.value,
                "last_synced": ANY,
                "last_sync_error": None,
                "sync_cursor": SYNC_CURSOR,
                "last_indexed_document_count": 0,
                "last_deleted_document_count": 0,
            },
            None,
            Status.CONNECTED.value,
        ),
        (
            mock_job(job_type=JobType.FULL),
            {
                "last_sync_status": JobStatus.COMPLETED.value,
                "last_synced": ANY,
                "last_sync_error": None,
                "sync_cursor": SYNC_CURSOR,
                "last_indexed_document_count": 0,
                "last_deleted_document_count": 0,
            },
            None,
            Status.CONNECTED.value,
        ),
        (
            mock_job(job_type=JobType.ACCESS_CONTROL),
            {
                "last_access_control_sync_status": JobStatus.COMPLETED.value,
                "last_synced": ANY,
                "last_access_control_sync_error": None,
                "last_indexed_document_count": 0,
                "last_deleted_document_count": 0,
            },
            None,
            Status.CONNECTED.value,
        ),
        (
            mock_job(
                status=JobStatus.ERROR,
                job_type=JobType.ACCESS_CONTROL,
                error="something wrong",
            ),
            {
                "last_access_control_sync_status": JobStatus.ERROR.value,
                "last_synced": ANY,
                "last_access_control_sync_error": "something wrong",
                "last_indexed_document_count": 0,
                "last_deleted_document_count": 0,
            },
            "something wrong",
            Status.ERROR.value,
        ),
        (
            mock_job(
                status=JobStatus.SUSPENDED,
                job_type=JobType.ACCESS_CONTROL,
                terminated=False,
            ),
            {
                "last_access_control_sync_status": JobStatus.SUSPENDED.value,
                "last_synced": ANY,
                "last_access_control_sync_error": None,
            },
            None,
            Status.CONNECTED.value,
        ),
        (
            mock_job(status=JobStatus.CANCELED, job_type=JobType.ACCESS_CONTROL),
            {
                "last_access_control_sync_status": JobStatus.CANCELED.value,
                "last_synced": ANY,
                "last_access_control_sync_error": None,
                "last_indexed_document_count": 0,
                "last_deleted_document_count": 0,
            },
            None,
            Status.CONNECTED.value,
        ),
    ],
)
async def test_sync_done_with_connector_api(job, last_sync_info, error, status):
    connector_doc = {"_id": "1"}
    index = Mock()
    index.feature_use_connectors_api = True
    index.api.connector_update_error = AsyncMock()
    index.api.connector_update_last_sync_info = AsyncMock()
    connector = Connector(elastic_index=index, doc_source=connector_doc)
    await connector.sync_done(job=job, cursor=SYNC_CURSOR)
    index.api.connector_update_last_sync_info(
        connector_id=connector.id, doc=last_sync_info
    )
    index.api.connector_update_error(connector_id=connector.id, error=error)


mock_next_run = iso_utc()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "scheduling_enabled, expected_next_sync, job_type",
    [
        (False, None, JobType.ACCESS_CONTROL),
        (True, mock_next_run, JobType.ACCESS_CONTROL),
        (False, None, JobType.FULL),
        (True, mock_next_run, JobType.FULL),
        (False, None, JobType.INCREMENTAL),
        (True, mock_next_run, JobType.INCREMENTAL),
    ],
)
@patch("connectors.protocol.connectors.next_run")
async def test_connector_next_sync(
    next_run, scheduling_enabled, expected_next_sync, job_type
):
    connector_doc = {
        "_id": "1",
        "_source": {
            "scheduling": {
                "access_control": {
                    "enabled": scheduling_enabled,
                    "interval": "1 * * * * *",
                },
                "full": {
                    "enabled": scheduling_enabled,
                    "interval": "1 * * * * *",
                },
                "incremental": {
                    "enabled": scheduling_enabled,
                    "interval": "1 * * * * *",
                },
            },
        },
    }
    index = Mock()
    next_run.return_value = mock_next_run
    connector = Connector(elastic_index=index, doc_source=connector_doc)

    assert connector.next_sync(job_type, datetime.utcnow()) == expected_next_sync


@pytest.mark.asyncio
async def test_sync_job_properties():
    sync_job_src = {
        "_id": "test",
        "_source": {
            "status": "error",
            "job_type": "access_control",
            "error": "something wrong",
            "indexed_document_count": 10,
            "indexed_document_volume": 20,
            "deleted_document_count": 30,
            "total_document_count": 100,
            "connector": {
                "id": "connector_id",
                "service_type": "test",
                "index_name": "search-some-index",
                "configuration": {},
                "language": "en",
                "filtering": {},
                "pipeline": {},
                "sync_cursor": SYNC_CURSOR,
            },
        },
    }

    index = Mock()
    sync_job = SyncJob(elastic_index=index, doc_source=sync_job_src)

    assert sync_job.id == "test"
    assert sync_job.status == JobStatus.ERROR
    assert sync_job.error == "something wrong"
    assert sync_job.connector_id == "connector_id"
    assert sync_job.service_type == "test"
    assert sync_job.configuration.is_empty()
    assert sync_job.index_name == "search-some-index"
    assert sync_job.language == "en"
    assert sync_job.sync_cursor == SYNC_CURSOR
    assert sync_job.indexed_document_count == 10
    assert sync_job.indexed_document_volume == 20
    assert sync_job.deleted_document_count == 30
    assert sync_job.total_document_count == 100

    assert isinstance(sync_job.filtering, Filter)
    assert isinstance(sync_job.pipeline, Pipeline)

    assert sync_job.job_type == JobType.ACCESS_CONTROL
    assert isinstance(sync_job.job_type, JobType)


@pytest.mark.parametrize(
    "job_type, is_content_sync",
    [
        (JobType.FULL, True),
        (JobType.INCREMENTAL, True),
        (JobType.ACCESS_CONTROL, False),
    ],
)
def test_is_content_sync(job_type, is_content_sync):
    source = {"_id": "1", "_source": {"job_type": job_type.value}}
    sync_job = SyncJob(elastic_index=None, doc_source=source)
    assert sync_job.is_content_sync() == is_content_sync


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "validation_result_state, validation_result_errors, should_raise_exception",
    [
        (
            FilteringValidationState.VALID,
            [],
            False,
        ),
        (
            FilteringValidationState.INVALID,
            ["something wrong"],
            True,
        ),
    ],
)
async def test_sync_job_validate_filtering(
    validation_result_state, validation_result_errors, should_raise_exception
):
    source = {"_id": "1"}
    index = Mock()
    validator = Mock()
    validation_result = FilteringValidationResult(
        state=validation_result_state, errors=validation_result_errors
    )
    validator.validate_filtering = AsyncMock(return_value=validation_result)

    sync_job = SyncJob(elastic_index=index, doc_source=source)

    try:
        await sync_job.validate_filtering(validator=validator)
    except InvalidFilteringError:
        assert should_raise_exception


@pytest.mark.asyncio
async def test_sync_job_claim():
    source = {"_id": "1"}
    index = Mock()
    index.update = AsyncMock(return_value=1)
    expected_doc_source_update = {
        "status": JobStatus.IN_PROGRESS.value,
        "started_at": ANY,
        "last_seen": ANY,
        "worker_hostname": ANY,
        "connector.sync_cursor": SYNC_CURSOR,
    }

    sync_job = SyncJob(elastic_index=index, doc_source=source)
    sync_job.index.feature_use_connectors_api = False
    await sync_job.claim(sync_cursor=SYNC_CURSOR)

    index.update.assert_called_with(doc_id=sync_job.id, doc=expected_doc_source_update)


@pytest.mark.asyncio
async def test_sync_job_claim_with_connector_api(set_env):
    source = {"_id": "1"}
    index = Mock()
    index.api.connector_sync_job_claim = AsyncMock(return_value={"result": "updated"})

    sync_job = SyncJob(elastic_index=index, doc_source=source)
    sync_job.index.feature_use_connectors_api = True
    await sync_job.claim(sync_cursor=SYNC_CURSOR)

    index.api.connector_sync_job_claim.assert_called_with(
        sync_job_id=sync_job.id, worker_hostname=ANY, sync_cursor=SYNC_CURSOR
    )


@pytest.mark.asyncio
async def test_sync_job_claim_fails():
    source = {"_id": "1"}
    index = Mock()
    api_meta = Mock()
    api_meta.status = 413
    error_body = {"error": {"reason": "mocked test failure"}}
    index.update = AsyncMock(
        side_effect=ApiError(
            message="this is an error message", body=error_body, meta=api_meta
        )
    )

    sync_job = SyncJob(elastic_index=index, doc_source=source)
    with pytest.raises(ProtocolError) as e:
        await sync_job.claim(sync_cursor=SYNC_CURSOR)
        assert (
            "because Elasticsearch responded with status 413. Reason: mocked test failure"
            in str(e)
        )


@pytest.mark.asyncio
async def test_sync_job_update_metadata():
    source = {"_id": "1"}
    index = Mock()
    index.feature_use_connectors_api = False
    index.update = AsyncMock(return_value=1)
    ingestion_stats = {
        "indexed_document_count": 1,
        "indexed_document_volume": 13,
        "deleted_document_count": 0,
    }
    connector_metadata = {"foo": "bar"}
    expected_doc_source_update = (
        {
            "last_seen": ANY,
        }
        | ingestion_stats
        | {"metadata": connector_metadata}
    )

    sync_job = SyncJob(elastic_index=index, doc_source=source)
    await sync_job.update_metadata(
        ingestion_stats=ingestion_stats | {"blah": 0},
        connector_metadata=connector_metadata,
    )

    index.update.assert_called_with(doc_id=sync_job.id, doc=expected_doc_source_update)


@pytest.mark.asyncio
async def test_sync_job_update_metadata_with_connector_api():
    source = {"_id": "1"}
    index = Mock()
    index.feature_use_connectors_api = True
    index.api.connector_sync_job_update_stats = AsyncMock()
    ingestion_stats = {
        "indexed_document_count": 1,
        "indexed_document_volume": 13,
        "deleted_document_count": 0,
    }
    connector_metadata = {"foo": "bar"}

    sync_job = SyncJob(elastic_index=index, doc_source=source)

    await sync_job.update_metadata(
        ingestion_stats=ingestion_stats | {"blah": 0},
        connector_metadata=connector_metadata,
    )

    index.api.connector_sync_job_update_stats.assert_called_with(
        sync_job_id=sync_job.id,
        ingestion_stats=ingestion_stats,
        metadata=connector_metadata,
    )


@pytest.mark.asyncio
async def test_sync_job_done():
    source = {"_id": "1"}
    index = Mock()
    index.update = AsyncMock(return_value=1)
    expected_doc_source_update = {
        "last_seen": ANY,
        "completed_at": ANY,
        "status": JobStatus.COMPLETED.value,
        "error": None,
    }

    sync_job = SyncJob(elastic_index=index, doc_source=source)
    await sync_job.done()

    index.update.assert_called_with(doc_id=sync_job.id, doc=expected_doc_source_update)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "error, expected_message",
    [
        ("something wrong", "something wrong"),
        (Exception(), "Exception"),
        (Exception("something wrong"), "Exception: something wrong"),
        (123, "123"),
    ],
)
async def test_sync_job_fail(error, expected_message):
    source = {"_id": "1"}
    index = Mock()
    index.update = AsyncMock(return_value=1)
    expected_doc_source_update = {
        "last_seen": ANY,
        "completed_at": ANY,
        "status": JobStatus.ERROR.value,
        "error": expected_message,
    }

    sync_job = SyncJob(elastic_index=index, doc_source=source)
    await sync_job.fail(error)

    index.update.assert_called_with(doc_id=sync_job.id, doc=expected_doc_source_update)


@pytest.mark.asyncio
async def test_sync_job_cancel():
    source = {"_id": "1"}
    index = Mock()
    index.update = AsyncMock(return_value=1)
    expected_doc_source_update = {
        "last_seen": ANY,
        "completed_at": ANY,
        "status": JobStatus.CANCELED.value,
        "error": None,
        "canceled_at": ANY,
    }

    sync_job = SyncJob(elastic_index=index, doc_source=source)
    await sync_job.cancel()

    index.update.assert_called_with(doc_id=sync_job.id, doc=expected_doc_source_update)


@pytest.mark.asyncio
async def test_sync_job_suspend():
    source = {"_id": "1"}
    index = Mock()
    index.update = AsyncMock(return_value=1)
    expected_doc_source_update = {
        "last_seen": ANY,
        "status": JobStatus.SUSPENDED.value,
        "error": None,
    }

    sync_job = SyncJob(elastic_index=index, doc_source=source)
    await sync_job.suspend()

    index.update.assert_called_with(doc_id=sync_job.id, doc=expected_doc_source_update)


class Banana(BaseDataSource):
    """Banana"""

    @classmethod
    def get_default_configuration(cls):
        return {"one": {"value": None}, "two": {"value": None}}


@pytest.mark.asyncio
async def test_connector_prepare_different_id_invalid_source():
    doc_id = "1"
    seq_no = 1
    primary_term = 2
    connector_doc = {
        "_id": doc_id,
        "_seq_no": seq_no,
        "_primary_term": primary_term,
        "_source": {},
    }
    config = {
        "connector_id": "2",
        "service_type": "banana",
    }
    sources = {}
    index = Mock()
    index.fetch_response_by_id = AsyncMock(return_value=connector_doc)
    index.update = AsyncMock()
    connector = Connector(elastic_index=index, doc_source=connector_doc)
    await connector.prepare(config, sources)
    index.update.assert_not_awaited()


@pytest.mark.parametrize(
    "main_doc_id, this_doc_id",
    [
        ("1", "1"),
        ("1", "2"),
    ],
)
@pytest.mark.asyncio
async def test_connector_prepare_with_prepared_connector(main_doc_id, this_doc_id):
    seq_no = 1
    primary_term = 2
    connector_doc = {
        "_id": this_doc_id,
        "_seq_no": seq_no,
        "_primary_term": primary_term,
        "_source": {
            "service_type": "banana",
            "configuration": {
                "one": {
                    "default_value": None,
                    "depends_on": [],
                    "display": "text",
                    "label": "",
                    "options": [],
                    "order": 1,
                    "required": True,
                    "sensitive": False,
                    "tooltip": None,
                    "type": "str",
                    "ui_restrictions": [],
                    "validations": [],
                    "value": "foobar",
                },
                "two": {
                    "default_value": None,
                    "depends_on": [],
                    "display": "text",
                    "label": "",
                    "options": [],
                    "order": 1,
                    "required": True,
                    "sensitive": False,
                    "tooltip": None,
                    "type": "str",
                    "ui_restrictions": [],
                    "validations": [],
                    "value": "foobar",
                },
            },
            "features": Banana.features(),
        },
    }
    config = {
        "connector_id": main_doc_id,
        "service_type": "banana",
    }
    sources = {"banana": "tests.protocol.test_connectors:Banana"}
    index = Mock()
    index.fetch_response_by_id = AsyncMock(return_value=connector_doc)
    index.update = AsyncMock()
    connector = Connector(elastic_index=index, doc_source=connector_doc)
    await connector.prepare(config, sources)
    index.update.assert_not_awaited()


@pytest.mark.parametrize(
    "main_doc_id, this_doc_id",
    [
        ("1", "1"),
        ("1", "2"),
    ],
)
@pytest.mark.asyncio
async def test_connector_prepare_with_connector_empty_config_creates_default(
    main_doc_id, this_doc_id
):
    seq_no = 1
    primary_term = 2
    connector_doc = {
        "_id": this_doc_id,
        "_seq_no": seq_no,
        "_primary_term": primary_term,
        "_source": {
            "service_type": "banana",
            "configuration": {},
            "features": Banana.features(),
        },
    }
    config = {
        "connector_id": main_doc_id,
        "service_type": "banana",
    }
    sources = {"banana": "tests.protocol.test_connectors:Banana"}
    index = Mock()
    index.fetch_response_by_id = AsyncMock(return_value=connector_doc)
    index.update = AsyncMock()
    connector = Connector(elastic_index=index, doc_source=connector_doc)

    expected = Banana.get_simple_configuration()

    # only updates fields with missing properties
    await connector.prepare(config, sources)
    index.update.assert_called_once_with(
        doc_id=this_doc_id,
        doc={"configuration": expected, "status": "needs_configuration"},
        if_seq_no=seq_no,
        if_primary_term=primary_term,
    )


@pytest.mark.parametrize(
    "main_doc_id, this_doc_id",
    [
        ("1", "1"),
        ("1", "2"),
    ],
)
@pytest.mark.asyncio
async def test_connector_prepare_with_connector_missing_fields_creates_them(
    main_doc_id, this_doc_id
):
    seq_no = 1
    primary_term = 2
    connector_doc = {
        "_id": this_doc_id,
        "_seq_no": seq_no,
        "_primary_term": primary_term,
        "_source": {
            "service_type": "banana",
            "configuration": {
                "two": {
                    "default_value": None,
                    "depends_on": [],
                    "display": "text",
                    "label": "",
                    "options": [],
                    "order": 1,
                    "required": True,
                    "sensitive": False,
                    "tooltip": None,
                    "type": "str",
                    "ui_restrictions": [],
                    "validations": [],
                    "value": "foobar",
                },
            },
            "features": Banana.features(),
        },
    }
    config = {
        "connector_id": main_doc_id,
        "service_type": "banana",
    }
    sources = {"banana": "tests.protocol.test_connectors:Banana"}
    index = Mock()
    index.fetch_response_by_id = AsyncMock(return_value=connector_doc)
    index.update = AsyncMock()
    connector = Connector(elastic_index=index, doc_source=connector_doc)

    expected = Banana.get_simple_configuration()
    del expected["two"]

    # only updates fields with missing properties
    await connector.prepare(config, sources)
    index.update.assert_called_once_with(
        doc_id=this_doc_id,
        doc={"configuration": expected},
        if_seq_no=seq_no,
        if_primary_term=primary_term,
    )


@pytest.mark.parametrize(
    "main_doc_id, this_doc_id",
    [
        ("1", "1"),
        ("1", "2"),
    ],
)
@pytest.mark.asyncio
async def test_connector_prepare_with_connector_missing_field_properties_creates_them(
    main_doc_id, this_doc_id
):
    seq_no = 1
    primary_term = 2
    connector_doc = {
        "_id": this_doc_id,
        "_seq_no": seq_no,
        "_primary_term": primary_term,
        "_source": {
            "service_type": "banana",
            "configuration": {
                "one": {"value": "hoho", "extra_property": "hehe"},
                "two": {
                    "default_value": None,
                    "depends_on": [],
                    "display": "text",
                    "label": "",
                    "options": [],
                    "order": 1,
                    "required": True,
                    "sensitive": False,
                    "tooltip": None,
                    "type": "str",
                    "ui_restrictions": [],
                    "validations": [],
                    "value": "foobar",
                },
            },
            "features": Banana.features(),
        },
    }
    config = {
        "connector_id": main_doc_id,
        "service_type": "banana",
    }
    sources = {"banana": "tests.protocol.test_connectors:Banana"}
    index = Mock()
    index.fetch_response_by_id = AsyncMock(return_value=connector_doc)
    index.update = AsyncMock()
    connector = Connector(elastic_index=index, doc_source=connector_doc)

    expected = Banana.get_simple_configuration()

    # only updates fields with missing properties
    expected["one"]["value"] = "hoho"
    expected["one"]["extra_property"] = "hehe"
    del expected["two"]

    await connector.prepare(config, sources)
    index.update.assert_called_once_with(
        doc_id=this_doc_id,
        doc={"configuration": expected},
        if_seq_no=seq_no,
        if_primary_term=primary_term,
    )


@pytest.mark.asyncio
async def test_connector_prepare_with_service_type_not_configured():
    doc_id = "1"
    seq_no = 1
    primary_term = 2
    connector_doc = {
        "_id": doc_id,
        "_seq_no": seq_no,
        "_primary_term": primary_term,
        "_source": {},
    }
    config = {
        "connector_id": doc_id,
    }
    sources = {"banana": "tests.protocol.test_connectors:Banana"}
    index = Mock()
    index.fetch_response_by_id = AsyncMock(return_value=connector_doc)
    index.update = AsyncMock()
    connector = Connector(elastic_index=index, doc_source=connector_doc)
    with pytest.raises(ServiceTypeNotConfiguredError):
        await connector.prepare(config, sources)
        index.update.assert_not_awaited()


@pytest.mark.asyncio
async def test_connector_prepare_with_service_type_not_supported():
    doc_id = "1"
    seq_no = 1
    primary_term = 2
    connector_doc = {
        "_id": doc_id,
        "_seq_no": seq_no,
        "_primary_term": primary_term,
        "_source": {},
    }
    config = {
        "connector_id": doc_id,
        "service_type": "banana",
    }
    sources = {}
    index = Mock()
    index.fetch_response_by_id = AsyncMock(return_value=connector_doc)
    index.update = AsyncMock()
    connector = Connector(elastic_index=index, doc_source=connector_doc)
    with pytest.raises(ServiceTypeNotSupportedError):
        await connector.prepare(config, sources)
        index.update.assert_not_awaited()


@pytest.mark.asyncio
async def test_connector_prepare_with_data_source_error():
    doc_id = "1"
    seq_no = 1
    primary_term = 2
    connector_doc = {
        "_id": doc_id,
        "_seq_no": seq_no,
        "_primary_term": primary_term,
        "_source": {},
    }
    config = {
        "connector_id": doc_id,
        "service_type": "banana",
    }
    sources = {"banana": "foobar"}
    index = Mock()
    index.fetch_response_by_id = AsyncMock(return_value=connector_doc)
    index.update = AsyncMock()
    connector = Connector(elastic_index=index, doc_source=connector_doc)
    with pytest.raises(DataSourceError):
        await connector.prepare(config, sources)
        index.update.assert_not_awaited()


@pytest.mark.asyncio
async def test_connector_prepare_with_different_features():
    doc_id = "1"
    seq_no = 1
    primary_term = 2
    connector_doc = {
        "_id": doc_id,
        "_seq_no": seq_no,
        "_primary_term": primary_term,
        "_source": {
            "service_type": "banana",
            "configuration": Banana.get_simple_configuration(),
            "features": {"foo": "bar"},
        },
    }
    config = {
        "connector_id": doc_id,
        "service_type": "banana",
    }
    sources = {"banana": "tests.protocol.test_connectors:Banana"}
    index = Mock()
    index.fetch_response_by_id = AsyncMock(return_value=connector_doc)
    index.update = AsyncMock()
    connector = Connector(elastic_index=index, doc_source=connector_doc)
    await connector.prepare(config, sources)
    index.update.assert_called_once_with(
        doc_id=doc_id,
        doc={"features": Banana.features()},
        if_seq_no=seq_no,
        if_primary_term=primary_term,
    )


@pytest.mark.asyncio
async def test_connector_prepare():
    doc_id = "1"
    seq_no = 1
    primary_term = 2
    connector_doc = {
        "_id": doc_id,
        "_seq_no": seq_no,
        "_primary_term": primary_term,
        "_source": {"configuration": {}},
    }
    config = {
        "connector_id": doc_id,
        "service_type": "banana",
    }
    sources = {"banana": "tests.protocol.test_connectors:Banana"}
    index = Mock()
    index.fetch_response_by_id = AsyncMock(return_value=connector_doc)
    index.update = AsyncMock()
    connector = Connector(elastic_index=index, doc_source=connector_doc)
    await connector.prepare(config, sources)
    index.update.assert_called_once_with(
        doc_id=doc_id,
        doc={
            "service_type": "banana",
            "configuration": Banana.get_simple_configuration(),
            "status": Status.NEEDS_CONFIGURATION.value,
            "features": Banana.features(),
        },
        if_seq_no=seq_no,
        if_primary_term=primary_term,
    )


@pytest.mark.asyncio
async def test_connector_prepare_with_race_condition():
    doc_id = "1"
    seq_no = 1
    primary_term = 2
    connector_doc = {
        "_id": doc_id,
        "_seq_no": seq_no,
        "_primary_term": primary_term,
        "_source": {"configuration": {}},
    }
    config = {
        "connector_id": doc_id,
        "service_type": "banana",
    }
    sources = {"banana": "tests.protocol.test_connectors:Banana"}
    connector_doc_after_update = connector_doc | {
        "_source": {
            "service_type": "banana",
            "configuration": Banana.get_simple_configuration(),
            "features": Banana.features(),
        }
    }
    index = Mock()
    index.fetch_response_by_id = AsyncMock(
        side_effect=[connector_doc, connector_doc_after_update]
    )
    index.update = AsyncMock(
        side_effect=ConflictError(
            message="This is an error message from test_connector_prepare_with_race_condition",
            meta=None,
            body={},
        )
    )
    connector = Connector(elastic_index=index, doc_source=connector_doc)
    await connector.prepare(config, sources)
    index.update.assert_called_once_with(
        doc_id=doc_id,
        doc={
            "service_type": "banana",
            "configuration": Banana.get_simple_configuration(),
            "status": Status.NEEDS_CONFIGURATION.value,
            "features": Banana.features(),
        },
        if_seq_no=seq_no,
        if_primary_term=primary_term,
    )


@pytest.mark.parametrize(
    "job_type, date_field_to_update",
    [
        (JobType.FULL, "last_sync_scheduled_at"),
        (JobType.INCREMENTAL, "last_incremental_sync_scheduled_at"),
        (JobType.ACCESS_CONTROL, "last_access_control_sync_scheduled_at"),
    ],
)
@pytest.mark.asyncio
async def test_connector_update_last_sync_scheduled_at_by_job_type(
    job_type, date_field_to_update
):
    doc_id = "2"
    seq_no = 2
    primary_term = 1
    new_ts = datetime.utcnow() + timedelta(seconds=30)
    connector_doc = {
        "_id": doc_id,
        "_seq_no": seq_no,
        "_primary_term": primary_term,
        "_source": {},
    }
    index = Mock()
    index.update = AsyncMock()
    index.feature_use_connectors_api = False
    connector = Connector(elastic_index=index, doc_source=connector_doc)
    await connector.update_last_sync_scheduled_at_by_job_type(job_type, new_ts)

    index.update.assert_awaited_once_with(
        doc_id=doc_id,
        doc={date_field_to_update: new_ts.isoformat()},
        if_seq_no=seq_no,
        if_primary_term=primary_term,
    )


@pytest.mark.parametrize(
    "job_type, date_field_to_update",
    [
        (JobType.FULL, "last_sync_scheduled_at"),
        (JobType.INCREMENTAL, "last_incremental_sync_scheduled_at"),
        (JobType.ACCESS_CONTROL, "last_access_control_sync_scheduled_at"),
    ],
)
@pytest.mark.asyncio
async def test_connector_update_last_sync_scheduled_at_by_job_type_with_connector_api(
    job_type, date_field_to_update
):
    doc_id = "2"
    new_ts = datetime.now(timezone.utc) + timedelta(seconds=30)
    connector_doc = {
        "_id": doc_id,
        "_source": {},
    }
    index = Mock()
    index.update = AsyncMock()
    index.api.connector_update_last_sync_info = AsyncMock()
    index.feature_use_connectors_api = True
    connector = Connector(elastic_index=index, doc_source=connector_doc)
    await connector.update_last_sync_scheduled_at_by_job_type(job_type, new_ts)

    index.api.connector_update_last_sync_info.assert_awaited_once_with(
        connector_id=doc_id, last_sync_info={date_field_to_update: new_ts.isoformat()}
    )


@pytest.mark.asyncio
async def test_connector_validate_filtering_not_edited():
    index = Mock()
    index.update = AsyncMock()
    index.feature_use_connectors_api = False
    index.fetch_response_by_id = AsyncMock(return_value=DOC_SOURCE)
    validator = Mock()
    validator.validate_filtering = AsyncMock()

    connector = Connector(elastic_index=index, doc_source=DOC_SOURCE)
    await connector.validate_filtering(validator=validator)

    validator.validate_filtering.assert_not_awaited()
    index.update.assert_not_awaited()


@pytest.mark.asyncio
async def test_connector_validate_filtering_invalid():
    doc_source = deepcopy(DOC_SOURCE_WITH_EDITED_FILTERING)
    index = Mock()
    index.update = AsyncMock()
    index.feature_use_connectors_api = False
    index.fetch_response_by_id = AsyncMock(return_value=doc_source)
    validation_result = FilteringValidationResult(
        state=FilteringValidationState.INVALID,
        errors=[FilterValidationError(ids=[1], messages="something wrong")],
    )
    validator = Mock()
    validator.validate_filtering = AsyncMock(return_value=validation_result)
    expected_validation_update_doc = {
        "filtering": [
            {
                "domain": DEFAULT_DOMAIN,
                "draft": DRAFT_FILTERING_DEFAULT_DOMAIN
                | {"validation": validation_result.to_dict()},
                "active": ACTIVE_FILTERING_DEFAULT_DOMAIN,
            },
            FILTERING_OTHER_DOMAIN,
        ]
    }

    connector = Connector(elastic_index=index, doc_source=doc_source)
    await connector.validate_filtering(validator=validator)

    validator.validate_filtering.assert_awaited()
    index.update.assert_awaited_once_with(
        doc_id=CONNECTOR_ID,
        doc=expected_validation_update_doc,
        if_seq_no=doc_source["_seq_no"],
        if_primary_term=doc_source["_primary_term"],
    )


@pytest.mark.asyncio
async def test_connector_validate_filtering_valid():
    doc_source = deepcopy(DOC_SOURCE_WITH_EDITED_FILTERING)
    index = Mock()
    index.update = AsyncMock()
    index.feature_use_connectors_api = False
    index.fetch_response_by_id = AsyncMock(return_value=doc_source)
    validation_result = FilteringValidationResult()
    validator = Mock()
    validator.validate_filtering = AsyncMock(return_value=validation_result)
    expected_validation_update_doc = {
        "filtering": [
            {
                "domain": DEFAULT_DOMAIN,
                "draft": DRAFT_FILTERING_DEFAULT_DOMAIN
                | {"validation": validation_result.to_dict()},
                "active": DRAFT_FILTERING_DEFAULT_DOMAIN
                | {"validation": validation_result.to_dict()},
            },
            FILTERING_OTHER_DOMAIN,
        ]
    }

    connector = Connector(elastic_index=index, doc_source=doc_source)
    await connector.validate_filtering(validator=validator)

    validator.validate_filtering.assert_awaited()
    index.update.assert_awaited_once_with(
        doc_id=CONNECTOR_ID,
        doc=expected_validation_update_doc,
        if_seq_no=doc_source["_seq_no"],
        if_primary_term=doc_source["_primary_term"],
    )


@pytest.mark.asyncio
async def test_connector_validate_filtering_with_race_condition():
    doc_source = deepcopy(DOC_SOURCE_WITH_EDITED_FILTERING)
    index = Mock()
    index.update = AsyncMock()
    index.feature_use_connectors_api = False
    validation_result = FilteringValidationResult()
    validator = Mock()
    validator.validate_filtering = AsyncMock(return_value=validation_result)
    expected_validation_update_doc = {
        "filtering": [
            {
                "domain": DEFAULT_DOMAIN,
                "draft": DRAFT_FILTERING_DEFAULT_DOMAIN
                | {"validation": validation_result.to_dict()},
                "active": DRAFT_FILTERING_DEFAULT_DOMAIN
                | {"validation": validation_result.to_dict()},
            },
            FILTERING_OTHER_DOMAIN,
        ]
    }
    # make reload method return `edited` filtering in the first call, and `valid` filtering in the second call
    index.fetch_response_by_id = AsyncMock(
        side_effect=[doc_source, deepcopy(DOC_SOURCE)]
    )

    connector = Connector(elastic_index=index, doc_source=doc_source)
    await connector.validate_filtering(validator=validator)

    validator.validate_filtering.assert_awaited()
    index.update.assert_awaited_once_with(
        doc_id=CONNECTOR_ID,
        doc=expected_validation_update_doc,
        if_seq_no=doc_source["_seq_no"],
        if_primary_term=doc_source["_primary_term"],
    )


@pytest.mark.asyncio
async def test_connector_validate_filtering_invalid_with_connector_api(set_env):
    doc_source = deepcopy(DOC_SOURCE_WITH_EDITED_FILTERING)
    index = Mock()
    index.api.connector_update_filtering_draft_validation = AsyncMock()
    index.api.connector_activate_filtering_draft = AsyncMock()
    index.feature_use_connectors_api = True
    index.fetch_response_by_id = AsyncMock(return_value=doc_source)
    validation_result = FilteringValidationResult(
        state=FilteringValidationState.INVALID,
        errors=[FilterValidationError(ids=[1], messages="something wrong")],
    )
    validator = Mock()
    validator.validate_filtering = AsyncMock(return_value=validation_result)

    connector = Connector(elastic_index=index, doc_source=doc_source)
    await connector.validate_filtering(validator=validator)

    validator.validate_filtering.assert_awaited()
    index.api.connector_update_filtering_draft_validation.assert_awaited_once_with(
        connector_id=CONNECTOR_ID,
        validation_result=validation_result.to_dict(),
    )
    index.api.connector_activate_filtering_draft.assert_not_awaited()


@pytest.mark.asyncio
async def test_connector_validate_filtering_valid_with_connector_api(set_env):
    doc_source = deepcopy(DOC_SOURCE_WITH_EDITED_FILTERING)
    index = Mock()
    index.api.connector_update_filtering_draft_validation = AsyncMock()
    index.api.connector_activate_filtering_draft = AsyncMock()
    index.feature_use_connectors_api = True
    index.fetch_response_by_id = AsyncMock(return_value=doc_source)
    validation_result = FilteringValidationResult(
        state=FilteringValidationState.VALID,
        errors=[],
    )
    validator = Mock()
    validator.validate_filtering = AsyncMock(return_value=validation_result)

    connector = Connector(elastic_index=index, doc_source=doc_source)
    await connector.validate_filtering(validator=validator)

    validator.validate_filtering.assert_awaited()
    index.api.connector_update_filtering_draft_validation.assert_awaited_once_with(
        connector_id=CONNECTOR_ID,
        validation_result=validation_result.to_dict(),
    )

    index.api.connector_activate_filtering_draft.assert_awaited_once_with(
        connector_id=CONNECTOR_ID,
    )


@pytest.mark.asyncio
async def test_document_count():
    expected_count = 20
    index = Mock()
    index.serverless = False
    index.client.indices.refresh = AsyncMock()
    index.client.count = AsyncMock(return_value={"count": expected_count})

    connector = Connector(elastic_index=index, doc_source=DOC_SOURCE)
    count = await connector.document_count()
    index.client.indices.refresh.assert_awaited_once()
    index.client.count.assert_awaited_once()
    assert count == expected_count


@pytest.mark.parametrize(
    "filtering_json, filter_state, domain, expected_filter",
    [
        (
            FILTERING,
            ACTIVE_FILTER_STATE,
            Filtering.DEFAULT_DOMAIN,
            ACTIVE_FILTERING_DEFAULT_DOMAIN,
        ),
        (
            FILTERING,
            DRAFT_FILTER_STATE,
            Filtering.DEFAULT_DOMAIN,
            DRAFT_FILTERING_DEFAULT_DOMAIN,
        ),
        (FILTERING, ACTIVE_FILTER_STATE, OTHER_DOMAIN_ONE, EMPTY_FILTER),
        (FILTERING, ACTIVE_FILTER_STATE, OTHER_DOMAIN_TWO, EMPTY_FILTER),
        # domains which do not exist should return an empty filter per default
        (FILTERING, ACTIVE_FILTER_STATE, NON_EXISTING_DOMAIN, EMPTY_FILTER),
        # if filtering is not present always return an empty filter
        ([], ACTIVE_FILTER_STATE, Filtering.DEFAULT_DOMAIN, EMPTY_FILTER),
        ([], ACTIVE_FILTER_STATE, NON_EXISTING_DOMAIN, EMPTY_FILTER),
        (None, ACTIVE_FILTER_STATE, Filtering.DEFAULT_DOMAIN, EMPTY_FILTER),
        (None, ACTIVE_FILTER_STATE, NON_EXISTING_DOMAIN, EMPTY_FILTER),
    ],
)
def test_get_filter(filtering_json, filter_state, domain, expected_filter):
    filtering = Filtering(filtering_json)

    assert filtering.get_filter(filter_state, domain) == expected_filter


@pytest.mark.parametrize(
    "domain, expected_filter",
    [
        (DEFAULT_DOMAIN, ACTIVE_FILTERING_DEFAULT_DOMAIN),
        (None, ACTIVE_FILTERING_DEFAULT_DOMAIN),
    ],
)
def test_get_active_filter(domain, expected_filter):
    filtering = Filtering(FILTERING)

    if domain is not None:
        assert filtering.get_active_filter(domain) == expected_filter
    else:
        assert filtering.get_active_filter() == expected_filter


@pytest.mark.parametrize(
    "domain, expected_filter",
    [
        (DEFAULT_DOMAIN, DRAFT_FILTERING_DEFAULT_DOMAIN),
        (None, DRAFT_FILTERING_DEFAULT_DOMAIN),
    ],
)
def test_get_draft_filter(domain, expected_filter):
    filtering = Filtering(FILTERING)

    if domain is not None:
        assert filtering.get_draft_filter(domain) == expected_filter
    else:
        assert filtering.get_draft_filter() == expected_filter


@pytest.mark.parametrize(
    "filtering, expected_transformed_filtering",
    [
        (
            {"advanced_snippet": {"value": {"query": {}}}, "rules": []},
            {"advanced_snippet": {"value": {"query": {}}}, "rules": []},
        ),
        (
            {"advanced_snippet": {"value": {}}, "rules": []},
            {"advanced_snippet": {"value": {}}, "rules": []},
        ),
        ({"advanced_snippet": {}, "rules": []}, {"advanced_snippet": {}, "rules": []}),
        ({}, {"advanced_snippet": {}, "rules": []}),
        (None, {"advanced_snippet": {}, "rules": []}),
    ],
)
def test_transform_filtering(filtering, expected_transformed_filtering):
    assert (
        Filter(filter_=filtering).transform_filtering()
        == expected_transformed_filtering
    )


@pytest.mark.parametrize(
    "features_json, feature_enabled",
    [
        (
            {
                "sync_rules": {
                    "basic": {"enabled": True},
                    "advanced": {"enabled": True},
                }
            },
            {
                Features.BASIC_RULES_NEW: True,
                Features.ADVANCED_RULES_NEW: True,
                Features.BASIC_RULES_OLD: False,
                Features.ADVANCED_RULES_OLD: False,
            },
        ),
        (
            {
                "sync_rules": {
                    "basic": {"enabled": True},
                    "advanced": {"enabled": False},
                }
            },
            {
                Features.BASIC_RULES_NEW: True,
                Features.ADVANCED_RULES_NEW: False,
                Features.BASIC_RULES_OLD: False,
                Features.ADVANCED_RULES_OLD: False,
            },
        ),
        (
            {
                "sync_rules": {
                    "basic": {"enabled": False},
                    "advanced": {"enabled": False},
                }
            },
            {
                Features.BASIC_RULES_NEW: False,
                Features.ADVANCED_RULES_NEW: False,
                Features.BASIC_RULES_OLD: False,
                Features.ADVANCED_RULES_OLD: False,
            },
        ),
        (
            {"filtering_advanced_config": True, "filtering_rules": True},
            {
                Features.BASIC_RULES_NEW: False,
                Features.ADVANCED_RULES_NEW: False,
                Features.BASIC_RULES_OLD: True,
                Features.ADVANCED_RULES_OLD: True,
            },
        ),
        (
            {"filtering_advanced_config": False, "filtering_rules": False},
            {
                Features.BASIC_RULES_NEW: False,
                Features.ADVANCED_RULES_NEW: False,
                Features.BASIC_RULES_OLD: False,
                Features.ADVANCED_RULES_OLD: False,
            },
        ),
        (
            {"filtering_advanced_config": True, "filtering_rules": False},
            {
                Features.BASIC_RULES_NEW: False,
                Features.ADVANCED_RULES_NEW: False,
                Features.BASIC_RULES_OLD: False,
                Features.ADVANCED_RULES_OLD: True,
            },
        ),
        (
            {
                "sync_rules": {
                    "basic": {"enabled": True},
                    "advanced": {"enabled": True},
                },
                "filtering_advanced_config": True,
                "filtering_rules": True,
            },
            {
                Features.BASIC_RULES_NEW: True,
                Features.ADVANCED_RULES_NEW: True,
                Features.BASIC_RULES_OLD: True,
                Features.ADVANCED_RULES_OLD: True,
            },
        ),
        (
            None,
            {
                Features.BASIC_RULES_NEW: False,
                Features.ADVANCED_RULES_NEW: False,
                Features.BASIC_RULES_OLD: False,
                Features.ADVANCED_RULES_OLD: False,
                Features.DOCUMENT_LEVEL_SECURITY: False,
            },
        ),
        (
            {},
            {
                Features.BASIC_RULES_NEW: False,
                Features.ADVANCED_RULES_NEW: False,
                Features.BASIC_RULES_OLD: False,
                Features.ADVANCED_RULES_OLD: False,
                Features.DOCUMENT_LEVEL_SECURITY: False,
            },
        ),
    ],
)
def test_feature_enabled(features_json, feature_enabled):
    features = Features(features_json)

    assert all(
        (
            features.feature_enabled(feature)
            if enabled
            else not features.feature_enabled(feature)
        )
        for feature, enabled in feature_enabled.items()
    )


@pytest.mark.parametrize(
    "features_json, sync_rules_enabled",
    [
        (
            {
                "sync_rules": {
                    "basic": {"enabled": True},
                    "advanced": {"enabled": False},
                },
                "filtering_advanced_config": False,
                "filtering_rules": False,
            },
            True,
        ),
        (
            {
                "sync_rules": {
                    "basic": {"enabled": False},
                    "advanced": {"enabled": True},
                },
                "filtering_advanced_config": False,
                "filtering_rules": False,
            },
            True,
        ),
        (
            {
                "sync_rules": {
                    "basic": {"enabled": False},
                    "advanced": {"enabled": False},
                },
                "filtering_advanced_config": True,
                "filtering_rules": False,
            },
            True,
        ),
        (
            {
                "sync_rules": {
                    "basic": {"enabled": False},
                    "advanced": {"enabled": False},
                },
                "filtering_advanced_config": False,
                "filtering_rules": True,
            },
            True,
        ),
        (
            {
                "sync_rules": {
                    "basic": {"enabled": False},
                    "advanced": {"enabled": False},
                },
                "filtering_advanced_config": False,
                "filtering_rules": False,
            },
            False,
        ),
        ({"other_feature": True}, False),
        (None, False),
        ({}, False),
    ],
)
def test_sync_rules_enabled(features_json, sync_rules_enabled):
    features = Features(features_json)

    assert features.sync_rules_enabled() == sync_rules_enabled


@pytest.mark.parametrize(
    "features_json, incremental_sync_enabled",
    [
        (
            {
                "incremental_sync": {
                    "enabled": True,
                },
            },
            True,
        ),
        (
            {
                "incremental_sync": {
                    "enabled": False,
                },
            },
            False,
        ),
        (
            {"incremental_sync": {}},
            False,
        ),
        ({"other_feature": True}, False),
        (None, False),
        ({}, False),
    ],
)
def test_incremental_sync_enabled(features_json, incremental_sync_enabled):
    features = Features(features_json)

    assert features.incremental_sync_enabled() == incremental_sync_enabled


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "trigger_method",
    [
        JobTriggerMethod.ON_DEMAND,
        JobTriggerMethod.SCHEDULED,
    ],
)
@patch("connectors.protocol.SyncJobIndex.index")
async def test_create_job(index_method, trigger_method, set_env):
    connector = Mock()
    connector.id = "id"
    connector.index_name = "index_name"
    connector.language = "en"
    config = load_config(CONFIG)
    connector.filtering.get_active_filter.transform_filtering.return_value = Filter()
    connector.pipeline = Pipeline({})

    expected_index_doc = {
        "connector": ANY,
        "trigger_method": trigger_method.value,
        "job_type": JobType.INCREMENTAL.value,
        "status": JobStatus.PENDING.value,
        "indexed_document_count": 0,
        "indexed_document_volume": 0,
        "deleted_document_count": 0,
        "created_at": ANY,
        "last_seen": ANY,
    }

    sync_job_index = SyncJobIndex(elastic_config=config["elasticsearch"])
    sync_job_index.feature_use_connectors_api = False
    await sync_job_index.create(
        connector=connector, trigger_method=trigger_method, job_type=JobType.INCREMENTAL
    )

    index_method.assert_called_with(expected_index_doc)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "trigger_method, job_type",
    [
        (JobTriggerMethod.ON_DEMAND, JobType.FULL),
        (JobTriggerMethod.ON_DEMAND, JobType.INCREMENTAL),
        (JobTriggerMethod.ON_DEMAND, JobType.ACCESS_CONTROL),
        (JobTriggerMethod.SCHEDULED, JobType.FULL),
        (JobTriggerMethod.SCHEDULED, JobType.INCREMENTAL),
        (JobTriggerMethod.SCHEDULED, JobType.ACCESS_CONTROL),
    ],
)
async def test_create_job_with_connector_api(trigger_method, job_type, set_env):
    connector = Mock()
    connector.id = "id"
    config = load_config(CONFIG)
    sync_job_index = SyncJobIndex(elastic_config=config["elasticsearch"])
    sync_job_index.feature_use_connectors_api = True
    sync_job_index.api.connector_sync_job_create = AsyncMock()
    await sync_job_index.create(
        connector=connector, trigger_method=trigger_method, job_type=job_type
    )

    sync_job_index.api.connector_sync_job_create.assert_awaited_once_with(
        connector_id=connector.id,
        trigger_method=trigger_method.value,
        job_type=job_type.value,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "job_type, target_index_name",
    [
        (JobType.FULL, INDEX_NAME),
        (JobType.INCREMENTAL, INDEX_NAME),
        (JobType.ACCESS_CONTROL, f"{ACCESS_CONTROL_INDEX_PREFIX}{INDEX_NAME}"),
    ],
)
@patch("connectors.protocol.SyncJobIndex.index")
@patch(
    "connectors.utils.ACCESS_CONTROL_INDEX_PREFIX",
    ACCESS_CONTROL_INDEX_PREFIX,
)
async def test_create_jobs_with_correct_target_index(
    index_method, job_type, target_index_name, set_env
):
    connector = Mock()
    connector.index_name = INDEX_NAME
    config = load_config(CONFIG)

    expected_index_doc = {
        "connector": {
            "id": ANY,
            "filtering": ANY,
            "index_name": target_index_name,
            "language": ANY,
            "pipeline": ANY,
            "service_type": ANY,
            "configuration": ANY,
        },
        "trigger_method": JobTriggerMethod.SCHEDULED.value,
        "job_type": job_type.value,
        "status": JobStatus.PENDING.value,
        "indexed_document_count": 0,
        "indexed_document_volume": 0,
        "deleted_document_count": 0,
        "created_at": ANY,
        "last_seen": ANY,
    }

    sync_job_index = SyncJobIndex(elastic_config=config["elasticsearch"])
    sync_job_index.feature_use_connectors_api = False
    await sync_job_index.create(
        connector=connector,
        trigger_method=JobTriggerMethod.SCHEDULED,
        job_type=job_type,
    )

    index_method.assert_called_with(expected_index_doc)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "job_types, job_type_query, remote_call",
    [
        (None, None, False),
        ("", None, False),
        (JobType.ACCESS_CONTROL.value, [JobType.ACCESS_CONTROL.value], True),
        (
            [JobType.FULL.value, JobType.INCREMENTAL.value],
            [JobType.FULL.value, JobType.INCREMENTAL.value],
            True,
        ),
    ],
)
@patch("connectors.protocol.SyncJobIndex.get_all_docs")
async def test_pending_jobs(
    get_all_docs, job_types, job_type_query, remote_call, set_env
):
    job = Mock()
    get_all_docs.return_value = AsyncIterator([job])
    config = load_config(CONFIG)
    connector_ids = [1, 2]
    expected_query = {
        "bool": {
            "must": [
                {
                    "terms": {
                        "status": [
                            JobStatus.PENDING.value,
                            JobStatus.SUSPENDED.value,
                        ]
                    }
                },
                {"terms": {"connector.id": connector_ids}},
                {"terms": {"job_type": job_type_query}},
            ]
        }
    }
    expected_sort = [{"created_at": Sort.ASC.value}]

    sync_job_index = SyncJobIndex(elastic_config=config["elasticsearch"])
    jobs = [
        job
        async for job in sync_job_index.pending_jobs(
            connector_ids=connector_ids, job_types=job_types
        )
    ]

    if remote_call:
        get_all_docs.assert_called_with(query=expected_query, sort=expected_sort)
        assert len(jobs) == 1
        assert jobs[0] == job
    else:
        get_all_docs.assert_not_called()
        assert len(jobs) == 0


@pytest.mark.asyncio
@patch("connectors.protocol.SyncJobIndex.get_all_docs")
async def test_orphaned_idle_jobs(get_all_docs, set_env):
    job = Mock()
    get_all_docs.return_value = AsyncIterator([job])
    config = load_config(CONFIG)
    connector_ids = [1, 2]
    expected_query = {
        "bool": {
            "must_not": {"terms": {"connector.id": connector_ids}},
            "filter": [
                {
                    "terms": {
                        "status": [
                            JobStatus.IN_PROGRESS.value,
                            JobStatus.CANCELING.value,
                        ]
                    }
                }
            ],
        }
    }

    sync_job_index = SyncJobIndex(elastic_config=config["elasticsearch"])
    jobs = [
        job
        async for job in sync_job_index.orphaned_idle_jobs(connector_ids=connector_ids)
    ]

    get_all_docs.assert_called_with(query=expected_query)
    assert len(jobs) == 1
    assert jobs[0] == job


@pytest.mark.asyncio
@patch("connectors.protocol.SyncJobIndex.get_all_docs")
async def test_idle_jobs(get_all_docs, set_env):
    job = Mock()
    get_all_docs.return_value = AsyncIterator([job])
    config = load_config(CONFIG)
    connector_ids = [1, 2]
    expected_query = {
        "bool": {
            "filter": [
                {"terms": {"connector.id": connector_ids}},
                {
                    "terms": {
                        "status": [
                            JobStatus.IN_PROGRESS.value,
                            JobStatus.CANCELING.value,
                        ]
                    }
                },
                {"range": {"last_seen": {"lte": f"now-{IDLE_JOBS_THRESHOLD}s"}}},
            ]
        }
    }

    sync_job_index = SyncJobIndex(elastic_config=config["elasticsearch"])
    jobs = [job async for job in sync_job_index.idle_jobs(connector_ids=connector_ids)]

    get_all_docs.assert_called_with(query=expected_query)
    assert len(jobs) == 1
    assert jobs[0] == job


@pytest.mark.parametrize(
    "filtering, should_advanced_rules_be_present",
    [
        ({"advanced_snippet": {"value": ADVANCED_RULES_NON_EMPTY}}, True),
        ({"advanced_snippet": {"value": ADVANCED_AND_BASIC_RULES_NON_EMPTY}}, True),
        ({"advanced_snippet": {"value": {}}}, False),
        ({"advanced_snippet": {"value": None}}, False),
        ({"advanced_snippet": {"value": {}}, "rules": BASIC_RULES_NON_EMPTY}, False),
        (None, False),
    ],
)
def test_advanced_rules_present(filtering, should_advanced_rules_be_present):
    assert Filter(filtering).has_advanced_rules() == should_advanced_rules_be_present


@pytest.mark.parametrize(
    "filtering, validation_state, has_expected_validation_state",
    [
        (FILTER_VALIDATION_STATE_VALID, FilteringValidationState.EDITED, False),
        (FILTER_VALIDATION_STATE_INVALID, FilteringValidationState.EDITED, False),
        (FILTER_VALIDATION_STATE_EDITED, FilteringValidationState.EDITED, True),
        ({}, FilteringValidationState.VALID, True),
    ],
)
def test_has_validation_state(
    filtering, validation_state, has_expected_validation_state
):
    assert (
        Filter(filtering).has_validation_state(validation_state)
        == has_expected_validation_state
    )


@pytest.mark.parametrize(
    "key, value, default_value",
    [
        ("name", "foobar", "ent-search-generic-ingestion"),
        ("extract_binary_content", False, True),
        ("reduce_whitespace", False, True),
        ("run_ml_inference", False, True),
    ],
)
def test_pipeline_properties(key, value, default_value):
    assert Pipeline({})[key] == default_value
    assert Pipeline({key: value})[key] == value


@pytest.mark.parametrize(
    "filtering, expected_advanced_rules",
    [
        ({"advanced_snippet": {"value": {"query": {}}}, "rules": []}, {"query": {}}),
        ({"advanced_snippet": {"value": {}}, "rules": []}, {}),
        ({"advanced_snippet": {}, "rules": []}, {}),
        ({}, {}),
        (None, {}),
    ],
)
def test_get_advanced_rules(filtering, expected_advanced_rules):
    assert Filter(filtering).get_advanced_rules() == expected_advanced_rules


def test_updated_configuration_fields():
    current = {
        "tenant_id": {"label": "Tenant ID", "order": 1, "type": "str", "value": "foo"},
        "tenant_name": {
            "label": "Tenant name",
            "order": 2,
            "type": "str",
            "value": "bar",
        },
        "client_id": {"label": "Client ID", "order": 3, "type": "str", "value": "baz"},
        "secret_value": {
            "label": "Secret value",
            "order": 4,
            "sensitive": True,
            "type": "str",
            "value": "qux",
        },
        "some_toggle": {"label": "toggle", "order": 5, "type": "bool", "value": False},
    }
    simple_default = {
        "tenant_id": {
            "label": "Tenant Identifier",
            "order": 1,
            "type": "str",
        },
        "tenant_name": {
            "label": "Tenant name",
            "order": 2,
            "type": "str",
        },
        "new_config": {
            "label": "Config label",
            "order": 3,
            "type": "bool",
            "value": True,
        },
        "client_id": {
            "label": "Client ID",
            "order": 4,
            "type": "str",
        },
        "secret_value": {
            "label": "Secret value",
            "order": 5,
            "sensitive": True,
            "type": "str",
        },
        "some_toggle": {"label": "toggle", "order": 6, "type": "bool", "value": True},
    }
    missing_configs = ["new_config"]
    connector = Connector(elastic_index=Mock(), doc_source={"_id": "test"})
    result = connector.updated_configuration_fields(
        missing_configs, current, simple_default
    )

    # all keys included where there are changes (excludes 'tenant_name')
    assert result.keys() == {
        "new_config",
        "tenant_id",
        "client_id",
        "secret_value",
        "some_toggle",
    }

    # order is adjusted
    assert result["client_id"]["order"] == 4
    assert result["secret_value"]["order"] == 5

    # so is label
    assert result["tenant_id"]["label"] == "Tenant Identifier"

    # value is not changed for existing configs
    assert "value" not in result["some_toggle"].keys()

    # value is set for new configs
    assert result["new_config"]["value"] is True


@pytest.mark.asyncio
async def test_native_connector_missing_features():
    doc_id = "1"
    seq_no = 1
    primary_term = 2
    connector_doc = {
        "_id": doc_id,
        "_seq_no": seq_no,
        "_primary_term": primary_term,
        "_source": {
            "configuration": {},
            "features": {
                "foo": "bar"  # This is the key bit. These "native features" don't align with Banana.features()
            },
            "service_type": "banana",
            "is_native": True,
        },
    }
    config = {"native_service_types": "banana"}
    sources = {"banana": "tests.protocol.test_connectors:Banana"}
    index = Mock()
    index.fetch_response_by_id = AsyncMock(side_effect=[connector_doc, connector_doc])
    index.update = AsyncMock()
    connector = Connector(elastic_index=index, doc_source=connector_doc)
    await connector.prepare(config, sources)
    index.update.assert_called_once_with(
        doc_id=doc_id,
        doc={
            "configuration": Banana.get_simple_configuration(),
            "status": Status.NEEDS_CONFIGURATION.value,
            "features": Banana.features()
            | {
                "foo": "bar"
            },  # This is the key assertion - the standard features get added
        },
        if_seq_no=seq_no,
        if_primary_term=primary_term,
    )


@pytest.mark.asyncio
async def test_get_connector_by_index():
    config = {
        "username": "elastic",
        "password": "changeme",
        "host": "http://nowhere.com:9200",
    }
    index_name = "search-mongo"
    doc = {"_id": "1", "_source": {"index_name": index_name}}

    es_client = ConnectorIndex(config)
    es_client.client = Mock()

    es_client.client.indices.refresh = AsyncMock()
    es_client.client.search = AsyncMock(
        return_value={"hits": {"total": {"value": 1}, "hits": [doc]}}
    )

    connector = await es_client.get_connector_by_index(index_name)
    es_client.client.search.assert_awaited_once()
    assert connector.id == doc["_id"]
    assert connector.index_name == index_name


@pytest.mark.parametrize(
    "indexed_timestamp, expected_datetime",
    [
        ("2024-05-28T12:34:56", datetime(2024, 5, 28, 12, 34, 56, tzinfo=timezone.utc)),
        (
            "2024-05-28T12:34:56+00:00",
            datetime(2024, 5, 28, 12, 34, 56, tzinfo=timezone.utc),
        ),
        (
            "2024-05-28T12:34:56+02:00",
            datetime(2024, 5, 28, 10, 34, 56, tzinfo=timezone.utc),
        ),
    ],
)
def test_property_as_datetime(indexed_timestamp, expected_datetime):
    connector = Connector(
        elastic_index=Mock(),
        doc_source={
            "_id": "test-tz-aware-timestamp-migration",
            "_source": {
                "last_sync_scheduled_at": indexed_timestamp,
                "last_incremental_sync_scheduled_at": indexed_timestamp,
                "last_access_control_sync_scheduled_at": indexed_timestamp,
            },
        },
    )

    assert connector.last_sync_scheduled_at == expected_datetime
    assert connector.last_incremental_sync_scheduled_at == expected_datetime
    assert connector.last_access_control_sync_scheduled_at == expected_datetime
