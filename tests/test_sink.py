#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import asyncio
import datetime
import itertools
import json
from copy import deepcopy
from unittest import mock
from unittest.mock import ANY, AsyncMock, Mock, call, patch

import pytest
from elasticsearch import ApiError, BadRequestError

from connectors.es import Mappings
from connectors.es.management_client import ESManagementClient
from connectors.es.settings import Settings
from connectors.es.sink import (
    BIN_DOCS_DOWNLOADED,
    BULK_OPERATIONS,
    BULK_RESPONSES,
    CREATES_QUEUED,
    DELETES_QUEUED,
    DOCS_EXTRACTED,
    OP_DELETE,
    OP_INDEX,
    OP_UPDATE,
    RESULT_SUCCESS,
    UPDATES_QUEUED,
    AsyncBulkRunningError,
    ElasticsearchOverloadedError,
    Extractor,
    ForceCanceledError,
    Sink,
    SyncOrchestrator,
)
from connectors.protocol import JobType, Pipeline
from connectors.protocol.connectors import (
    DELETED_DOCUMENT_COUNT,
    INDEXED_DOCUMENT_COUNT,
    INDEXED_DOCUMENT_VOLUME,
)
from connectors.utils import ErrorMonitor, TooManyErrors
from tests.commons import AsyncIterator

INDEX = "some-index"
TIMESTAMP = datetime.datetime(year=2023, month=1, day=1)
NO_FILTERING = ()
DOC_ONE_ID = 1
DOC_TWO_ID = 2
DOC_THREE_ID = 3

DOC_ONE = {"_id": DOC_ONE_ID, "_timestamp": TIMESTAMP}

DOC_ONE_DIFFERENT_TIMESTAMP = {
    "_id": DOC_ONE_ID,
    "_timestamp": TIMESTAMP + datetime.timedelta(days=1),
}

DOC_TWO = {"_id": 2, "_timestamp": TIMESTAMP}
DOC_THREE = {"_id": 3, "_timestamp": TIMESTAMP}
DOC_FOUR = {"_id": 4, "_timestamp": TIMESTAMP}

BULK_ACTION_ERROR = "some error"

SYNC_RULES_ENABLED = True
SYNC_RULES_DISABLED = False
CONTENT_EXTRACTION_ENABLED = True
CONTENT_EXTRACTION_DISABLED = False


def failed_bulk_action(doc_id, action, result, error=BULK_ACTION_ERROR):
    return {action: {"_id": doc_id, "result": result, "error": error}}


def successful_bulk_action(doc_id, action, result):
    return {action: {"_id": doc_id, "result": result}}


def successful_action_log_message(doc_id, action, result):
    return f"Successfully executed '{action}' on document with id '{doc_id}'. Result: {result}"


def successful_operation_with_non_successful_result_log_message(doc_id, action, result):
    return f"Executed '{action}' on document with id '{doc_id}', but got non-successful result: {result}"


def failed_action_log_message(doc_id, action, result, error=BULK_ACTION_ERROR):
    return (
        f"Failed to execute '{action}' on document with id '{doc_id}'. Error: {error}"
    )


@patch("connectors.es.sink.CANCELATION_TIMEOUT", -1)
@pytest.mark.asyncio
async def test_prepare_content_index_raise_error_when_index_creation_failed(
    mock_responses,
):
    index_name = "search-new-index"
    config = {"host": "http://nowhere.com:9200", "user": "tarek", "password": "blah"}
    headers = {"X-Elastic-Product": "Elasticsearch"}
    mock_responses.post(
        "http://nowhere.com:9200/.elastic-connectors/_refresh", headers=headers
    )

    # not found
    mock_responses.get(
        f"http://nowhere.com:9200/{index_name}?ignore_unavailable=true",
        headers=headers,
        status=200,
        body="{}",
    )

    mock_responses.put(
        f"http://nowhere.com:9200/{index_name}",
        payload={"_id": "1"},
        headers=headers,
        status=400,
    )

    es = SyncOrchestrator(config)
    es._sink = Mock()
    es._extractor = Mock()

    with pytest.raises(BadRequestError):
        await es.prepare_content_index(index_name)
    await es.close()


@patch("connectors.es.sink.CANCELATION_TIMEOUT", -1)
@pytest.mark.asyncio
async def test_prepare_content_index_create_index(
    mock_responses,
):
    index_name = "search-new-index"
    language_code = "jp"
    config = {"host": "http://nowhere.com:9200", "user": "tarek", "password": "blah"}
    headers = {"X-Elastic-Product": "Elasticsearch"}
    mock_responses.post(
        "http://nowhere.com:9200/.elastic-connectors/_refresh", headers=headers
    )

    # not found
    mock_responses.get(
        f"http://nowhere.com:9200/{index_name}?ignore_unavailable=true",
        headers=headers,
        status=200,
        body="{}",
    )

    mock_responses.put(
        f"http://nowhere.com:9200/{index_name}",
        payload={"_id": "1"},
        headers=headers,
    )

    mock_responses.put(
        f"http://nowhere.com:9200/{index_name}/_settings",
        payload={"_id": "1"},
        headers=headers,
    )

    es = SyncOrchestrator(config)
    es._sink = Mock()
    es._extractor = Mock()

    create_index_result = asyncio.Future()
    create_index_result.set_result({"acknowledged": True})

    with mock.patch.object(
        es.es_management_client,
        "create_content_index",
        return_value=create_index_result,
    ) as create_index_mock:
        await es.prepare_content_index(index_name, language_code)

        await es.close()

        create_index_mock.assert_called_with(index_name, language_code)


@patch("connectors.es.sink.CANCELATION_TIMEOUT", -1)
@pytest.mark.asyncio
async def test_prepare_content_index(mock_responses):
    language_code = "en"
    config = {"host": "http://nowhere.com:9200", "user": "tarek", "password": "blah"}
    headers = {"X-Elastic-Product": "Elasticsearch"}
    settings = Settings(language_code=language_code, analysis_icu=False).to_hash()
    # prepare-index, with mappings

    mappings = Mappings.default_text_fields_mappings(is_connectors_index=True)
    index_name = "search-new-index"

    response = {
        index_name: {
            "mappings": {},
            "settings": {},
        }
    }
    mock_responses.get(
        f"http://nowhere.com:9200/{index_name}?ignore_unavailable=true",
        headers=headers,
        status=200,
        body=json.dumps(response),
    )

    mock_responses.put(
        f"http://nowhere.com:9200/{index_name}/_mapping",
        headers=headers,
        payload=mappings,
        body='{"acknowledged": True}',
    )

    mock_responses.put(
        f"http://nowhere.com:9200/{index_name}/_settings",
        headers=headers,
        payload=settings,
        body='{"acknowledged": True}',
    )

    es = SyncOrchestrator(config)
    es._sink = Mock()
    es._extractor = Mock()
    with mock.patch.object(
        es.es_management_client,
        "ensure_content_index_mappings",
    ) as put_mapping_mock:
        await es.prepare_content_index(index_name, language_code)

        await es.close()

        put_mapping_mock.assert_called_with(
            index_name=index_name, index=response[index_name], desired_mappings=mappings
        )


def set_responses(mock_responses, ts=None):
    if ts is None:
        ts = datetime.datetime.now().isoformat()
    headers = {"X-Elastic-Product": "Elasticsearch"}
    mock_responses.head(
        "http://nowhere.com:9200/search-some-index",
        headers=headers,
    )

    mock_responses.get(
        "http://nowhere.com:9200/search-some-index",
        payload={"_id": "1"},
        headers=headers,
    )
    source_1 = {"id": "1", "_timestamp": ts}
    source_2 = {"id": "2", "_timestamp": ts}

    mock_responses.post(
        "http://nowhere.com:9200/search-some-index/_search?scroll=5m",
        payload={
            "_scroll_id": "1234",
            "_shards": {},
            "hits": {
                "hits": [
                    {"_id": "1", "_source": source_1},
                    {"_id": "2", "_source": source_2},
                ]
            },
        },
        headers=headers,
    )
    mock_responses.delete(
        "http://nowhere.com:9200/_search/scroll",
        headers=headers,
    )
    mock_responses.post(
        "http://nowhere.com:9200/_search/scroll",
        payload={
            "hits": {
                "hits": [
                    {"_id": "1", "_source": source_1},
                    {"_id": "2", "_source": source_2},
                ]
            }
        },
        headers=headers,
    )

    mock_responses.put(
        "http://nowhere.com:9200/_bulk?pipeline=ent-search-generic-ingestion",
        payload={
            "took": 7,
            "errors": False,
            "items": [
                {
                    "index": {
                        "_index": "test",
                        "_id": "1",
                        "_version": 1,
                        "result": "created",
                        "forced_refresh": False,
                        "status": 200,
                    }
                },
                {
                    "delete": {
                        "_index": "test",
                        "_id": "3",
                        "result": "deleted",
                        "status": 200,
                    }
                },
                {
                    "update": {
                        "_index": "test",
                        "_id": "3",
                        "result": "updated",
                        "status": 200,
                    }
                },
            ],
        },
        headers=headers,
    )


@patch("connectors.es.sink.CANCELATION_TIMEOUT", -1)
@pytest.mark.asyncio
async def test_async_bulk(mock_responses):
    config = {"host": "http://nowhere.com:9200", "user": "tarek", "password": "blah"}
    set_responses(mock_responses)

    es = SyncOrchestrator(config)
    pipeline = Pipeline({})

    async def get_docs():
        async def _dl_none(doit=True, timestamp=None):
            return None

        async def _dl(doit=True, timestamp=None):
            if not doit:
                return
            return {"TEXT": "DATA", "_timestamp": timestamp, "_id": "1"}

        yield (
            {
                "_id": "1",
                "_timestamp": datetime.datetime.now().isoformat(),
            },
            _dl,
            "index",
        )
        yield {"_id": "3"}, _dl_none, "index"

    await es.async_bulk("search-some-index", get_docs(), pipeline, JobType.FULL)
    while not es.done():
        await asyncio.sleep(0.1)

    ingestion_stats = es.ingestion_stats()

    assert ingestion_stats == {
        CREATES_QUEUED: 1,
        BIN_DOCS_DOWNLOADED: 1,
        UPDATES_QUEUED: 1,
        DELETES_QUEUED: 1,
        f"{BULK_OPERATIONS}.{OP_INDEX}": 2,
        f"{BULK_OPERATIONS}.{OP_DELETE}": 1,
        INDEXED_DOCUMENT_COUNT: 2,
        INDEXED_DOCUMENT_VOLUME: ANY,
        DELETED_DOCUMENT_COUNT: 1,
        f"{BULK_RESPONSES}.{OP_DELETE}": 1,
        f"{BULK_RESPONSES}.{OP_INDEX}": 1,
        f"{BULK_RESPONSES}.{OP_UPDATE}": 1,
        DOCS_EXTRACTED: 2,
        RESULT_SUCCESS: 3,
    }

    # 2nd sync
    set_responses(mock_responses)
    with pytest.raises(AsyncBulkRunningError):
        await es.async_bulk("search-some-index", get_docs(), pipeline, JobType.FULL)

    await es.close()


def index_operation(doc):
    # deepcopy as get_docs mutates docs
    doc_copy = deepcopy(doc)
    doc_id = doc_copy["id"] = doc_copy.pop("_id")

    return {"_op_type": "index", "_index": INDEX, "_id": doc_id, "doc": doc_copy}


def update_operation(doc):
    # deepcopy as get_docs mutates docs
    doc_copy = deepcopy(doc)
    doc_id = doc_copy["id"] = doc_copy.pop("_id")

    return {"_op_type": "update", "_index": INDEX, "_id": doc_id, "doc": doc_copy}


def delete_operation(doc):
    return {"_op_type": "delete", "_index": INDEX, "_id": doc["_id"]}


def end_docs_operation():
    return "END_DOCS"


def created(doc_count):
    """Used for test readability."""
    return doc_count


def updated(doc_count):
    """Used for test readability."""
    return doc_count


def deleted(doc_count):
    """Used for test readability."""
    return doc_count


def total_downloads(count):
    """Used for test readability."""
    return count


async def queue_mock():
    queue = Mock()
    future = asyncio.Future()
    future.set_result(1)
    queue.put = Mock(return_value=future)
    return queue


def lazy_download_fake(doc):
    async def lazy_download(**kwargs):
        # also deepcopy to prevent side effects as docs get mutated in get_docs
        return deepcopy(doc)

    return lazy_download


def crashing_lazy_download_fake():
    async def lazy_download(**kwargs):
        msg = "Could not download"
        raise Exception(msg)

    return lazy_download


def queue_called_with_operations(queue, operations):
    expected_calls = [call(operation) for operation in operations]
    actual_calls = queue.put.call_args_list

    return actual_calls == expected_calls and queue.put.call_count == len(
        expected_calls
    )


async def basic_rule_engine_mock(return_values):
    basic_rule_engine = Mock()
    basic_rule_engine.should_ingest = (
        Mock(side_effect=list(return_values))
        if len(return_values)
        else Mock(return_value=True)
    )
    return basic_rule_engine


async def lazy_downloads_mock():
    lazy_downloads = Mock()
    future = asyncio.Future()
    future.set_result(1)
    lazy_downloads.put = Mock(return_value=future)
    return lazy_downloads


async def setup_extractor(
    queue,
    basic_rule_engine=None,
    sync_rules_enabled=False,
    content_extraction_enabled=False,
):
    config = {
        "username": "elastic",
        "password": "changeme",
        "host": "http://nowhere.com:9200",
    }
    # filtering content doesn't matter as the BasicRuleEngine behavior is mocked
    filter_mock = Mock()
    filter_mock.get_active_filter = Mock(return_value={})
    extractor = Extractor(
        ESManagementClient(config),
        queue,
        INDEX,
        filter_=filter_mock,
        content_extraction_enabled=content_extraction_enabled,
    )
    extractor.basic_rule_engine = basic_rule_engine if sync_rules_enabled else None
    return extractor


@pytest.mark.parametrize(
    "existing_docs, docs_from_source, doc_should_ingest, sync_rules_enabled, content_extraction_enabled, expected_queue_operations, "
    "expected_total_docs_updated, expected_total_docs_created, expected_total_docs_deleted, expected_total_downloads",
    [
        (
            # no docs exist, data source has doc 1 -> ingest doc 1
            [],
            [(DOC_ONE, None, "index")],
            NO_FILTERING,
            SYNC_RULES_ENABLED,
            CONTENT_EXTRACTION_ENABLED,
            [index_operation(DOC_ONE), end_docs_operation()],
            updated(0),
            created(1),
            deleted(0),
            total_downloads(0),
        ),
        (
            # doc 1 is present, nothing in source -> delete one doc
            [DOC_ONE],
            [],
            NO_FILTERING,
            SYNC_RULES_ENABLED,
            CONTENT_EXTRACTION_ENABLED,
            [delete_operation(DOC_ONE), end_docs_operation()],
            updated(0),
            created(0),
            deleted(1),
            total_downloads(0),
        ),
        (
            # doc 2 is present, data source only returns doc 1 -> delete doc 2 and ingest doc 1
            [DOC_TWO],
            [(DOC_ONE, None, "index")],
            NO_FILTERING,
            SYNC_RULES_ENABLED,
            CONTENT_EXTRACTION_ENABLED,
            [index_operation(DOC_ONE), delete_operation(DOC_TWO), end_docs_operation()],
            updated(0),
            created(1),
            deleted(1),
            total_downloads(0),
        ),
        (
            # doc 1 is present, data source also has doc 1 with the same timestamp -> doc one is updated
            [DOC_ONE],
            [(DOC_ONE, None, "index")],
            NO_FILTERING,
            SYNC_RULES_ENABLED,
            CONTENT_EXTRACTION_ENABLED,
            [
                # update happens through overwriting
                index_operation(DOC_ONE),
                end_docs_operation(),
            ],
            updated(1),
            created(0),
            deleted(0),
            total_downloads(0),
        ),
        (
            # doc 1 is present, data source has doc 1 with different timestamp -> doc is updated
            [DOC_ONE],
            [(DOC_ONE_DIFFERENT_TIMESTAMP, None, "index")],
            NO_FILTERING,
            SYNC_RULES_ENABLED,
            CONTENT_EXTRACTION_ENABLED,
            [
                # update happens through overwriting
                index_operation(DOC_ONE_DIFFERENT_TIMESTAMP),
                end_docs_operation(),
            ],
            updated(1),
            created(0),
            deleted(0),
            total_downloads(0),
        ),
        (
            [],
            [(DOC_ONE, None, "index")],
            # filter out doc 1
            (False,),
            SYNC_RULES_ENABLED,
            CONTENT_EXTRACTION_ENABLED,
            [
                # should not ingest doc 1
                end_docs_operation()
            ],
            updated(0),
            created(0),
            deleted(0),
            total_downloads(0),
        ),
        (
            # doc 1 is present, data source has doc 1, doc 1 should be filtered -> delete doc 1
            [DOC_ONE],
            [(DOC_ONE, None, "index")],
            # filter out doc 1
            (False,),
            SYNC_RULES_ENABLED,
            CONTENT_EXTRACTION_ENABLED,
            [
                delete_operation(DOC_ONE),
                end_docs_operation(),
            ],
            updated(0),
            created(0),
            deleted(1),
            total_downloads(0),
        ),
        (
            # doc 1 is present, data source has doc 1 (different timestamp), doc 1 should be filtered -> delete doc 1
            [DOC_ONE],
            [(DOC_ONE_DIFFERENT_TIMESTAMP, None, "index")],
            # still filter out doc 1
            (False,),
            SYNC_RULES_ENABLED,
            CONTENT_EXTRACTION_ENABLED,
            [delete_operation(DOC_ONE), end_docs_operation()],
            updated(0),
            created(0),
            deleted(1),
            total_downloads(0),
        ),
        (
            [],
            [(DOC_ONE, None, "index")],
            # filtering throws exception
            [Exception()],
            SYNC_RULES_ENABLED,
            CONTENT_EXTRACTION_ENABLED,
            ["EXTRACTOR_ERROR"],
            updated(0),
            created(0),
            deleted(0),
            total_downloads(0),
        ),
        (
            [],
            # no doc present, lazy download for doc 1 specified -> index and increment the downloads counter
            [(DOC_ONE, lazy_download_fake(DOC_ONE), "index")],
            NO_FILTERING,
            SYNC_RULES_ENABLED,
            CONTENT_EXTRACTION_ENABLED,
            [index_operation(DOC_ONE), end_docs_operation()],
            updated(0),
            created(1),
            deleted(0),
            total_downloads(1),
        ),
        (
            # doc 1 present, data source has doc 1 -> lazy download occurs
            [DOC_ONE],
            [(DOC_ONE, lazy_download_fake(DOC_ONE), "index")],
            NO_FILTERING,
            SYNC_RULES_ENABLED,
            CONTENT_EXTRACTION_ENABLED,
            [index_operation(DOC_ONE), end_docs_operation()],
            updated(1),
            created(0),
            deleted(0),
            total_downloads(1),
        ),
        (
            # doc 1 present, data source has doc 1 with different timestamp
            # lazy download coroutine present -> execute lazy download
            [DOC_ONE],
            [
                (
                    DOC_ONE_DIFFERENT_TIMESTAMP,
                    lazy_download_fake(DOC_ONE_DIFFERENT_TIMESTAMP),
                    "index",
                )
            ],
            NO_FILTERING,
            SYNC_RULES_ENABLED,
            CONTENT_EXTRACTION_ENABLED,
            [index_operation(DOC_ONE_DIFFERENT_TIMESTAMP), end_docs_operation()],
            updated(1),
            created(0),
            deleted(0),
            total_downloads(1),
        ),
        (
            [],
            [(DOC_ONE, None, "index")],
            # filter out doc 1
            (False,),
            SYNC_RULES_DISABLED,
            CONTENT_EXTRACTION_ENABLED,
            [
                # should ingest doc 1 as sync rules are disabled
                index_operation(DOC_ONE),
                end_docs_operation(),
            ],
            updated(0),
            created(1),
            deleted(0),
            total_downloads(0),
        ),
        (
            # content_extraction_enabled is false and no download is provided,
            # indexing should still work, but nothing should be downloaded
            [],
            [(DOC_ONE, None, "index")],
            NO_FILTERING,
            SYNC_RULES_ENABLED,
            CONTENT_EXTRACTION_DISABLED,
            [index_operation(DOC_ONE), end_docs_operation()],
            updated(0),
            created(1),
            deleted(0),
            total_downloads(0),
        ),
        (
            # content_extraction_enabled is false but a download is also provided,
            # indexing should still work, but nothing should be downloaded
            [],
            [(DOC_ONE, lazy_download_fake(DOC_ONE), "index")],
            NO_FILTERING,
            SYNC_RULES_ENABLED,
            CONTENT_EXTRACTION_DISABLED,
            [index_operation(DOC_ONE), end_docs_operation()],
            updated(0),
            created(1),
            deleted(0),
            total_downloads(0),
        ),
    ],
)
@mock.patch(
    "connectors.es.management_client.ESManagementClient.yield_existing_documents_metadata"
)
@pytest.mark.asyncio
async def test_get_docs(
    yield_existing_documents_metadata,
    existing_docs,
    docs_from_source,
    doc_should_ingest,
    sync_rules_enabled,
    content_extraction_enabled,
    expected_queue_operations,
    expected_total_docs_updated,
    expected_total_docs_created,
    expected_total_docs_deleted,
    expected_total_downloads,
):
    lazy_downloads = await lazy_downloads_mock()

    yield_existing_documents_metadata.return_value = AsyncIterator(
        [(doc["_id"], doc["_timestamp"]) for doc in existing_docs]
    )

    with mock.patch("connectors.utils.ConcurrentTasks", return_value=lazy_downloads):
        queue = await queue_mock()
        basic_rule_engine = await basic_rule_engine_mock(doc_should_ingest)

        # deep copying docs is needed as get_docs mutates the document ids which has side effects on other test
        # instances
        doc_generator = AsyncIterator([deepcopy(doc) for doc in docs_from_source])

        extractor = await setup_extractor(
            queue,
            basic_rule_engine=basic_rule_engine,
            sync_rules_enabled=sync_rules_enabled,
            content_extraction_enabled=content_extraction_enabled,
        )

        await extractor.run(doc_generator, JobType.FULL)

        assert extractor.counters.get(UPDATES_QUEUED) == expected_total_docs_updated
        assert extractor.counters.get(CREATES_QUEUED) == expected_total_docs_created
        assert extractor.counters.get(DELETES_QUEUED) == expected_total_docs_deleted
        assert extractor.counters.get(BIN_DOCS_DOWNLOADED) == expected_total_downloads

        assert queue_called_with_operations(queue, expected_queue_operations)


@pytest.mark.parametrize(
    "docs_from_source, doc_should_ingest, sync_rules_enabled, content_extraction_enabled, expected_queue_operations, "
    "expected_total_docs_updated, expected_total_docs_created, expected_total_docs_deleted, expected_total_downloads",
    [
        (
            [],
            NO_FILTERING,
            SYNC_RULES_ENABLED,
            CONTENT_EXTRACTION_ENABLED,
            [end_docs_operation()],
            updated(0),
            created(0),
            deleted(0),
            total_downloads(0),
        ),
        (
            [(DOC_ONE, None, "index")],
            NO_FILTERING,
            SYNC_RULES_ENABLED,
            CONTENT_EXTRACTION_ENABLED,
            [index_operation(DOC_ONE), end_docs_operation()],
            updated(0),
            created(1),
            deleted(0),
            total_downloads(0),
        ),
        (
            [(DOC_ONE, None, "update")],
            NO_FILTERING,
            SYNC_RULES_ENABLED,
            CONTENT_EXTRACTION_ENABLED,
            [update_operation(DOC_ONE), end_docs_operation()],
            updated(1),
            created(0),
            deleted(0),
            total_downloads(0),
        ),
        (
            [(DOC_ONE, None, "delete")],
            NO_FILTERING,
            SYNC_RULES_ENABLED,
            CONTENT_EXTRACTION_ENABLED,
            [delete_operation(DOC_ONE), end_docs_operation()],
            updated(0),
            created(0),
            deleted(1),
            total_downloads(0),
        ),
        (
            [(DOC_ONE, None, "index"), (DOC_TWO, None, "delete")],
            NO_FILTERING,
            SYNC_RULES_ENABLED,
            CONTENT_EXTRACTION_ENABLED,
            [index_operation(DOC_ONE), delete_operation(DOC_TWO), end_docs_operation()],
            updated(0),
            created(1),
            deleted(1),
            total_downloads(0),
        ),
        (
            [(DOC_ONE, None, "index")],
            # filter out doc 1
            (False,),
            SYNC_RULES_ENABLED,
            CONTENT_EXTRACTION_ENABLED,
            [
                # should not ingest doc 1
                end_docs_operation()
            ],
            updated(0),
            created(0),
            deleted(0),
            total_downloads(0),
        ),
        (
            [(DOC_ONE, None, "index")],
            # filtering throws exception
            [Exception()],
            SYNC_RULES_ENABLED,
            CONTENT_EXTRACTION_ENABLED,
            ["EXTRACTOR_ERROR"],
            updated(0),
            created(0),
            deleted(0),
            total_downloads(0),
        ),
        (
            # no doc present, lazy download for doc 1 specified -> index and increment the downloads counter
            [(DOC_ONE, lazy_download_fake(DOC_ONE), "index")],
            NO_FILTERING,
            SYNC_RULES_ENABLED,
            CONTENT_EXTRACTION_ENABLED,
            [index_operation(DOC_ONE), end_docs_operation()],
            updated(0),
            created(1),
            deleted(0),
            total_downloads(1),
        ),
        (
            [(DOC_ONE, None, "index")],
            # filter out doc 1
            (False,),
            SYNC_RULES_DISABLED,
            CONTENT_EXTRACTION_ENABLED,
            [
                # should ingest doc 1 as sync rules are disabled
                index_operation(DOC_ONE),
                end_docs_operation(),
            ],
            updated(0),
            created(1),
            deleted(0),
            total_downloads(0),
        ),
        (
            [(DOC_ONE, None, "index")],
            NO_FILTERING,
            SYNC_RULES_ENABLED,
            CONTENT_EXTRACTION_DISABLED,
            [index_operation(DOC_ONE), end_docs_operation()],
            updated(0),
            created(1),
            deleted(0),
            total_downloads(0),
        ),
        (
            [(DOC_ONE, lazy_download_fake(DOC_ONE), "index")],
            NO_FILTERING,
            SYNC_RULES_ENABLED,
            CONTENT_EXTRACTION_DISABLED,
            [index_operation(DOC_ONE), end_docs_operation()],
            updated(0),
            created(1),
            deleted(0),
            total_downloads(0),
        ),
    ],
)
@pytest.mark.asyncio
async def test_get_docs_incrementally(
    docs_from_source,
    doc_should_ingest,
    sync_rules_enabled,
    content_extraction_enabled,
    expected_queue_operations,
    expected_total_docs_updated,
    expected_total_docs_created,
    expected_total_docs_deleted,
    expected_total_downloads,
):
    lazy_downloads = await lazy_downloads_mock()

    with mock.patch("connectors.utils.ConcurrentTasks", return_value=lazy_downloads):
        queue = await queue_mock()
        basic_rule_engine = await basic_rule_engine_mock(doc_should_ingest)

        # deep copying docs is needed as get_docs mutates the document ids which has side effects on other test
        # instances
        doc_generator = AsyncIterator([deepcopy(doc) for doc in docs_from_source])

        extractor = await setup_extractor(
            queue,
            basic_rule_engine=basic_rule_engine,
            sync_rules_enabled=sync_rules_enabled,
            content_extraction_enabled=content_extraction_enabled,
        )

        await extractor.run(doc_generator, JobType.INCREMENTAL)

        assert extractor.counters.get(UPDATES_QUEUED) == expected_total_docs_updated
        assert extractor.counters.get(CREATES_QUEUED) == expected_total_docs_created
        assert extractor.counters.get(DELETES_QUEUED) == expected_total_docs_deleted
        assert extractor.counters.get(BIN_DOCS_DOWNLOADED) == expected_total_downloads

        assert queue_called_with_operations(queue, expected_queue_operations)


@pytest.mark.parametrize(
    "existing_docs, docs_from_source, expected_queue_operations, "
    "expected_total_docs_updated, expected_total_docs_created, expected_total_docs_deleted",
    [
        (
            [],
            [],
            [end_docs_operation()],
            updated(0),
            created(0),
            deleted(0),
        ),
        (
            [],
            [(DOC_ONE, None, None)],
            [index_operation(DOC_ONE), end_docs_operation()],
            updated(0),
            created(1),
            deleted(0),
        ),
        (
            [DOC_ONE_DIFFERENT_TIMESTAMP],
            [(DOC_ONE, None, None)],
            [update_operation(DOC_ONE), end_docs_operation()],
            updated(1),
            created(0),
            deleted(0),
        ),
        (
            [DOC_ONE],
            [],
            [delete_operation(DOC_ONE), end_docs_operation()],
            updated(0),
            created(0),
            deleted(1),
        ),
        (
            [DOC_TWO],
            [(DOC_ONE, None, None)],
            [index_operation(DOC_ONE), delete_operation(DOC_TWO), end_docs_operation()],
            updated(0),
            created(1),
            deleted(1),
        ),
        (
            [DOC_ONE_DIFFERENT_TIMESTAMP, DOC_TWO],
            [(DOC_ONE, None, None)],
            [
                update_operation(DOC_ONE),
                delete_operation(DOC_TWO),
                end_docs_operation(),
            ],
            updated(1),
            created(0),
            deleted(1),
        ),
        (
            [DOC_ONE_DIFFERENT_TIMESTAMP, DOC_TWO],
            [(DOC_ONE, None, None), (DOC_THREE, None, None)],
            [
                update_operation(DOC_ONE),
                index_operation(DOC_THREE),
                delete_operation(DOC_TWO),
                end_docs_operation(),
            ],
            updated(1),
            created(1),
            deleted(1),
        ),
    ],
)
@mock.patch(
    "connectors.es.management_client.ESManagementClient.yield_existing_documents_metadata"
)
@pytest.mark.asyncio
async def test_get_access_control_docs(
    yield_existing_documents_metadata,
    existing_docs,
    docs_from_source,
    expected_queue_operations,
    expected_total_docs_updated,
    expected_total_docs_created,
    expected_total_docs_deleted,
):
    yield_existing_documents_metadata.return_value = AsyncIterator(
        [(doc["_id"], doc["_timestamp"]) for doc in existing_docs]
    )

    queue = await queue_mock()
    # deep copying docs is needed as get_access_control_docs mutates the document ids which has side effects on other test
    # instances
    doc_generator = AsyncIterator([deepcopy(doc) for doc in docs_from_source])

    extractor = await setup_extractor(queue=queue)

    await extractor.run(doc_generator, JobType.ACCESS_CONTROL)

    assert extractor.counters.get(UPDATES_QUEUED) == expected_total_docs_updated
    assert extractor.counters.get(CREATES_QUEUED) == expected_total_docs_created
    assert extractor.counters.get(DELETES_QUEUED) == expected_total_docs_deleted

    assert queue_called_with_operations(queue, expected_queue_operations)


STATS = {
    "index": {"1": 1},
    "update": {"2": 1},
    "delete": {"3": 0},
}

INDEX_ITEM = {"index": {"_id": "1", "result": "created"}}
FAILED_INDEX_ITEM = {"index": {"_id": "1"}}
UPDATE_ITEM = {"update": {"_id": "2", "result": "updated"}}
FAILED_UPDATE_ITEM = {"update": {"_id": "2"}}
DELETE_ITEM = {"delete": {"_id": "3", "result": "deleted"}}
FAILED_DELETE_ITEM = {"delete": {"_id": "3"}}


@pytest.mark.parametrize(
    "res, expected_result",
    [
        (
            {"items": [INDEX_ITEM, UPDATE_ITEM, DELETE_ITEM]},
            {
                "indexed_document_count": 2,
                "indexed_document_volume": 2,
                "deleted_document_count": 1,
            },
        ),
        (
            {"items": [FAILED_INDEX_ITEM, UPDATE_ITEM, DELETE_ITEM]},
            {
                "indexed_document_count": 1,
                "indexed_document_volume": 1,
                "deleted_document_count": 1,
            },
        ),
        (
            {"items": [INDEX_ITEM, FAILED_UPDATE_ITEM, DELETE_ITEM]},
            {
                "indexed_document_count": 1,
                "indexed_document_volume": 1,
                "deleted_document_count": 1,
            },
        ),
        (
            {"items": [INDEX_ITEM, UPDATE_ITEM, FAILED_DELETE_ITEM]},
            {
                "indexed_document_count": 2,
                "indexed_document_volume": 2,
                "deleted_document_count": 0,
            },
        ),
        (
            {"items": [INDEX_ITEM, FAILED_UPDATE_ITEM, FAILED_DELETE_ITEM]},
            {
                "indexed_document_count": 1,
                "indexed_document_volume": 1,
                "deleted_document_count": 0,
            },
        ),
        (
            {"items": [FAILED_INDEX_ITEM, UPDATE_ITEM, FAILED_DELETE_ITEM]},
            {
                "indexed_document_count": 1,
                "indexed_document_volume": 1,
                "deleted_document_count": 0,
            },
        ),
        (
            {"items": [FAILED_INDEX_ITEM, FAILED_UPDATE_ITEM, DELETE_ITEM]},
            {
                "indexed_document_count": 0,
                "indexed_document_volume": 0,
                "deleted_document_count": 1,
            },
        ),
        (
            {"items": [FAILED_INDEX_ITEM, FAILED_UPDATE_ITEM, FAILED_DELETE_ITEM]},
            {
                "indexed_document_count": 0,
                "indexed_document_volume": 0,
                "deleted_document_count": 0,
            },
        ),
    ],
)
def test_bulk_populate_stats(res, expected_result):
    sink = Sink(
        client=None,
        queue=None,
        error_monitor=Mock(),
        chunk_size=0,
        pipeline=None,
        chunk_mem_size=0,
        max_concurrency=0,
        max_retries=3,
        retry_interval=10,
    )
    sink._populate_stats(deepcopy(STATS), res)

    assert (
        sink.counters.get(INDEXED_DOCUMENT_COUNT)
        == expected_result[INDEXED_DOCUMENT_COUNT]
    )
    assert (
        sink.counters.get(INDEXED_DOCUMENT_VOLUME)
        == expected_result[INDEXED_DOCUMENT_VOLUME]
    )
    assert (
        sink.counters.get(DELETED_DOCUMENT_COUNT)
        == expected_result[DELETED_DOCUMENT_COUNT]
    )


@pytest.mark.asyncio
async def test_batch_bulk_with_retry():
    config = {
        "username": "elastic",
        "password": "changeme",
        "host": "http://nowhere.com:9200",
    }
    client = ESManagementClient(config)
    client.client = AsyncMock()
    sink = Sink(
        client=client,
        queue=None,
        error_monitor=Mock(),
        chunk_size=0,
        pipeline={"name": "pipeline"},
        chunk_mem_size=0,
        max_concurrency=0,
        max_retries=3,
        retry_interval=10,
    )

    with mock.patch.object(asyncio, "sleep"):
        # first call raises exception, and the second call succeeds
        error_meta = Mock()
        error_meta.status = 429
        first_call_error = ApiError(429, meta=error_meta, body="error")
        second_call_result = {"items": []}
        client.client.bulk = AsyncMock(
            side_effect=[first_call_error, second_call_result]
        )
        await sink._batch_bulk([], {OP_INDEX: {}, OP_UPDATE: {}, OP_DELETE: {}})

        assert client.client.bulk.await_count == 2


@pytest.mark.asyncio
async def test_batch_bulk_with_error_monitor():
    config = {
        "username": "elastic",
        "password": "changeme",
        "host": "http://nowhere.com:9200",
    }
    client = ESManagementClient(config)
    client.client = AsyncMock()
    sink = Sink(
        client=client,
        queue=None,
        error_monitor=ErrorMonitor(max_consecutive_errors=10),
        chunk_size=0,
        pipeline={"name": "pipeline"},
        chunk_mem_size=0,
        max_concurrency=0,
        max_retries=3,
        retry_interval=10,
    )

    def _create_response(num_successful, num_failed):
        call_result = {"items": []}
        for id_ in range(num_successful):
            call_result["items"].append(
                {OP_UPDATE: {"_id": str(id_), "result": "updated"}}
            )

        for id_ in range(num_failed):
            call_result["items"].append(
                {
                    OP_UPDATE: {
                        "_id": str(num_successful + id_),
                        "result": "failed",
                        "error": "something went wrong",
                    }
                }
            )

        return call_result

    # First add 45 successful items and 5 failed
    first_call_result = _create_response(45, 5)

    # Then add 40 successful items and 10 failed
    second_call_result = _create_response(40, 10)

    # Lastly add 0 successful items and 1 failed
    # This should fail the test as max consecutive_error_count=10
    # 10 errors came last in second call, and this one just triggers it
    third_call_result = _create_response(0, 1)

    client.client.bulk = AsyncMock(
        side_effect=[first_call_result, second_call_result, third_call_result]
    )

    await sink._batch_bulk([], {OP_INDEX: {}, OP_UPDATE: {}, OP_DELETE: {}})
    await sink._batch_bulk([], {OP_INDEX: {}, OP_UPDATE: {}, OP_DELETE: {}})
    with pytest.raises(TooManyErrors):
        await sink._batch_bulk([], {OP_INDEX: {}, OP_UPDATE: {}, OP_DELETE: {}})


@pytest.mark.asyncio
async def test_batch_bulk_with_errors(patch_logger):
    config = {
        "username": "elastic",
        "password": "changeme",
        "host": "http://nowhere.com:9200",
    }
    client = ESManagementClient(config)
    client.client = AsyncMock()
    sink = Sink(
        client=client,
        queue=None,
        error_monitor=Mock(),
        chunk_size=0,
        pipeline={"name": "pipeline"},
        chunk_mem_size=0,
        max_concurrency=0,
        max_retries=3,
        retry_interval=10,
    )

    with mock.patch.object(asyncio, "sleep"):
        error = {
            "type": "illegal_argument_exception",
            "reason": "if _id is specified it must not be empty",
        }
        mock_result = {
            "errors": True,
            "items": [{"index": {"_id": "1", "error": error}}],
        }
        client.client.bulk = AsyncMock(return_value=mock_result)
        await sink._batch_bulk([], {OP_INDEX: {"1": 20}, OP_UPDATE: {}, OP_DELETE: {}})
        patch_logger.assert_present(f"operation index failed for doc 1, {error}")


@patch("connectors.es.sink.CANCELATION_TIMEOUT", -1)
@pytest.mark.parametrize(
    "extractor_task, extractor_task_done, sink_task, sink_task_done, expected_result",
    [
        (None, False, None, False, True),
        (Mock(), False, None, False, False),
        (Mock(), True, None, False, True),
        (None, False, Mock(), False, False),
        (None, False, Mock(), True, True),
        (Mock(), False, Mock(), False, False),
        (Mock(), False, Mock(), True, False),
        (Mock(), True, Mock(), False, False),
        (Mock(), True, Mock(), True, True),
    ],
)
@pytest.mark.asyncio
async def test_sync_orchestrator_done_and_cleanup(
    extractor_task, extractor_task_done, sink_task, sink_task_done, expected_result
):
    if extractor_task is not None:
        extractor_task.cancel = Mock()
        extractor_task.done.return_value = extractor_task_done
    if sink_task is not None:
        sink_task.cancel = Mock()
        sink_task.done.return_value = sink_task_done

    config = {"host": "http://nowhere.com:9200", "user": "tarek", "password": "blah"}
    es = SyncOrchestrator(config)
    es._extractor = Mock()
    es._extractor.error = None
    es._sink = Mock()
    es._sink.error = None
    es._extractor_task = extractor_task
    es._sink_task = sink_task

    assert es.done() == expected_result

    await es.close()

    if extractor_task and not extractor_task_done:
        extractor_task.cancel.assert_called_once()
    if sink_task and not sink_task_done:
        sink_task.cancel.assert_called_once()


@pytest.mark.asyncio
async def test_extractor_put_doc():
    doc = {"id": 123}
    queue = Mock()
    queue.put = AsyncMock()
    extractor = Extractor(None, queue, INDEX)

    await extractor.put_doc(doc)
    queue.put.assert_awaited_once_with(doc)


@pytest.mark.asyncio
@mock.patch(
    "connectors.es.management_client.ESManagementClient.yield_existing_documents_metadata"
)
@mock.patch("connectors.utils.ConcurrentTasks.cancel")
async def test_extractor_get_docs_when_downloads_fail(
    yield_existing_documents_metadata, concurrent_tasks_cancel
):
    queue = await queue_mock()

    yield_existing_documents_metadata.return_value = AsyncIterator([])

    docs_from_source = [
        (DOC_ONE, crashing_lazy_download_fake(), "index"),
        (DOC_TWO, crashing_lazy_download_fake(), "index"),
        (DOC_THREE, crashing_lazy_download_fake(), "index"),
        (DOC_FOUR, crashing_lazy_download_fake(), "index"),
    ]
    # deep copying docs is needed as get_docs mutates the document ids which has side effects on other test
    # instances
    doc_generator = AsyncIterator([deepcopy(doc) for doc in docs_from_source])

    extractor = await setup_extractor(queue, content_extraction_enabled=True)

    await extractor.run(doc_generator, JobType.FULL)
    concurrent_tasks_cancel.assert_called_once()


@pytest.mark.asyncio
async def test_force_canceled_extractor_put_doc():
    doc = {"id": 123}
    queue = Mock()
    queue.put = AsyncMock()
    extractor = Extractor(None, queue, INDEX)

    extractor.force_cancel()
    with pytest.raises(ForceCanceledError):
        await extractor.put_doc(doc)
        queue.put.assert_not_awaited()


@pytest.mark.asyncio
async def test_force_canceled_extractor_with_other_errors(patch_logger):
    queue = Mock()
    queue.put = AsyncMock()
    extractor = Extractor(
        None,
        queue,
        INDEX,
    )
    generator = AsyncMock(side_effect=Exception("a non-ForceCanceledError"))

    extractor.force_cancel()
    # no error thrown
    await extractor.run(generator, JobType.FULL)
    patch_logger.assert_present(
        "Extractor did not stop within 5 seconds of cancelation, force-canceling the task."
    )


@pytest.mark.asyncio
async def test_sink_fetch_doc():
    expected_doc = {"id": 123}
    queue = Mock()
    queue.get = AsyncMock(return_value=expected_doc)
    sink = Sink(
        None,
        queue,
        error_monitor=Mock(),
        chunk_size=0,
        pipeline={"name": "pipeline"},
        chunk_mem_size=0,
        max_concurrency=0,
        max_retries=3,
        retry_interval=10,
    )

    doc = await sink.fetch_doc()
    queue.get.assert_awaited_once()
    assert doc == expected_doc


@pytest.mark.asyncio
async def test_force_canceled_sink_fetch_doc():
    expected_doc = {"id": 123}
    queue = Mock()
    queue.get = AsyncMock(return_value=expected_doc)
    sink = Sink(
        None,
        queue,
        error_monitor=Mock(),
        chunk_size=0,
        pipeline={"name": "pipeline"},
        chunk_mem_size=0,
        max_concurrency=0,
        max_retries=3,
        retry_interval=10,
    )

    sink.force_cancel()
    with pytest.raises(ForceCanceledError):
        await sink.fetch_doc()
        queue.get.assert_not_awaited()


@pytest.mark.asyncio
async def test_force_canceled_sink_with_other_errors(patch_logger):
    queue = Mock()
    queue.get = AsyncMock(side_effect=Exception("a non-ForceCanceledError"))
    sink = Sink(
        None,
        queue,
        error_monitor=Mock(),
        chunk_size=0,
        pipeline={"name": "pipeline"},
        chunk_mem_size=0,
        max_concurrency=0,
        max_retries=3,
        retry_interval=10,
    )

    sink.force_cancel()
    # no error thrown
    await sink.run()
    patch_logger.assert_present(
        "Sink did not stop within 5 seconds of cancelation, force-canceling the task."
    )


@pytest.mark.parametrize(
    "extractor_task_done, sink_task_done, force_cancel",
    [
        (
            itertools.chain([False, False, False], itertools.repeat(True)),
            itertools.chain([False, False], itertools.repeat(True)),
            False,
        ),
        (
            itertools.chain([False, False, False], itertools.repeat(True)),
            itertools.repeat(False),
            True,
        ),
        (
            itertools.repeat(False),
            itertools.chain([False, False], itertools.repeat(True)),
            True,
        ),
        (
            itertools.repeat(False),
            itertools.repeat(False),
            True,
        ),
    ],
)
@pytest.mark.asyncio
async def test_cancel_sync(extractor_task_done, sink_task_done, force_cancel):
    config = {"host": "http://nowhere.com:9200", "user": "tarek", "password": "blah"}
    es = SyncOrchestrator(config)
    es._extractor = Mock()
    es._extractor.force_cancel = Mock()

    es._sink = Mock()
    es._sink.force_cancel = Mock()

    es._extractor_task = mock.create_autospec(asyncio.Task)
    es._extractor_task.cancel = Mock()
    es._extractor_task.done = Mock(side_effect=extractor_task_done)

    es._sink_task = mock.create_autospec(asyncio.Task)
    es._sink_task.cancel = Mock()
    es._sink_task.done = Mock(side_effect=sink_task_done)

    with mock.patch.object(asyncio, "sleep"):
        await es.cancel()
        es._extractor_task.cancel.assert_called_once()
        es._sink_task.cancel.assert_called_once()

        if force_cancel:
            es._extractor.force_cancel.assert_called_once()
            es._sink.force_cancel.assert_called_once()
        else:
            es._extractor.force_cancel.assert_not_called()
            es._sink.force_cancel.assert_not_called()


@pytest.mark.asyncio
async def test_extractor_run_when_mem_full_is_raised():
    docs_from_source = [
        {"_id": 1},
        {"_id": 2},
        {"_id": 3},
    ]
    es_client = AsyncMock()
    es_client.yield_existing_documents_metadata = Mock(return_value=AsyncIterator([]))

    queue = await queue_mock()
    queue.clear = Mock()

    def _put_side_effect(value):
        if isinstance(value, str):
            pass
        else:
            raise asyncio.QueueFull()

    queue.put = AsyncMock(side_effect=_put_side_effect)
    # deep copying docs is needed as get_access_control_docs mutates the document ids which has side effects on other test
    # instances
    doc_generator = AsyncIterator([(doc, None, "") for doc in docs_from_source])

    extractor = Extractor(es_client, queue, INDEX)

    await extractor.run(doc_generator, JobType.FULL)

    queue.clear.assert_called_once()
    assert isinstance(extractor.error, ElasticsearchOverloadedError)


@pytest.mark.asyncio
async def test_should_not_log_bulk_operations_if_doc_id_tracing_is_disabled(
    patch_logger,
):
    action = "create"
    result = "created"
    operations = []
    client = Mock()
    client.bulk_insert = AsyncMock(
        return_value={"items": [successful_bulk_action(DOC_ONE_ID, action, result)]}
    )
    sink = Sink(
        client=client,
        queue=Mock(),
        error_monitor=Mock(),
        chunk_size=0,
        pipeline={"name": "pipeline"},
        chunk_mem_size=0,
        max_concurrency=0,
        max_retries=3,
        retry_interval=10,
        enable_bulk_operations_logging=False,
    )

    await sink._batch_bulk(operations, STATS)

    patch_logger.assert_not_present(
        successful_action_log_message(DOC_ONE_ID, action, result)
    )


@pytest.mark.parametrize(
    "operation_results, expected_logs",
    [
        (
            [successful_bulk_action(DOC_ONE_ID, "create", "created")],
            [successful_action_log_message(DOC_ONE_ID, "create", "created")],
        ),
        (
            [successful_bulk_action(DOC_ONE_ID, "delete", "deleted")],
            [successful_action_log_message(DOC_ONE_ID, "delete", "deleted")],
        ),
        (
            [successful_bulk_action(DOC_ONE_ID, "index", "created")],
            [successful_action_log_message(DOC_ONE_ID, "index", "created")],
        ),
        (
            [successful_bulk_action(DOC_ONE_ID, "update", "updated")],
            [successful_action_log_message(DOC_ONE_ID, "update", "updated")],
        ),
        (
            [successful_bulk_action(DOC_ONE_ID, "delete", "not_found")],
            [
                successful_operation_with_non_successful_result_log_message(
                    DOC_ONE_ID, "delete", "not_found"
                )
            ],
        ),
        (
            [failed_bulk_action(DOC_ONE_ID, "create", "not_found")],
            [failed_action_log_message(DOC_ONE_ID, "create", "not_found")],
        ),
        (
            [failed_bulk_action(DOC_ONE_ID, "delete", "not_found")],
            [failed_action_log_message(DOC_ONE_ID, "delete", "not_found")],
        ),
        (
            [failed_bulk_action(DOC_ONE_ID, "index", "not_found")],
            [failed_action_log_message(DOC_ONE_ID, "index", "not_found")],
        ),
        (
            [failed_bulk_action(DOC_ONE_ID, "update", "not_found")],
            [failed_action_log_message(DOC_ONE_ID, "update", "not_found")],
        ),
        (
            [
                successful_bulk_action(DOC_ONE_ID, "create", "created"),
                successful_bulk_action(DOC_TWO_ID, "update", "updated"),
                failed_bulk_action(DOC_THREE_ID, "update", "not_found"),
            ],
            [
                successful_action_log_message(DOC_ONE_ID, "create", "created"),
                successful_action_log_message(DOC_TWO_ID, "update", "updated"),
                failed_action_log_message(DOC_THREE_ID, "update", "not_found"),
            ],
        ),
    ],
)
@pytest.mark.asyncio
async def test_should_log_bulk_operations_if_doc_id_tracing_is_enabled(
    patch_logger, operation_results, expected_logs
):
    operations = []
    client = Mock()
    client.bulk_insert = AsyncMock(return_value={"items": operation_results})
    sink = Sink(
        client=client,
        queue=Mock(),
        error_monitor=Mock(),
        chunk_size=0,
        pipeline={"name": "pipeline"},
        chunk_mem_size=0,
        max_concurrency=0,
        max_retries=3,
        retry_interval=10,
        enable_bulk_operations_logging=True,
    )

    await sink._batch_bulk(operations, STATS)

    for log in expected_logs:
        patch_logger.assert_present(log)


@pytest.mark.asyncio
async def test_should_log_error_when_id_is_missing(patch_logger):
    operations = []
    client = Mock()
    # item missing id
    item = {"index": {"result": "created"}}

    client.bulk_insert = AsyncMock(
        return_value={
            "items": [item, successful_bulk_action(DOC_ONE_ID, "create", "created")]
        }
    )
    sink = Sink(
        client=client,
        queue=Mock(),
        error_monitor=Mock(),
        chunk_size=0,
        pipeline={"name": "pipeline"},
        chunk_mem_size=0,
        max_concurrency=0,
        max_retries=3,
        retry_interval=10,
        enable_bulk_operations_logging=True,
    )

    await sink._batch_bulk(operations, STATS)

    patch_logger.assert_present(f"Could not retrieve '_id' for document {item}")

    # Should continue logging after a failure
    patch_logger.assert_present(
        successful_action_log_message(DOC_ONE_ID, "create", "created")
    )


@pytest.mark.asyncio
async def test_should_log_error_when_unknown_action_item_returned(patch_logger):
    operations = []
    client = Mock()

    # item with unknown action item
    item = {"unknown": {"result": "created"}}

    client.bulk_insert = AsyncMock(
        return_value={
            "items": [item, successful_bulk_action(DOC_ONE_ID, "create", "created")]
        }
    )
    sink = Sink(
        client=client,
        queue=Mock(),
        error_monitor=Mock(),
        chunk_size=0,
        pipeline={"name": "pipeline"},
        chunk_mem_size=0,
        max_concurrency=0,
        max_retries=3,
        retry_interval=10,
        enable_bulk_operations_logging=True,
    )

    await sink._batch_bulk(operations, STATS)

    patch_logger.assert_present(
        f"Unknown action item returned from _bulk API for item {item}"
    )

    # Should continue logging after a failure
    patch_logger.assert_present(
        successful_action_log_message(DOC_ONE_ID, "create", "created")
    )
