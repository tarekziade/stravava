#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""
`SyncOrchestrator` is orchestrating a sync by:

- creating a queue
- launching a `Extractor`, a wrapper on the top of the documents' generator
- launching a `Sink`, a class that aggregates documents and run the bulk API


                  SyncOrchestrator.async_bulk(generator)
                               |
                               |
Elasticsearch <== Sink <== queue <== Extractor <== generator

"""

import asyncio
import copy
import functools
import logging
import time

from connectors.config import (
    DEFAULT_ELASTICSEARCH_MAX_RETRIES,
    DEFAULT_ELASTICSEARCH_RETRY_INTERVAL,
)
from connectors.es.management_client import ESManagementClient
from connectors.es.settings import TIMESTAMP_FIELD, Mappings
from connectors.filtering.basic_rule import BasicRuleEngine, parse
from connectors.logger import logger, tracer
from connectors.protocol import Filter, JobType
from connectors.protocol.connectors import (
    DELETED_DOCUMENT_COUNT,
    INDEXED_DOCUMENT_COUNT,
    INDEXED_DOCUMENT_VOLUME,
)
from connectors.utils import (
    DEFAULT_CHUNK_MEM_SIZE,
    DEFAULT_CHUNK_SIZE,
    DEFAULT_CONCURRENT_DOWNLOADS,
    DEFAULT_DISPLAY_EVERY,
    DEFAULT_MAX_CONCURRENCY,
    DEFAULT_QUEUE_MEM_SIZE,
    DEFAULT_QUEUE_SIZE,
    ConcurrentTasks,
    Counters,
    ErrorMonitor,
    MemQueue,
    aenumerate,
    get_size,
    iso_utc,
    retryable,
)

__all__ = ["SyncOrchestrator"]
EXTRACTOR_ERROR = "EXTRACTOR_ERROR"
END_DOCS = "END_DOCS"

OP_INDEX = "index"
OP_DELETE = "delete"
OP_CREATE = "create"
OP_UPDATE = "update"
OP_UNKNOWN = "operation_unknown"
CANCELATION_TIMEOUT = 5

# counter keys
BIN_DOCS_DOWNLOADED = "binary_docs_downloaded"
BULK_OPERATIONS = "bulk_operations"
BULK_RESPONSES = "bulk_item_responses"
CREATES_QUEUED = "doc_creates_queued"
UPDATES_QUEUED = "doc_updates_queued"
DELETES_QUEUED = "doc_deletes_queued"
DOCS_EXTRACTED = "docs_extracted"
DOCS_FILTERED = "docs_filtered"
DOCS_DROPPED = "docs_dropped"
ID_MISSING = "_ids_missing"
RESULT_ERROR = "result_errors"
RESULT_SUCCESS = "result_successes"
RESULT_UNDEFINED = "results_undefined"
ID_CHANGED_AFTER_REQUEST = "_ids_changed_after_request"
ID_DUPLICATE = "_id_duplicates"

# Successful results according to the docs: https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-bulk.html#bulk-api-response-body
SUCCESSFUL_RESULTS = ("created", "deleted", "updated")


def get_mib_size(obj):
    """Returns the size of ob in MiB"""
    return round(get_size(obj) / (1024 * 1024), 2)


class UnsupportedJobType(Exception):
    pass


class ForceCanceledError(Exception):
    pass


class ContentIndexDoesNotExistError(Exception):
    pass


class ElasticsearchOverloadedError(Exception):
    def __init__(self, cause=None):
        msg = "Connector was unable to ingest data into overloaded Elasticsearch. Make sure Elasticsearch instance is healthy, has enough resources and content index is healthy."
        super().__init__(msg)
        self.__cause__ = cause


class DocumentIngestionError(Exception):
    pass


class Sink:
    """Send bulk operations in batches by consuming a queue.

    This class runs a coroutine that gets operations out of a `queue` and collects them to
    build and send bulk requests using a `client`

    Arguments:

    - `client` -- an instance of `connectors.es.ESManagementClient`
    - `queue` -- an instance of `asyncio.Queue` to pull docs from
    - `chunk_size` -- a maximum number of operations to send per request
    - `pipeline` -- ingest pipeline settings to pass to the bulk API
    - `chunk_mem_size` -- a maximum size in MiB for each bulk request
    - `max_concurrency` -- a maximum number of concurrent bulk requests
    """

    def __init__(
        self,
        client,
        queue,
        chunk_size,
        pipeline,
        chunk_mem_size,
        max_concurrency,
        max_retries,
        retry_interval,
        error_monitor,
        logger_=None,
        enable_bulk_operations_logging=False,
    ):
        self.client = client
        self.queue = queue
        self.chunk_size = chunk_size
        self.pipeline = pipeline
        self.chunk_mem_size = chunk_mem_size * 1024 * 1024
        self.bulk_tasks = ConcurrentTasks(max_concurrency=max_concurrency)
        self.error_monitor = error_monitor
        self.max_retires = max_retries
        self.retry_interval = retry_interval
        self.error = None
        self._logger = logger_ or logger
        self._canceled = False
        self._enable_bulk_operations_logging = enable_bulk_operations_logging
        self.counters = Counters()

    def _bulk_op(self, doc, operation=OP_INDEX):
        doc_id = doc["_id"]
        index = doc["_index"]

        if operation == OP_INDEX:
            return [{operation: {"_index": index, "_id": doc_id}}, doc["doc"]]
        if operation == OP_UPDATE:
            return [
                {operation: {"_index": index, "_id": doc_id}},
                {"doc": doc["doc"], "doc_as_upsert": True},
            ]
        if operation == OP_DELETE:
            return [{operation: {"_index": index, "_id": doc_id}}]

        raise TypeError(operation)

    @tracer.start_as_current_span("_bulk API call", slow_log=1.0)
    async def _batch_bulk(self, operations, stats):
        # TODO: make this retry policy work with unified retry strategy
        @retryable(retries=self.max_retires, interval=self.retry_interval)
        async def _bulk_api_call():
            return await self.client.client.bulk(
                operations=operations, pipeline=self.pipeline["name"]
            )

        # TODO: treat result to retry errors like in async_streaming_bulk
        task_num = len(self.bulk_tasks)

        if self._logger.isEnabledFor(logging.DEBUG):
            self._logger.debug(
                f"Task {task_num} - Sending a batch of {len(operations)} ops -- {get_mib_size(operations)}MiB"
            )

        # TODO: retry 429s for individual items here
        res = await self.client.bulk_insert(operations, self.pipeline["name"])
        ids_to_ops = self._map_id_to_op(operations)
        await self._process_bulk_response(
            res, ids_to_ops, do_log=self._enable_bulk_operations_logging
        )

        if res.get("errors"):
            for item in res["items"]:
                for op, data in item.items():
                    if "error" in data:
                        self._logger.error(
                            f"operation {op} failed for doc {data['_id']}, {data['error']}"
                        )

        self._populate_stats(stats, res)

        return res

    def _map_id_to_op(self, operations):
        """
        Takes operations like: [{operation: {"_index": index, "_id": doc_id}}, doc["doc"]]
        and turns them into { doc_id : operation }
        """
        result = {}
        for entry in operations:
            if len(entry.keys()) == 1:  # only looking at "operation" entries
                for op, doc in entry.items():
                    if (
                        isinstance(doc, dict)
                        and "_id" in doc.keys()
                        and "_index" in doc.keys()
                    ):  # avoiding update bulk extra entries
                        result[doc["_id"]] = op
        return result

    async def _process_bulk_response(self, res, ids_to_ops, do_log=False):
        for item in res.get("items", []):
            if OP_INDEX in item:
                action_item = OP_INDEX
            elif OP_DELETE in item:
                action_item = OP_DELETE
            elif OP_CREATE in item:
                action_item = OP_CREATE
            elif OP_UPDATE in item:
                action_item = OP_UPDATE
            else:
                # Should only happen, if the _bulk API changes
                # Unlikely, but as this functionality could be used for audits we want to detect changes fast
                if do_log:
                    self._logger.error(
                        f"Unknown action item returned from _bulk API for item {item}"
                    )
                self.counters.increment(OP_UNKNOWN, namespace=BULK_RESPONSES)
                continue

            self.counters.increment(action_item, namespace=BULK_RESPONSES)
            doc_id = item[action_item].get("_id")
            if doc_id is None:
                # Should only happen, if the _bulk API changes
                # Unlikely, but as this functionality could be used for audits we want to detect changes fast
                if do_log:
                    self._logger.error(f"Could not retrieve '_id' for document {item}")
                self.counters.increment(ID_MISSING, namespace=BULK_RESPONSES)
                continue

            result = item[action_item].get("result")

            requested_op = ids_to_ops.get(doc_id, None)
            if requested_op is None:
                # This ID wasn't in the request we sent, meaning that the ID was changed (probably via pipeline).
                self.counters.increment(
                    ID_CHANGED_AFTER_REQUEST, namespace=BULK_RESPONSES
                )
            elif action_item != OP_UPDATE and result == "updated":
                # This means we sent an `index` op, but didn't create a new doc. This could mean that there was a
                # doc with this ID in the index before this sync, OR that this ID showed up more than once during
                # this sync.
                self.counters.increment(ID_DUPLICATE, namespace=BULK_RESPONSES)

            if result == "noop":
                # This means that whatever the requested op was, nothing happened. This is most likely to mean
                # that the document was dropped during the ingest pipeline
                self.counters.increment(DOCS_DROPPED, namespace=BULK_RESPONSES)

            successful_result = result in SUCCESSFUL_RESULTS
            if not successful_result:
                if "error" in item[action_item]:
                    message = f"Failed to execute '{action_item}' on document with id '{doc_id}'. Error: {item[action_item].get('error')}"
                    self.error_monitor.track_error(DocumentIngestionError(message))
                    if do_log:
                        self._logger.debug(message)
                    self.counters.increment(RESULT_ERROR, namespace=BULK_RESPONSES)
                else:
                    message = f"Executed '{action_item}' on document with id '{doc_id}', but got non-successful result: {result}"
                    self.error_monitor.track_error(DocumentIngestionError(message))
                    if do_log:
                        self._logger.debug(message)
                    self.counters.increment(RESULT_UNDEFINED, namespace=BULK_RESPONSES)
            else:
                self.error_monitor.track_success()
                if do_log:
                    self._logger.debug(
                        f"Successfully executed '{action_item}' on document with id '{doc_id}'. Result: {result}"
                    )
                self.counters.increment(RESULT_SUCCESS)

    def _populate_stats(self, stats, res):
        for item in res["items"]:
            for op, data in item.items():
                # "result" is only present in successful operations
                if "result" not in data:
                    del stats[op][data["_id"]]

        self.counters.increment(
            INDEXED_DOCUMENT_COUNT, len(stats[OP_INDEX]) + len(stats[OP_UPDATE])
        )
        self.counters.increment(
            INDEXED_DOCUMENT_VOLUME,
            sum(stats[OP_INDEX].values()) + sum(stats[OP_UPDATE].values()),
        )
        self.counters.increment(DELETED_DOCUMENT_COUNT, len(stats[OP_DELETE]))

        self._logger.debug(
            f"Sink stats - no. of docs indexed: {self.counters.get(INDEXED_DOCUMENT_COUNT)}, volume of docs indexed: {round(self.counters.get(INDEXED_DOCUMENT_VOLUME))} bytes, no. of docs deleted: {self.counters.get(DELETED_DOCUMENT_COUNT)}"
        )

    def force_cancel(self):
        self._canceled = True

    async def fetch_doc(self):
        if self._canceled:
            raise ForceCanceledError

        return await self.queue.get()

    async def run(self):
        try:
            await self._run()
        except asyncio.CancelledError:
            self._logger.info("Task is canceled, stop Sink...")
            raise
        except asyncio.QueueFull as e:
            raise ElasticsearchOverloadedError from e
        except Exception as e:
            if isinstance(e, ForceCanceledError) or self._canceled:
                self._logger.warning(
                    f"Sink did not stop within {CANCELATION_TIMEOUT} seconds of cancelation, force-canceling the task."
                )
                return
            raise

    async def _run(self):
        """Creates batches of bulk calls given a queue of items.

        An item is a (size, object) tuple. Exits when the
        item is the `END_DOCS` or `EXTRACTOR_ERROR` string.

        Bulk calls are executed concurrently with a maximum number of concurrent
        requests.
        """
        try:
            batch = []
            # stats is a dictionary containing stats for 3 operations. In each sub-dictionary, it is a doc id to size map.
            stats = {OP_INDEX: {}, OP_UPDATE: {}, OP_DELETE: {}}
            bulk_size = 0
            overhead_size = None
            batch_num = 0

            while True:
                batch_num += 1
                doc_size, doc = await self.fetch_doc()
                if doc in (END_DOCS, EXTRACTOR_ERROR):
                    break
                operation = doc["_op_type"]
                doc_id = doc["_id"]
                if not doc_id:
                    self._logger.warning(f"Skip document {doc} as '_id' is missing.")
                    continue
                if operation == OP_DELETE:
                    stats[operation][doc_id] = 0
                else:
                    # the doc_size also includes _op_type, _index and _id,
                    # which we want to exclude when calculating the size.
                    if overhead_size is None:
                        overhead = {
                            "_op_type": operation,
                            "_index": doc["_index"],
                            "_id": doc_id,
                        }
                        overhead_size = get_size(overhead)
                    stats[operation][doc_id] = max(doc_size - overhead_size, 0)
                self.counters.increment(operation, namespace=BULK_OPERATIONS)
                batch.extend(self._bulk_op(doc, operation))

                bulk_size += doc_size
                if len(batch) >= self.chunk_size or bulk_size > self.chunk_mem_size:
                    await self.bulk_tasks.put(
                        functools.partial(
                            self._batch_bulk,
                            copy.copy(batch),
                            copy.copy(stats),
                        ),
                        name=f"Elasticsearch Sink: _bulk batch #{batch_num}",
                    )
                    batch.clear()
                    stats = {OP_INDEX: {}, OP_UPDATE: {}, OP_DELETE: {}}
                    bulk_size = 0

                await asyncio.sleep(0)
                self.bulk_tasks.raise_any_exception()

            await self.bulk_tasks.join(raise_on_error=True)
            if len(batch) > 0:
                await self._batch_bulk(batch, stats)
        except Exception as e:
            self.error = e
            raise


class Extractor:
    """Grabs data and adds them in the queue for the bulker.

    This class runs a coroutine that puts docs in `queue`, given a document generator.

    Arguments:
    - client: an instance of `connectors.es.ESManagementClient`
    - queue: an `asyncio.Queue` to put docs in
    - index: the target Elasticsearch index
    - filter_: an instance of `Filter` to apply on the fetched document -- default: `None`
    - sync_rules_enabled: if `True`, we apply rules -- default: `False`
    - content_extraction_enabled: if `True`, download content -- default `True`
    - display_every -- display a log every `display_every` doc -- default: `DEFAULT_DISPLAY_EVERY`
    - concurrent_downloads: -- concurrency level for downloads -- default: `DEFAULT_CONCURRENT_DOWNLOADS`
    """

    def __init__(
        self,
        client,
        queue,
        index,
        filter_=None,
        sync_rules_enabled=False,
        content_extraction_enabled=True,
        display_every=DEFAULT_DISPLAY_EVERY,
        concurrent_downloads=DEFAULT_CONCURRENT_DOWNLOADS,
        logger_=None,
        skip_unchanged_documents=False,
    ):
        if filter_ is None:
            filter_ = Filter()
        self.client = client
        self.queue = queue
        self.index = index
        self.loop = asyncio.get_event_loop()
        self.counters = Counters()
        self.error = None
        self.filter_ = filter_
        self.basic_rule_engine = (
            BasicRuleEngine(parse(filter_.basic_rules)) if sync_rules_enabled else None
        )
        self.content_extraction_enabled = content_extraction_enabled
        self.display_every = display_every
        self.concurrent_downloads = concurrent_downloads
        self._logger = logger_ or logger
        self._canceled = False
        self.skip_unchanged_documents = skip_unchanged_documents

    async def _deferred_index(self, lazy_download, doc_id, doc, operation):
        try:
            data = await lazy_download(doit=True, timestamp=doc[TIMESTAMP_FIELD])

            if data is not None:
                self.counters.increment(BIN_DOCS_DOWNLOADED)
                data.pop("_id", None)
                data.pop(TIMESTAMP_FIELD, None)
                doc.update(data)

            doc.pop("_original_filename", None)

            await self.put_doc(
                {
                    "_op_type": operation,
                    "_index": self.index,
                    "_id": doc_id,
                    "doc": doc,
                }
            )
        except ForceCanceledError:
            raise
        except Exception as ex:
            self._logger.error(
                f"Failed to do deferred index operation for doc {doc_id}: {ex}"
            )

    def force_cancel(self):
        self._canceled = True

    async def put_doc(self, doc):
        if self._canceled:
            raise ForceCanceledError

        await self.queue.put(doc)

    async def run(self, generator, job_type):
        try:
            match job_type:
                case JobType.FULL:
                    await self.get_docs(generator)
                case JobType.INCREMENTAL:
                    if self.skip_unchanged_documents:
                        await self.get_docs(generator, skip_unchanged_documents=True)
                    else:
                        await self.get_docs_incrementally(generator)
                case JobType.ACCESS_CONTROL:
                    await self.get_access_control_docs(generator)
                case _:
                    raise UnsupportedJobType
        except asyncio.CancelledError:
            self._logger.info("Task is canceled, stop Extractor...")
            raise
        except asyncio.QueueFull as e:
            self._logger.error("Sync was throttled by Elasticsearch")
            # We clear the queue as we could not actually ingest anything.
            # After that we indicate that we've encountered an error
            self.queue.clear()
            await self.put_doc(EXTRACTOR_ERROR)
            self.error = ElasticsearchOverloadedError(e)
        except Exception as e:
            if isinstance(e, ForceCanceledError) or self._canceled:
                self._logger.warning(
                    f"Extractor did not stop within {CANCELATION_TIMEOUT} seconds of cancelation, force-canceling the task."
                )
                return

            self._logger.error("Document extractor failed", exc_info=True)
            await self.put_doc(EXTRACTOR_ERROR)
            self.error = e

    @tracer.start_as_current_span("get_doc call", slow_log=1.0)
    async def _decorate_with_metrics_span(self, generator):
        """Wrapper for metrics"""
        async for doc in generator:
            yield doc

    async def get_docs(self, generator, skip_unchanged_documents=False):
        """Iterate on a generator of documents to fill a queue of bulk operations for the `Sink` to consume.
        Extraction happens in a separate task, when a document contains files.

        Args:
           generator (generator): BaseDataSource child get_docs or get_docs_incrementally
           skip_unchanged_documents (bool): if True, will skip documents that have not changed since last sync
        """
        generator = self._decorate_with_metrics_span(generator)
        existing_ids = await self._load_existing_docs()

        self._logger.info("Iterating on remote documents")
        lazy_downloads = ConcurrentTasks(self.concurrent_downloads)
        download_num = 0
        try:
            async for count, doc in aenumerate(generator):
                self.counters.increment(DOCS_EXTRACTED)
                doc, lazy_download, operation = doc
                if count % self.display_every == 0:
                    self._log_progress()

                doc_id = doc["id"] = doc.pop("_id")

                if self.basic_rule_engine and not self.basic_rule_engine.should_ingest(
                    doc
                ):
                    self.counters.increment((DOCS_FILTERED))
                    continue

                if doc_id in existing_ids:
                    # pop out of existing_ids, so they do not get deleted
                    ts = existing_ids.pop(doc_id)

                    if (
                        skip_unchanged_documents
                        and TIMESTAMP_FIELD in doc
                        and ts == doc[TIMESTAMP_FIELD]
                    ):
                        # cancel the download
                        if (
                            self.content_extraction_enabled
                            and lazy_download is not None
                        ):
                            await lazy_download(doit=False)

                        self._logger.debug(
                            f"Skipping document with id '{doc_id}' because field '{TIMESTAMP_FIELD}' has not changed since last sync"
                        )
                        continue

                    self.counters.increment(UPDATES_QUEUED)

                else:
                    self.counters.increment(CREATES_QUEUED)
                    if TIMESTAMP_FIELD not in doc:
                        doc[TIMESTAMP_FIELD] = iso_utc()

                # if we need to call lazy_download we push it in lazy_downloads
                if self.content_extraction_enabled and lazy_download is not None:
                    download_num += 1
                    await lazy_downloads.put(
                        functools.partial(
                            self._deferred_index, lazy_download, doc_id, doc, operation
                        ),
                        name=f"Extractor download #{download_num}",
                    )

                else:
                    # we can push into the queue right away
                    await self.put_doc(
                        {
                            "_op_type": operation,
                            "_index": self.index,
                            "_id": doc_id,
                            "doc": doc,
                        }
                    )
                # We try raising every loop to not miss a moment when
                # too many errors happened when downloading
                lazy_downloads.raise_any_exception()

                # We try raising every loop to not miss a moment when
                # too many errors happened when downloading
                lazy_downloads.raise_any_exception()

                await asyncio.sleep(0)

            # Sit and wait until an error happens
            await lazy_downloads.join(raise_on_error=True)
        except Exception as ex:
            self._logger.error(f"Extractor failed with an error: {ex}")
            lazy_downloads.cancel()
            raise
        finally:
            # wait for all downloads to be finished
            # even if we errored out
            await lazy_downloads.join()

        await self.enqueue_docs_to_delete(existing_ids)
        await self.put_doc(END_DOCS)

    async def _load_existing_docs(self):
        start = time.time()
        self._logger.info("Collecting local document ids")

        existing_ids = {
            k: v
            async for (k, v) in self.client.yield_existing_documents_metadata(
                self.index
            )
        }

        self._logger.debug(
            f"Found {len(existing_ids)} docs in {self.index} (duration "
            f"{int(time.time() - start)} seconds) "
        )

        if self._logger.isEnabledFor(logging.DEBUG):
            self._logger.debug(
                f"Size of ids in memory is {get_mib_size(existing_ids)}MiB"
            )

        return existing_ids

    async def get_docs_incrementally(self, generator):
        """Iterate on a generator of documents to fill a queue with bulk operations for the `Sink` to consume.

        A document might be discarded if its timestamp has not changed.
        Extraction happens in a separate task, when a document contains files.
        """
        generator = self._decorate_with_metrics_span(generator)

        self._logger.info("Iterating on remote documents incrementally when possible")
        lazy_downloads = ConcurrentTasks(self.concurrent_downloads)
        num_downloads = 0
        try:
            async for count, doc in aenumerate(generator):
                doc, lazy_download, operation = doc
                if count % self.display_every == 0:
                    self._log_progress()

                doc_id = doc["id"] = doc.pop("_id")

                if self.basic_rule_engine and not self.basic_rule_engine.should_ingest(
                    doc
                ):
                    continue

                if operation == OP_INDEX:
                    self.counters.increment(CREATES_QUEUED)
                elif operation == OP_UPDATE:
                    self.counters.increment(UPDATES_QUEUED)
                elif operation == OP_DELETE:
                    self.counters.increment(DELETES_QUEUED)
                else:
                    self._logger.error(
                        f"unsupported operation {operation} for doc {doc_id}"
                    )

                if TIMESTAMP_FIELD not in doc:
                    doc[TIMESTAMP_FIELD] = iso_utc()

                # if we need to call lazy_download we push it in lazy_downloads
                if self.content_extraction_enabled and lazy_download is not None:
                    num_downloads += 1
                    await lazy_downloads.put(
                        functools.partial(
                            self._deferred_index, lazy_download, doc_id, doc, operation
                        ),
                        name=f"Extractor download #{num_downloads}",
                    )

                else:
                    # we can push into the queue right away
                    item = {
                        "_op_type": operation,
                        "_index": self.index,
                        "_id": doc_id,
                    }
                    if operation in (OP_INDEX, OP_UPDATE):
                        item["doc"] = doc
                    await self.put_doc(item)

                await asyncio.sleep(0)
        finally:
            # wait for all downloads to be finished
            await lazy_downloads.join()

        await self.put_doc(END_DOCS)

    async def get_access_control_docs(self, generator):
        """Iterate on a generator of access control documents to fill a queue with bulk operations for the `Sink` to consume.

        A document might be discarded if its timestamp has not changed.
        """
        self._logger.info("Starting access control doc lookups")
        generator = self._decorate_with_metrics_span(generator)

        existing_ids = {
            doc_id: last_update_timestamp
            async for (
                doc_id,
                last_update_timestamp,
            ) in self.client.yield_existing_documents_metadata(self.index)
        }

        if self._logger.isEnabledFor(logging.DEBUG):
            self._logger.debug(
                f"Size of {len(existing_ids)} access control document ids  in memory is {get_mib_size(existing_ids)}MiB"
            )

        count = 0
        async for doc in generator:
            doc, _, _ = doc
            count += 1
            if count % self.display_every == 0:
                self._log_progress()

            doc_id = doc["id"] = doc.pop("_id")
            doc_exists = doc_id in existing_ids

            if doc_exists:
                last_update_timestamp = existing_ids.pop(doc_id)
                doc_not_updated = (
                    TIMESTAMP_FIELD in doc
                    and last_update_timestamp == doc[TIMESTAMP_FIELD]
                )

                if doc_not_updated:
                    continue

                self.counters.increment(UPDATES_QUEUED)

                operation = OP_UPDATE
            else:
                self.counters.increment(CREATES_QUEUED)

                if TIMESTAMP_FIELD not in doc:
                    doc[TIMESTAMP_FIELD] = iso_utc()

                operation = OP_INDEX

            await self.put_doc(
                {
                    "_op_type": operation,
                    "_index": self.index,
                    "_id": doc_id,
                    "doc": doc,
                }
            )
            await asyncio.sleep(0)

        await self.enqueue_docs_to_delete(existing_ids)
        await self.put_doc(END_DOCS)

    async def enqueue_docs_to_delete(self, existing_ids):
        self._logger.debug(f"Delete {len(existing_ids)} docs from index '{self.index}'")
        for doc_id in existing_ids.keys():
            await self.put_doc(
                {
                    "_op_type": OP_DELETE,
                    "_index": self.index,
                    "_id": doc_id,
                }
            )
            self.counters.increment(DELETES_QUEUED)

    def _log_progress(
        self,
    ):
        self._logger.info(
            "Sync progress -- "
            f"created: {self.counters.get(CREATES_QUEUED)} | "
            f"updated: {self.counters.get(UPDATES_QUEUED)} | "
            f"deleted: {self.counters.get(DELETES_QUEUED)}"
        )


class ContentIndexNameInvalid(Exception):
    pass


class AsyncBulkRunningError(Exception):
    pass


class SyncOrchestrator:
    """This class is the sync orchestrator.

    It does the following in `async_bulk`

    - grabs all ids on Elasticsearch for the index
    - creates a MemQueue to hold documents to stream
    - runs a `Extractor` (producer) and a `Sink` (consumer) against the queue
    - once they are both over, returns totals
    """

    def __init__(self, elastic_config, logger_=None):
        self._logger = logger_ or logger
        self._logger.debug(f"SyncOrchestrator connecting to {elastic_config['host']}")
        self.es_management_client = ESManagementClient(elastic_config)
        self.loop = asyncio.get_event_loop()
        self._extractor = None
        self._extractor_task = None
        self._sink = None
        self._sink_task = None
        self.error = None
        self.canceled = False
        error_monitor_config = elastic_config.get("bulk", {}).get("error_monitor", {})
        self.error_monitor = ErrorMonitor(error_monitor_config)

    async def close(self):
        await self.es_management_client.close()
        await self.cancel()

    async def has_active_license_enabled(self, license_):
        # TODO: think how to make it not a proxy method to the client
        return await self.es_management_client.has_active_license_enabled(license_)

    def extract_index_or_alias(self, get_index_response, expected_index_name):
        return None

    async def prepare_content_index(self, index_name, language_code=None):
        """Creates the index, given a mapping/settings if it does not exist."""
        self._logger.debug(f"Checking index {index_name}")

        index = await self.es_management_client.get_index_or_alias(
            index_name, ignore_unavailable=True
        )

        mappings = Mappings.default_text_fields_mappings(is_connectors_index=True)

        if index:
            # Update the index mappings if needed
            self._logger.debug(f"{index_name} exists")

            # Settings contain analyzers which are being used in the index mappings
            # Therefore settings must be applied before mappings
            await self.es_management_client.ensure_content_index_settings(
                index_name=index_name, index=index, language_code=language_code
            )

            await self.es_management_client.ensure_content_index_mappings(
                index_name=index_name, index=index, desired_mappings=mappings
            )
        else:
            # Create a new index
            self._logger.info(f"Creating content index: {index_name}")
            await self.es_management_client.create_content_index(
                index_name, language_code
            )
            self._logger.info(f"Content index successfully created:  {index_name}")

    def done(self):
        """
        An async task (which this mimics) should be "done" if:
         - it was canceled
         - it errored
         - it completed successfully
        :return: True if the orchestrator is "done", else False
        """
        if self.get_error() is not None:
            return True

        extractor_done = (
            True
            if self._extractor_task is None or self._extractor_task.done()
            else False
        )
        sink_done = True if self._sink_task is None or self._sink_task.done() else False
        return extractor_done and sink_done

    def _sink_task_running(self):
        return self._sink_task is not None and not self._sink_task.done()

    def _extractor_task_running(self):
        return self._extractor_task is not None and not self._extractor_task.done()

    async def cancel(self):
        if self._sink_task_running():
            self._logger.info(
                f"Canceling the Sink task: {self._sink_task.get_name()}"  # pyright: ignore
            )
            self._sink_task.cancel()
        else:
            self._logger.debug(
                "Orchestrator cancel() called, but sink task is not running"
            )

        if self._extractor_task_running():
            self._logger.info(
                f"Canceling the Extractor task: {self._extractor_task.get_name()}"
            )
            self._extractor_task.cancel()
        else:
            self._logger.debug(
                "Orchestrator cancel() called, but extractor task is not running"
            )

        self.canceled = True

        cancelation_timeout = CANCELATION_TIMEOUT
        self._logger.debug(
            f"Allowing {cancelation_timeout} seconds for the orchestrator tasks to finish..."
        )
        while cancelation_timeout > 0:
            await asyncio.sleep(1)
            cancelation_timeout -= 1
            if not self._sink_task_running() and not self._extractor_task_running():
                self._logger.info(
                    "Both Extractor and Sink tasks are successfully stopped."
                )
                return
            self._logger.debug(f"Sink task running? {self._sink_task_running()}")
            self._logger.debug(
                f"Extractor task running? {self._extractor_task_running()}"
            )

        self._logger.error(
            f"Sync job did not stop within {CANCELATION_TIMEOUT} seconds of canceling. Force-canceling."
        )
        self._sink.force_cancel()
        self._extractor.force_cancel()

    def ingestion_stats(self):
        stats = {}
        if self._extractor is not None:
            stats.update(self._extractor.counters.to_dict())
        if self._sink is not None:
            stats.update(self._sink.counters.to_dict())
            stats[INDEXED_DOCUMENT_VOLUME] = round(
                self._sink.counters.get(INDEXED_DOCUMENT_VOLUME) / (1024 * 1024)
            )  # return indexed_document_volume in number of MiB
        return stats

    def get_error(self):
        return (
            None
            if self._extractor is None
            else (self._extractor.error or self._sink.error or self.error)
        )

    async def async_bulk(
        self,
        index,
        generator,
        pipeline,
        job_type,
        filter_=None,
        sync_rules_enabled=False,
        content_extraction_enabled=True,
        options=None,
        skip_unchanged_documents=False,
        enable_bulk_operations_logging=False,
    ):
        """Performs a batch of `_bulk` calls, given a generator of documents

        Arguments:
        - index: target index
        - generator: documents generator
        - pipeline: ingest pipeline settings to pass to the bulk API
        - job_type: the job type of the sync job
        - filter_: an instance of `Filter` to apply on the fetched document  -- default: `None`
        - sync_rules_enabled: if enabled, applies rules -- default: `False`
        - content_extraction_enabled: if enabled, will download content -- default: `True`
        - options: dict of options (from `elasticsearch.bulk` in the config file)
        """
        if self._extractor_task is not None or self._sink_task is not None:
            msg = "Async bulk task has already started."
            raise AsyncBulkRunningError(msg)
        if filter_ is None:
            filter_ = Filter()
        if options is None:
            options = {}
        queue_size = options.get("queue_max_size", DEFAULT_QUEUE_SIZE)
        display_every = options.get("display_every", DEFAULT_DISPLAY_EVERY)
        queue_mem_size = options.get("queue_max_mem_size", DEFAULT_QUEUE_MEM_SIZE)
        chunk_mem_size = options.get("chunk_max_mem_size", DEFAULT_CHUNK_MEM_SIZE)
        max_concurrency = options.get("max_concurrency", DEFAULT_MAX_CONCURRENCY)
        chunk_size = options.get("chunk_size", DEFAULT_CHUNK_SIZE)
        concurrent_downloads = options.get(
            "concurrent_downloads", DEFAULT_CONCURRENT_DOWNLOADS
        )
        max_bulk_retries = options.get("max_retries", DEFAULT_ELASTICSEARCH_MAX_RETRIES)
        retry_interval = options.get(
            "retry_interval", DEFAULT_ELASTICSEARCH_RETRY_INTERVAL
        )
        mem_queue_refresh_timeout = options.get("queue_refresh_timeout", 60)
        mem_queue_refresh_interval = options.get("queue_refresh_interval", 1)

        stream = MemQueue(
            maxsize=queue_size,
            maxmemsize=queue_mem_size * 1024 * 1024,
            refresh_timeout=mem_queue_refresh_timeout,
            refresh_interval=mem_queue_refresh_interval,
        )

        # start the fetcher
        self._extractor = Extractor(
            self.es_management_client,
            stream,
            index,
            filter_=filter_,
            sync_rules_enabled=sync_rules_enabled,
            content_extraction_enabled=content_extraction_enabled,
            display_every=display_every,
            concurrent_downloads=concurrent_downloads,
            logger_=self._logger,
            skip_unchanged_documents=skip_unchanged_documents,
        )
        self._extractor_task = asyncio.create_task(
            self._extractor.run(generator, job_type),
            name=f"Extractor for {job_type} sync to {index}",
        )
        self._extractor_task.add_done_callback(
            functools.partial(self.extractor_task_callback)
        )

        # start the bulker
        self._sink = Sink(
            self.es_management_client,
            stream,
            chunk_size,
            pipeline,
            chunk_mem_size=chunk_mem_size,
            max_concurrency=max_concurrency,
            max_retries=max_bulk_retries,
            retry_interval=retry_interval,
            error_monitor=self.error_monitor,
            logger_=self._logger,
            enable_bulk_operations_logging=enable_bulk_operations_logging,
        )
        self._sink_task = asyncio.create_task(
            self._sink.run(), name=f"Sink for {job_type} sync to {index}"
        )
        self._sink_task.add_done_callback(functools.partial(self.sink_task_callback))

    def sink_task_callback(self, task):
        if task.cancelled():
            self._logger.warning(
                f"{type(self._sink).__name__}: {task.get_name()} was cancelled before completion"
            )
        elif task.exception():
            self._logger.error(
                f"Encountered an error in the sync's {type(self._sink).__name__}: {task.get_name()}",
                exc_info=task.exception(),
            )
            self.error = task.exception()

    def extractor_task_callback(self, task):
        if task.cancelled():
            self._logger.warning(
                f"{type(self._extractor).__name__}: {task.get_name()} was cancelled before completion"
            )
        elif task.exception():
            self._logger.error(
                f"Encountered an error in the sync's {type(self._extractor).__name__}: {task.get_name()}",
                exc_info=task.exception(),
            )
            self.error = task.exception()
