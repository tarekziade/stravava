#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import asyncio
import time

import elasticsearch
from elasticsearch import (
    AuthorizationException as ElasticAuthorizationException,
)
from elasticsearch import NotFoundError as ElasticNotFoundError

from connectors.config import DataSourceFrameworkConfig
from connectors.es.client import License, with_concurrency_control
from connectors.es.index import DocumentNotFoundError
from connectors.es.license import requires_platinum_license
from connectors.es.management_client import ESManagementClient
from connectors.es.sink import (
    CREATES_QUEUED,
    DELETES_QUEUED,
    OP_INDEX,
    UPDATES_QUEUED,
    SyncOrchestrator,
    UnsupportedJobType,
)
from connectors.logger import logger
from connectors.protocol import JobStatus, JobType
from connectors.protocol.connectors import (
    DELETED_DOCUMENT_COUNT,
    INDEXED_DOCUMENT_COUNT,
    INDEXED_DOCUMENT_VOLUME,
)
from connectors.source import BaseDataSource
from connectors.utils import ErrorMonitor, truncate_id

UTF_8 = "utf-8"

JOB_REPORTING_INTERVAL = 10
JOB_CHECK_INTERVAL = 1
ES_ID_SIZE_LIMIT = 512


class SyncJobRunningError(Exception):
    pass


class InsufficientESLicenseError(Exception):
    def __init__(self, required_license, actual_license):
        super().__init__(
            f"Minimum required Elasticsearch license: '{required_license.value}'. Actual license: '{actual_license.value}'."
        )


class SyncJobStartError(Exception):
    pass


class ConnectorNotFoundError(Exception):
    def __init__(self, connector_id):
        super().__init__(f"Connector is not found for connector ID {connector_id}.")


class ConnectorJobNotFoundError(Exception):
    def __init__(self, job_id):
        super().__init__(f"Connector job is not found for job ID {job_id}.")


class ConnectorJobCanceledError(Exception):
    pass


class ConnectorJobNotRunningError(Exception):
    def __init__(self, job_id, status):
        super().__init__(
            f"Connector job (ID: {job_id}) is not running but in status of {status}."
        )


class ApiKeyNotFoundError(Exception):
    pass


class SyncJobRunner:
    """The class to run a sync job.

    It takes a sync job, and tries to claim it, and executes it. It also makes sure the sync job is updated
    appropriately when it errors out, is canceled, completes successfully or the service shuts down.

    Arguments:
        - `source_klass`: The source class of the connector
        - `sync_job`: The sync job to run
        - `connector`: The connector of the sync job
        - `es_config`: The elasticsearch configuration to build connection to Elasticsearch server

    """

    def __init__(
        self,
        source_klass,
        sync_job,
        connector,
        es_config,
        service_config,
    ):
        self.source_klass = source_klass
        self.data_provider = None
        self.sync_job = sync_job
        self.connector = connector
        self.es_config = es_config
        self.service_config = service_config
        self.sync_orchestrator = None
        error_monitor_config = service_config.get("extraction", {}).get(
            "error_monitor", {}
        )
        self.error_monitor = ErrorMonitor(**error_monitor_config)
        self.job_reporting_task = None
        self.bulk_options = self.es_config.get("bulk", {})
        self._start_time = None
        self.running = False
        self._enable_bulk_operations_logging = self.bulk_options.get(
            "enable_operations_logging"
        )

    async def execute(self):
        if self.running:
            msg = f"Sync job {self.sync_job.id} is already running."
            raise SyncJobRunningError(msg)

        self.running = True

        job_type = self.sync_job.job_type

        self.sync_job.log_debug(f"Starting execution of {job_type} sync job.")

        await self.sync_starts()  # This MUST be the last line before the `try` begins

        try:
            sync_cursor = (
                self.connector.sync_cursor
                if self.sync_job.job_type == JobType.INCREMENTAL
                else None
            )
            self._start_time = time.time()
            await self.sync_job.claim(sync_cursor=sync_cursor)

            self.sync_job.log_debug("Successfully claimed the sync job.")

            self.data_provider = self.source_klass(
                configuration=self.sync_job.configuration
            )
            self.data_provider.set_logger(self.sync_job.logger)
            self.data_provider.set_error_monitor(self.error_monitor)
            self.data_provider.set_framework_config(
                self._data_source_framework_config()
            )

            self.sync_job.log_debug("Instantiated data provider for the sync job.")

            if not await self.data_provider.changed():
                self.sync_job.log_info("No change in remote source, skipping sync")
                await self._sync_done(sync_status=JobStatus.COMPLETED)
                return

            self.data_provider.set_features(self.connector.features)

            self.sync_job.log_debug("Validating configuration")
            self.data_provider.validate_config_fields()
            await self.data_provider.validate_config()

            self.sync_job.log_debug("Pinging the backend")
            await self.data_provider.ping()

            job_type = self.sync_job.job_type

            # allows the data provider to change the bulk options
            bulk_options = self.bulk_options.copy()
            self.data_provider.tweak_bulk_options(bulk_options)

            if (
                self.connector.native
                and self.connector.features.native_connector_api_keys_enabled()
                and self.service_config.get("_use_native_connector_api_keys", True)
            ):
                # Update the config so native connectors can use API key authentication during sync
                await self._update_native_connector_authentication()

            self.sync_orchestrator = SyncOrchestrator(
                self.es_config, self.sync_job.logger
            )

            if job_type in [JobType.INCREMENTAL, JobType.FULL]:
                self.sync_job.log_info(f"Executing {job_type.value} sync")
                await self._execute_content_sync_job(job_type, bulk_options)
            elif job_type == JobType.ACCESS_CONTROL:
                self.sync_job.log_info("Executing access control sync")
                await self._execute_access_control_sync_job(job_type, bulk_options)
            else:
                raise UnsupportedJobType

            self.job_reporting_task = asyncio.create_task(
                self.update_ingestion_stats(JOB_REPORTING_INTERVAL)
            )

            while not self.sync_orchestrator.done():
                await self.check_job()
                await asyncio.sleep(JOB_CHECK_INTERVAL)
            sync_error = self.sync_orchestrator.get_error()
            sync_status = JobStatus.COMPLETED if sync_error is None else JobStatus.ERROR
            await self._sync_done(sync_status=sync_status, sync_error=sync_error)
        except asyncio.CancelledError:
            self.sync_job.log_warning("Caught signal to suspend the job.")
            self.sync_job.log_debug("with stack:", exc_info=True)
            await self._sync_done(sync_status=JobStatus.SUSPENDED)
        except ConnectorJobCanceledError:
            self.sync_job.log_warning("Caught signal to cancel the job.")
            self.sync_job.log_debug("with stack:", exc_info=True)
            await self._sync_done(sync_status=JobStatus.CANCELED)
        except ElasticAuthorizationException as e:
            error_msg = f"Connector is not authorized to access index [{self.sync_job.index_name}]. API key may need to be regenerated. Status code: [{e.status_code}]."
            self.sync_job.log_error(error_msg, exc_info=True)
            await self._sync_done(sync_status=JobStatus.ERROR, sync_error=error_msg)
        except Exception as e:
            self.sync_job.log_error(e, exc_info=True)
            await self._sync_done(sync_status=JobStatus.ERROR, sync_error=e)
        finally:
            self.running = False
            self.sync_job.log_info("Job terminated. Cleaning up.")
            if self.sync_orchestrator is not None:
                await self.sync_orchestrator.close()
            if self.data_provider is not None:
                await self.data_provider.close()

    async def _update_native_connector_authentication(self):
        """
        The connector secrets API endpoint can only be accessed by the Enterprise Search system role,
        so we need to use a client initialised with the config's username and password to first fetch
        the API key for native connectors.
        After that, we can provide the API key to the sync orchestrator to initialise a new client
        so that an API key can be used for the sync.
        This function should not be run for connector clients.
        """
        es_management_client = ESManagementClient(self.es_config)
        try:
            self.sync_job.log_debug(
                f"Checking secrets storage for API key for index [{self.connector.index_name}]..."
            )
            api_key = await es_management_client.get_connector_secret(
                self.connector.api_key_secret_id
            )
            self.sync_job.log_debug(
                f"API key found in secrets storage for index [{self.connector.index_name}], will use this for authorization."
            )
            self.es_config["api_key"] = api_key
        except ElasticNotFoundError as e:
            msg = f"API key not found in secrets storage for index [{self.connector.index_name}]."
            raise ApiKeyNotFoundError(msg) from e
        finally:
            await es_management_client.close()

    def _data_source_framework_config(self):
        builder = DataSourceFrameworkConfig.Builder().with_max_file_size(
            self.service_config.get("max_file_download_size")
        )
        return builder.build()

    async def _execute_access_control_sync_job(self, job_type, bulk_options):
        if requires_platinum_license(self.sync_job, self.connector, self.source_klass):
            (
                is_platinum_license_enabled,
                license_enabled,
            ) = await self.sync_orchestrator.has_active_license_enabled(
                License.PLATINUM
            )

            if not is_platinum_license_enabled:
                raise InsufficientESLicenseError(
                    required_license=License.PLATINUM, actual_license=license_enabled
                )

        await self.sync_orchestrator.async_bulk(
            self.sync_job.index_name,
            self.generator(),
            self.sync_job.pipeline,
            job_type,
            options=bulk_options,
            enable_bulk_operations_logging=self._enable_bulk_operations_logging,
        )

    def _skip_unchanged_documents_enabled(self, job_type, data_provider):
        """
        Check if timestamp optimization is enabled for the current data source.
        Timestamp optimization can be enabled only for incremental jobs.
        """
        # The job type is not incremental, so we can't use timestamp optimization
        if job_type != JobType.INCREMENTAL:
            return False

        # Data provider has to have the method get_docs_incrementally (inherent from BaseDataSource or implemented in a subclass)
        if not hasattr(data_provider, "get_docs_incrementally"):
            return False

        # make sure that the data provider is not overriding the default implementation of get_docs_incrementally
        return (
            data_provider.get_docs_incrementally.__func__
            is BaseDataSource.get_docs_incrementally
        )

    async def _execute_content_sync_job(self, job_type, bulk_options):
        if (
            self.sync_job.job_type == JobType.INCREMENTAL
            and not self.connector.features.incremental_sync_enabled()
        ):
            msg = f"Connector {self.connector.id} does not support incremental sync."
            raise UnsupportedJobType(msg)

        sync_rules_enabled = self.connector.features.sync_rules_enabled()
        if sync_rules_enabled:
            await self.sync_job.validate_filtering(validator=self.data_provider)

        logger.debug("Preparing the content index")

        await self.sync_orchestrator.prepare_content_index(
            index_name=self.sync_job.index_name, language_code=self.sync_job.language
        )

        await self.sync_orchestrator.async_bulk(
            self.sync_job.index_name,
            self.prepare_docs(),
            self.sync_job.pipeline,
            job_type,
            filter_=self.sync_job.filtering,
            sync_rules_enabled=sync_rules_enabled,
            content_extraction_enabled=self._content_extraction_enabled(
                self.sync_job.configuration, self.sync_job.pipeline
            ),
            options=bulk_options,
            skip_unchanged_documents=self._skip_unchanged_documents_enabled(
                job_type, self.data_provider
            ),
            enable_bulk_operations_logging=self._enable_bulk_operations_logging,
        )

    async def _sync_done(self, sync_status, sync_error=None):
        if self.job_reporting_task is not None and not self.job_reporting_task.done():
            self.job_reporting_task.cancel()
            try:
                await self.job_reporting_task
            except asyncio.CancelledError:
                self.sync_job.log_debug("Job reporting task is stopped.")
        if self.sync_orchestrator is not None:
            await (
                self.sync_orchestrator.cancel()
            )  # in case the extractor/sink tasks are still running

        ingestion_stats = (
            {}
            if self.sync_orchestrator is None
            else self.sync_orchestrator.ingestion_stats()
        )
        persisted_stats = {
            INDEXED_DOCUMENT_COUNT: ingestion_stats.get(INDEXED_DOCUMENT_COUNT, 0),
            INDEXED_DOCUMENT_VOLUME: ingestion_stats.get(INDEXED_DOCUMENT_VOLUME, 0),
            DELETED_DOCUMENT_COUNT: ingestion_stats.get(DELETED_DOCUMENT_COUNT, 0),
        }

        if await self.reload_sync_job():
            if await self.reload_connector():
                persisted_stats[
                    "total_document_count"
                ] = await self.connector.document_count()

            if sync_status == JobStatus.ERROR:
                await self.sync_job.fail(sync_error, ingestion_stats=persisted_stats)
            elif sync_status == JobStatus.SUSPENDED:
                await self.sync_job.suspend(ingestion_stats=persisted_stats)
            elif sync_status == JobStatus.CANCELED:
                await self.sync_job.cancel(ingestion_stats=persisted_stats)
            else:
                await self.sync_job.done(ingestion_stats=persisted_stats)

        if await self.reload_connector():
            sync_cursor = (
                None
                if not self.data_provider  # If we failed before initializing the data provider, we don't need to change the cursor
                else (
                    self.data_provider.sync_cursor()
                    if self.sync_job.is_content_sync()
                    else None
                )
            )
            await self.connector.sync_done(
                self.sync_job if await self.reload_sync_job() else None,
                cursor=sync_cursor,
            )

        self.sync_job.log_info(
            f"Sync ended with status {sync_status.value} -- "
            f"created: {ingestion_stats.get(CREATES_QUEUED, 0)} | "
            f"updated: {ingestion_stats.get(UPDATES_QUEUED, 0)} | "
            f"deleted: {ingestion_stats.get(DELETES_QUEUED, 0)} "
            f"(took {int(time.time() - self._start_time)} seconds)"  # pyright: ignore
        )
        self.log_counters(ingestion_stats)

    def log_counters(self, counters):
        """
        Logs out a dump of everything in "counters"

        This serves a dual-purpose:
        1. Providing human-readable counts on separate lines, which can be helpful for
            visualizing how a counter changes over multiple runs (like in Discover)
        2. Providing a machine-readable hash of all counters, that can be easily copy-pasted
           into a REPL or script for analysis.
        See more of the discussion: https://github.com/elastic/connectors/pull/2323
        :param counters: a dictionary of counter_name -> counter_value like: {"added": 2, "deleted": 1}
        """
        self.sync_job.log_info("--- Counters ---")
        for k, v in sorted(counters.items()):
            self.sync_job.log_info(f"'{k}' : {v}")
        self.sync_job.log_info(f"full counters dictionary: {counters}")
        self.sync_job.log_info("----------------")

    @with_concurrency_control()
    async def sync_starts(self):
        if not await self.reload_connector():
            msg = f"Couldn't reload connector {self.connector.id}"
            raise SyncJobStartError(msg)

        job_type = self.sync_job.job_type

        if job_type in [JobType.FULL, JobType.INCREMENTAL]:
            if self.connector.last_sync_status == JobStatus.IN_PROGRESS:
                logger.debug(
                    f"A content sync job is started for connector {self.connector.id} by another connector instance, skipping..."
                )
                msg = f"A content sync job is started for connector {self.connector.id} by another connector instance"
                raise SyncJobStartError(msg)
        elif job_type == JobType.ACCESS_CONTROL:
            if self.connector.last_access_control_sync_status == JobStatus.IN_PROGRESS:
                logger.debug(
                    f"An access control sync job is started for connector {self.connector.id} by another connector instance, skipping..."
                )
                msg = f"An access control sync job is started for connector {self.connector.id} by another connector instance"
                raise SyncJobStartError(msg)
        else:
            logger.error(f"Unknown job type: '{job_type}'. Skipping running sync job")
            msg = f"Unknown job type: '{job_type}'. Skipping running sync job"
            raise SyncJobStartError(msg)

        try:
            await self.connector.sync_starts(job_type)
        except elasticsearch.ConflictError:
            raise
        except Exception as e:
            raise SyncJobStartError from e

    async def prepare_docs(self):
        self.sync_job.log_debug(f"Using pipeline {self.sync_job.pipeline}")

        async for doc, lazy_download, operation in self.generator():
            doc_id = str(doc.get("_id", ""))
            doc_id_size = len(doc_id.encode(UTF_8))

            if doc_id_size > ES_ID_SIZE_LIMIT:
                self.sync_job.log_debug(
                    f"Id '{truncate_id(doc_id)}' is too long: {doc_id_size} of maximum {ES_ID_SIZE_LIMIT} bytes, hashing"
                )

                hashed_id = self.source_klass.hash_id(doc_id)
                hashed_id_size = len(hashed_id.encode(UTF_8))

                if hashed_id_size > ES_ID_SIZE_LIMIT:
                    self.sync_job.log_error(
                        f"Hashed document id '{hashed_id}' with a size of '{hashed_id_size}' bytes is above the size limit of '{ES_ID_SIZE_LIMIT}' bytes."
                        f"Check the `hash_id` implementation of {self.source_klass.name}."
                    )
                    continue

                doc["_id"] = hashed_id

            # adapt doc for pipeline settings
            doc["_extract_binary_content"] = self.sync_job.pipeline[
                "extract_binary_content"
            ]
            doc["_reduce_whitespace"] = self.sync_job.pipeline["reduce_whitespace"]
            doc["_run_ml_inference"] = self.sync_job.pipeline["run_ml_inference"]
            yield doc, lazy_download, operation

    async def generator(self):
        skip_unchanged_documents = self._skip_unchanged_documents_enabled(
            self.sync_job.job_type, self.data_provider
        )

        match [self.sync_job.job_type, skip_unchanged_documents]:
            case [JobType.FULL, _]:
                async for doc, lazy_download in self.data_provider.get_docs(
                    filtering=self.sync_job.filtering
                ):
                    yield doc, lazy_download, OP_INDEX
            case [JobType.INCREMENTAL, optimization] if optimization is False:
                async for (
                    doc,
                    lazy_download,
                    operation,
                ) in self.data_provider.get_docs_incrementally(
                    sync_cursor=self.connector.sync_cursor,
                    filtering=self.sync_job.filtering,
                ):
                    yield doc, lazy_download, operation
            case [JobType.INCREMENTAL, optimization] if optimization is True:
                async for doc, lazy_download in self.data_provider.get_docs(
                    filtering=self.sync_job.filtering
                ):
                    yield doc, lazy_download, OP_INDEX
            case [JobType.ACCESS_CONTROL, _]:
                async for doc in self.data_provider.get_access_control():
                    yield doc, None, None
            case _:
                raise UnsupportedJobType

    async def update_ingestion_stats(self, interval):
        while True:
            await asyncio.sleep(interval)

            if not await self.reload_sync_job():
                break

            result = self.sync_orchestrator.ingestion_stats()
            ingestion_stats = {
                INDEXED_DOCUMENT_COUNT: result.get(INDEXED_DOCUMENT_COUNT, 0),
                INDEXED_DOCUMENT_VOLUME: result.get(INDEXED_DOCUMENT_VOLUME, 0),
                DELETED_DOCUMENT_COUNT: result.get(DELETED_DOCUMENT_COUNT, 0),
            }
            await self.sync_job.update_metadata(ingestion_stats=ingestion_stats)

    async def check_job(self):
        if not await self.reload_connector():
            raise ConnectorNotFoundError(self.connector.id)

        if not await self.reload_sync_job():
            raise ConnectorJobNotFoundError(self.sync_job.id)

        if self.sync_job.status == JobStatus.CANCELING:
            raise ConnectorJobCanceledError

        if self.sync_job.status != JobStatus.IN_PROGRESS:
            raise ConnectorJobNotRunningError(self.sync_job.id, self.sync_job.status)

    async def reload_sync_job(self):
        try:
            await self.sync_job.reload()
            return True
        except DocumentNotFoundError:
            self.sync_job.log_error("Couldn't reload sync job")
            return False

    async def reload_connector(self):
        try:
            await self.connector.reload()
            return True
        except DocumentNotFoundError:
            self.connector.log_error("Couldn't reload connector")
            return False

    def _content_extraction_enabled(self, sync_job_config, pipeline_config):
        if sync_job_config.get("use_text_extraction_service"):
            logger.debug(
                f"Binary content extraction via local extraction service is enabled for connector {self.connector.id} during sync job {self.sync_job.id}."
            )
            return True

        if pipeline_config.get("extract_binary_content"):
            logger.debug(
                f"Binary content extraction via pipelines is enabled for connector {self.connector.id} during sync job {self.sync_job.id}."
            )
            return True

        logger.debug(
            f"Binary content extraction is disabled for connector {self.connector.id} during sync job {self.sync_job.id}."
        )
        return False
