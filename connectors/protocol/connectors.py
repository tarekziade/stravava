#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""
Database for connectors - see docs/CONNECTOR_PROTOCOL.md indices.

Main classes are :

- ConnectorIndex: represents a document in `.elastic-connectors`
- SyncJob: represents a document in `.elastic-connectors-sync-jobs`

"""

import socket
from collections import UserDict
from copy import deepcopy
from datetime import datetime, timezone
from enum import Enum

from elasticsearch import ApiError

from connectors.es import ESDocument, ESIndex
from connectors.es.client import with_concurrency_control
from connectors.filtering.validation import (
    FilteringValidationState,
    InvalidFilteringError,
)
from connectors.logger import logger
from connectors.source import (
    DEFAULT_CONFIGURATION,
    DataSourceConfiguration,
    get_source_klass,
)
from connectors.utils import (
    ACCESS_CONTROL_INDEX_PREFIX,
    deep_merge_dicts,
    filter_nested_dict_by_keys,
    iso_utc,
    nested_get_from_dict,
    next_run,
    parse_datetime_string,
    with_utc_tz,
)

__all__ = [
    "CONNECTORS_INDEX",
    "JOBS_INDEX",
    "CONCRETE_CONNECTORS_INDEX",
    "CONCRETE_JOBS_INDEX",
    "ConnectorIndex",
    "Filter",
    "SyncJobIndex",
    "DataSourceError",
    "JobStatus",
    "Pipeline",
    "JobType",
    "JobTriggerMethod",
    "ServiceTypeNotConfiguredError",
    "ServiceTypeNotSupportedError",
    "Status",
    "IDLE_JOBS_THRESHOLD",
    "JOB_NOT_FOUND_ERROR",
    "Connector",
    "Features",
    "Filtering",
    "Sort",
    "SyncJob",
    "CONNECTORS_ACCESS_CONTROL_INDEX_PREFIX",
]


CONNECTORS_INDEX = ".elastic-connectors"
JOBS_INDEX = ".elastic-connectors-sync-jobs"
CONCRETE_CONNECTORS_INDEX = CONNECTORS_INDEX + "-v1"
CONCRETE_JOBS_INDEX = JOBS_INDEX + "-v1"
CONNECTORS_ACCESS_CONTROL_INDEX_PREFIX = ".search-acl-filter-"

JOB_NOT_FOUND_ERROR = "Couldn't find the job"
UNKNOWN_ERROR = "unknown error"

INDEXED_DOCUMENT_COUNT = "indexed_document_count"
INDEXED_DOCUMENT_VOLUME = "indexed_document_volume"
DELETED_DOCUMENT_COUNT = "deleted_document_count"
TOTAL_DOCUMENT_COUNT = "total_document_count"
ALLOWED_INGESTION_STATS_KEYS = (
    INDEXED_DOCUMENT_COUNT,
    INDEXED_DOCUMENT_VOLUME,
    DELETED_DOCUMENT_COUNT,
    TOTAL_DOCUMENT_COUNT,
)


class Status(Enum):
    CREATED = "created"
    NEEDS_CONFIGURATION = "needs_configuration"
    CONFIGURED = "configured"
    CONNECTED = "connected"
    ERROR = "error"
    UNSET = None


class JobStatus(Enum):
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    CANCELING = "canceling"
    CANCELED = "canceled"
    SUSPENDED = "suspended"
    COMPLETED = "completed"
    ERROR = "error"
    UNSET = None


class JobType(Enum):
    FULL = "full"
    INCREMENTAL = "incremental"
    ACCESS_CONTROL = "access_control"
    UNSET = None


class JobTriggerMethod(Enum):
    ON_DEMAND = "on_demand"
    SCHEDULED = "scheduled"
    UNSET = None


class Sort(Enum):
    ASC = "asc"
    DESC = "desc"


class ServiceTypeNotSupportedError(Exception):
    pass


class ServiceTypeNotConfiguredError(Exception):
    pass


class DataSourceError(Exception):
    pass


class InvalidConnectorSetupError(Exception):
    pass


class ProtocolError(Exception):
    pass


class ConnectorIndex(ESIndex):
    def __init__(self, elastic_config):
        logger.debug(f"ConnectorIndex connecting to {elastic_config['host']}")
        # initialize ESIndex instance
        super().__init__(index_name=CONNECTORS_INDEX, elastic_config=elastic_config)

    async def heartbeat(self, doc_id):
        if self.feature_use_connectors_api:
            await self.api.connector_check_in(doc_id)
        else:
            await self.update(doc_id=doc_id, doc={"last_seen": iso_utc()})

    async def supported_connectors(self, native_service_types=None, connector_ids=None):
        if native_service_types is None:
            native_service_types = []
        if connector_ids is None:
            connector_ids = []

        if len(native_service_types) == 0 and len(connector_ids) == 0:
            return

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
            query = {
                "bool": {"should": [native_connectors_query, custom_connectors_query]}
            }
        elif len(native_service_types) > 0:
            query = native_connectors_query
        else:
            query = custom_connectors_query

        async for connector in self.get_all_docs(query=query):
            yield connector

    def _create_object(self, doc_source):
        return Connector(
            self,
            doc_source,
        )

    async def get_connector_by_index(self, index_name):
        connectors = [
            connector
            async for connector in self.get_all_docs(
                {"match": {"index_name": index_name}}
            )
        ]
        if len(connectors) > 1:
            msg = f"Multiple connectors exist for index {index_name}"
            raise InvalidConnectorSetupError(msg)
        elif len(connectors) == 0:
            return None
        else:
            return connectors[0]

    async def all_connectors(self):
        async for connector in self.get_all_docs():
            yield connector


def filter_ingestion_stats(ingestion_stats):
    if ingestion_stats is None:
        return {}

    return {
        k: v for (k, v) in ingestion_stats.items() if k in ALLOWED_INGESTION_STATS_KEYS
    }


class SyncJob(ESDocument):
    @property
    def status(self):
        return JobStatus(self.get("status"))

    @property
    def error(self):
        return self.get("error")

    @property
    def connector_id(self):
        return self.get("connector", "id")

    @property
    def index_name(self):
        return self.get("connector", "index_name")

    @property
    def language(self):
        return self.get("connector", "language")

    @property
    def service_type(self):
        return self.get("connector", "service_type")

    @property
    def configuration(self):
        return DataSourceConfiguration(self.get("connector", "configuration"))

    @property
    def filtering(self):
        return Filter(self.get("connector", "filtering", default={}))

    @property
    def pipeline(self):
        return Pipeline(self.get("connector", "pipeline"))

    @property
    def sync_cursor(self):
        return self.get("connector", "sync_cursor")

    @property
    def terminated(self):
        return self.status in (JobStatus.ERROR, JobStatus.COMPLETED, JobStatus.CANCELED)

    @property
    def indexed_document_count(self):
        return self.get(INDEXED_DOCUMENT_COUNT, default=0)

    @property
    def indexed_document_volume(self):
        return self.get(INDEXED_DOCUMENT_VOLUME, default=0)

    @property
    def deleted_document_count(self):
        return self.get(DELETED_DOCUMENT_COUNT, default=0)

    @property
    def total_document_count(self):
        return self.get(TOTAL_DOCUMENT_COUNT, default=0)

    @property
    def job_type(self):
        return JobType(self.get("job_type"))

    def is_content_sync(self):
        return self.job_type in (JobType.FULL, JobType.INCREMENTAL)

    async def validate_filtering(self, validator):
        validation_result = await validator.validate_filtering(self.filtering)

        if validation_result.state != FilteringValidationState.VALID:
            msg = f"Filtering in state {validation_result.state}, errors: {validation_result.errors}."
            raise InvalidFilteringError(msg)

    def _wrap_errors(self, operation_name, e):
        if isinstance(e, ApiError):
            reason = None
            if e.info is not None and "error" in e.info and "reason" in e.info["error"]:
                reason = e.info["error"]["reason"]
            msg = f"Failed to {operation_name} for job {self.id} because Elasticsearch responded with status {e.status_code}.{f' Reason: {reason}' if reason else ''}"
            raise ProtocolError(msg) from e
        else:
            msg = f"Failed to {operation_name} for job {self.id} because of {e.__class__.__name__}: {str(e)}"
            raise ProtocolError(msg) from e

    async def claim(self, sync_cursor=None):
        try:
            if self.index.feature_use_connectors_api:
                await self.index.api.connector_sync_job_claim(
                    sync_job_id=self.id,
                    worker_hostname=socket.gethostname(),
                    sync_cursor=sync_cursor,
                )
            else:
                doc = {
                    "status": JobStatus.IN_PROGRESS.value,
                    "started_at": iso_utc(),
                    "last_seen": iso_utc(),
                    "worker_hostname": socket.gethostname(),
                    "connector.sync_cursor": sync_cursor,
                }
                await self.index.update(doc_id=self.id, doc=doc)
        except Exception as e:
            self._wrap_errors("claim job", e)

    async def update_metadata(self, ingestion_stats=None, connector_metadata=None):
        try:
            ingestion_stats = filter_ingestion_stats(ingestion_stats)
            if self.index.feature_use_connectors_api:
                await self.index.api.connector_sync_job_update_stats(
                    sync_job_id=self.id,
                    ingestion_stats=ingestion_stats,
                    metadata=connector_metadata,
                )
            else:
                if connector_metadata is None:
                    connector_metadata = {}

                doc = {
                    "last_seen": iso_utc(),
                }
                doc.update(ingestion_stats)
                if len(connector_metadata) > 0:
                    doc["metadata"] = connector_metadata
                await self.index.update(doc_id=self.id, doc=doc)
        except Exception as e:
            self._wrap_errors("update metadata and stats", e)

    async def done(self, ingestion_stats=None, connector_metadata=None):
        try:
            await self._terminate(
                JobStatus.COMPLETED, None, ingestion_stats, connector_metadata
            )
        except Exception as e:
            self._wrap_errors("terminate as completed", e)

    async def fail(self, error, ingestion_stats=None, connector_metadata=None):
        try:
            if isinstance(error, str):
                message = error
            elif isinstance(error, Exception):
                message = f"{error.__class__.__name__}"
                if str(error):
                    message += f": {str(error)}"
            else:
                message = str(error)
            await self._terminate(
                JobStatus.ERROR, message, ingestion_stats, connector_metadata
            )
        except Exception as e:
            self._wrap_errors("terminate as failed", e)

    async def cancel(self, ingestion_stats=None, connector_metadata=None):
        try:
            await self._terminate(
                JobStatus.CANCELED, None, ingestion_stats, connector_metadata
            )
        except Exception as e:
            self._wrap_errors("terminate as canceled", e)

    async def suspend(self, ingestion_stats=None, connector_metadata=None):
        try:
            await self._terminate(
                JobStatus.SUSPENDED, None, ingestion_stats, connector_metadata
            )
        except Exception as e:
            self._wrap_errors("terminate as suspended", e)

    async def _terminate(
        self, status, error=None, ingestion_stats=None, connector_metadata=None
    ):
        ingestion_stats = filter_ingestion_stats(ingestion_stats)
        if connector_metadata is None:
            connector_metadata = {}

        doc = {
            "last_seen": iso_utc(),
            "status": status.value,
            "error": error,
        }
        if status in (JobStatus.ERROR, JobStatus.COMPLETED, JobStatus.CANCELED):
            doc["completed_at"] = iso_utc()
        if status == JobStatus.CANCELED:
            doc["canceled_at"] = iso_utc()
        doc.update(ingestion_stats)
        if len(connector_metadata) > 0:
            doc["metadata"] = connector_metadata
        await self.index.update(doc_id=self.id, doc=doc)

    def _prefix(self):
        return f"[Connector id: {self.connector_id}, index name: {self.index_name}, Sync job id: {self.id}]"

    def _extra(self):
        return {
            "labels.sync_job_id": self.id,
            "labels.connector_id": self.connector_id,
            "labels.index_name": self.index_name,
            "labels.service_type": self.service_type,
        }


class Filtering:
    DEFAULT_DOMAIN = "DEFAULT"

    def __init__(self, filtering=None):
        if filtering is None:
            filtering = []

        self.filtering = filtering

    def get_active_filter(self, domain=DEFAULT_DOMAIN):
        return self.get_filter(filter_state="active", domain=domain)

    def get_draft_filter(self, domain=DEFAULT_DOMAIN):
        return self.get_filter(filter_state="draft", domain=domain)

    def get_filter(self, filter_state="active", domain=DEFAULT_DOMAIN):
        return next(
            (
                Filter(filter_[filter_state])
                for filter_ in self.filtering
                if filter_["domain"] == domain
            ),
            Filter(),
        )

    def to_list(self):
        return list(self.filtering)


class Filter(dict):
    def __init__(self, filter_=None):
        if filter_ is None:
            filter_ = {}

        super().__init__(filter_)

        self.advanced_rules = filter_.get("advanced_snippet", {})
        self.basic_rules = filter_.get("rules", [])
        self.validation = filter_.get(
            "validation", {"state": FilteringValidationState.VALID.value, "errors": []}
        )

    def get_advanced_rules(self):
        return self.advanced_rules.get("value", {})

    def has_advanced_rules(self):
        advanced_rules = self.get_advanced_rules()
        return advanced_rules is not None and len(advanced_rules) > 0

    def has_validation_state(self, validation_state):
        return FilteringValidationState(self.validation["state"]) == validation_state

    def transform_filtering(self):
        """
        Transform the filtering in .elastic-connectors to filtering ready-to-use in .elastic-connectors-sync-jobs
        """
        # deepcopy to not change the reference resulting in changing .elastic-connectors filtering
        filtering = (
            {"advanced_snippet": {}, "rules": []} if len(self) == 0 else deepcopy(self)
        )

        return filtering


PIPELINE_DEFAULT = {
    "name": "ent-search-generic-ingestion",
    "extract_binary_content": True,
    "reduce_whitespace": True,
    "run_ml_inference": True,
}


class Pipeline(UserDict):
    def __init__(self, data):
        if data is None:
            data = {}
        default = PIPELINE_DEFAULT.copy()
        default.update(data)
        super().__init__(default)


class Features:
    DOCUMENT_LEVEL_SECURITY = "document_level_security"

    BASIC_RULES_NEW = "basic_rules_new"
    ADVANCED_RULES_NEW = "advanced_rules_new"

    # keep backwards compatibility
    BASIC_RULES_OLD = "basic_rules_old"
    ADVANCED_RULES_OLD = "advanced_rules_old"

    NATIVE_CONNECTOR_API_KEYS = "native_connector_api_keys"

    def __init__(self, features=None):
        if features is None:
            features = {}

        self.features = features

    def incremental_sync_enabled(self):
        return nested_get_from_dict(
            self.features, ["incremental_sync", "enabled"], default=False
        )

    def document_level_security_enabled(self):
        return nested_get_from_dict(
            self.features, ["document_level_security", "enabled"], default=False
        )

    def native_connector_api_keys_enabled(self):
        return nested_get_from_dict(
            self.features, ["native_connector_api_keys", "enabled"], default=True
        )

    def sync_rules_enabled(self):
        return any(
            [
                self.feature_enabled(Features.BASIC_RULES_NEW),
                self.feature_enabled(Features.BASIC_RULES_OLD),
                self.feature_enabled(Features.ADVANCED_RULES_NEW),
                self.feature_enabled(Features.ADVANCED_RULES_OLD),
            ]
        )

    def feature_enabled(self, feature):
        match feature:
            case Features.BASIC_RULES_NEW:
                return nested_get_from_dict(
                    self.features, ["sync_rules", "basic", "enabled"], default=False
                )
            case Features.ADVANCED_RULES_NEW:
                return nested_get_from_dict(
                    self.features, ["sync_rules", "advanced", "enabled"], default=False
                )
            case Features.BASIC_RULES_OLD:
                return self.features.get("filtering_rules", False)
            case Features.ADVANCED_RULES_OLD:
                return self.features.get("filtering_advanced_config", False)
            case _:
                return False


class Connector(ESDocument):
    @property
    def status(self):
        return Status(self.get("status"))

    @property
    def service_type(self):
        return self.get("service_type")

    @property
    def last_seen(self):
        return self._property_as_datetime("last_seen")

    @property
    def native(self):
        return self.get("is_native", default=False)

    @property
    def full_sync_scheduling(self):
        return self.get("scheduling", "full", default={})

    @property
    def incremental_sync_scheduling(self):
        return self.get("scheduling", "incremental", default={})

    @property
    def access_control_sync_scheduling(self):
        return self.get("scheduling", "access_control", default={})

    @property
    def configuration(self):
        return DataSourceConfiguration(self.get("configuration"))

    @property
    def index_name(self):
        return self.get("index_name")

    @property
    def language(self):
        return self.get("language")

    @property
    def filtering(self):
        return Filtering(self.get("filtering"))

    @property
    def pipeline(self):
        return Pipeline(self.get("pipeline"))

    @property
    def features(self):
        return Features(self.get("features"))

    @property
    def last_sync_status(self):
        return JobStatus(self.get("last_sync_status"))

    @property
    def last_access_control_sync_status(self):
        return JobStatus(self.get("last_access_control_sync_status"))

    def _property_as_datetime(self, key):
        value = self.get(key)
        if value is not None:
            value = parse_datetime_string(value)  # pyright: ignore
            # Ensure the datetime is in UTC for backward compatibility with historically naive timestamps.
            # This guarantees that job scheduling logic with offset-aware timestamps works correctly.
            value = with_utc_tz(value)
        return value

    @property
    def last_sync_scheduled_at(self):
        return self._property_as_datetime("last_sync_scheduled_at")

    @property
    def last_incremental_sync_scheduled_at(self):
        return self._property_as_datetime("last_incremental_sync_scheduled_at")

    @property
    def last_access_control_sync_scheduled_at(self):
        return self._property_as_datetime("last_access_control_sync_scheduled_at")

    def last_sync_scheduled_at_by_job_type(self, job_type):
        match job_type:
            case JobType.ACCESS_CONTROL:
                return self.last_access_control_sync_scheduled_at
            case JobType.INCREMENTAL:
                return self.last_incremental_sync_scheduled_at
            case JobType.FULL:
                return self.last_sync_scheduled_at
            case _:
                msg = f"Unknown job type: {job_type}"
                raise ValueError(msg)

    @property
    def sync_cursor(self):
        return self.get("sync_cursor")

    @property
    def api_key_secret_id(self):
        return self.get("api_key_secret_id")

    async def heartbeat(self, interval):
        if (
            self.last_seen is None
            or (datetime.now(timezone.utc) - self.last_seen).total_seconds() > interval
        ):
            self.log_debug("Sending heartbeat")
            await self.index.heartbeat(doc_id=self.id)

    def next_sync(self, job_type, now):
        """Returns the datetime in UTC timezone when the next sync for a given job type will run, return None if it's disabled."""

        match job_type:
            case JobType.ACCESS_CONTROL:
                scheduling_property = self.access_control_sync_scheduling
            case JobType.INCREMENTAL:
                scheduling_property = self.incremental_sync_scheduling
            case JobType.FULL:
                scheduling_property = self.full_sync_scheduling
            case _:
                msg = f"Unknown job type: {job_type}"
                raise ValueError(msg)

        if not scheduling_property.get("enabled", False):
            return None
        return next_run(scheduling_property.get("interval"), now)

    async def _update_datetime(self, field, new_ts):
        if self.index.feature_use_connectors_api:
            await self.index.api.connector_update_last_sync_info(
                connector_id=self.id, last_sync_info={field: iso_utc(new_ts)}
            )
        else:
            await self.index.update(
                doc_id=self.id,
                doc={field: iso_utc(new_ts)},
                if_seq_no=self._seq_no,
                if_primary_term=self._primary_term,
            )

    async def update_last_sync_scheduled_at_by_job_type(self, job_type, new_ts):
        match job_type:
            case JobType.ACCESS_CONTROL:
                await self._update_datetime(
                    "last_access_control_sync_scheduled_at", new_ts
                )
            case JobType.INCREMENTAL:
                await self._update_datetime(
                    "last_incremental_sync_scheduled_at", new_ts
                )
            case JobType.FULL:
                await self._update_datetime("last_sync_scheduled_at", new_ts)
            case _:
                msg = f"Unknown job type: {job_type}"
                raise ValueError(msg)

    async def sync_starts(self, job_type):
        if job_type == JobType.ACCESS_CONTROL:
            last_sync_information = {
                "last_access_control_sync_status": JobStatus.IN_PROGRESS.value,
                "last_access_control_sync_error": None,
            }
        elif job_type in [JobType.INCREMENTAL, JobType.FULL]:
            last_sync_information = {
                "last_sync_status": JobStatus.IN_PROGRESS.value,
                "last_sync_error": None,
            }
        else:
            msg = f"Unknown job type: {job_type}"
            raise ValueError(msg)

        if self.index.feature_use_connectors_api:
            await self.index.api.connector_update_error(
                connector_id=self.id, error=None
            )
            await self.index.api.connector_update_last_sync_info(
                connector_id=self.id, last_sync_info=last_sync_information
            )
        else:
            doc = {
                "status": Status.CONNECTED.value,
                "error": None,
            } | last_sync_information

            await self.index.update(
                doc_id=self.id,
                doc=doc,
                if_seq_no=self._seq_no,
                if_primary_term=self._primary_term,
            )

    async def error(self, error):
        if self.index.feature_use_connectors_api:
            await self.index.api.connector_update_error(
                connector_id=self.id, error=error
            )
        else:
            doc = {
                "status": Status.ERROR.value,
                "error": str(error),
            }
            await self.index.update(doc_id=self.id, doc=doc)

    async def sync_done(self, job, cursor=None):
        job_status = JobStatus.ERROR if job is None else job.status
        job_error = JOB_NOT_FOUND_ERROR if job is None else job.error
        job_type = job.job_type if job is not None else None

        if job_error is None and job_status == JobStatus.ERROR:
            job_error = UNKNOWN_ERROR
        connector_status = (
            Status.ERROR if job_status == JobStatus.ERROR else Status.CONNECTED
        )

        if job_type == JobType.ACCESS_CONTROL:
            last_sync_information = {
                "last_access_control_sync_status": job_status.value,
                "last_access_control_sync_error": job_error,
            }
        elif job_type in [JobType.INCREMENTAL, JobType.FULL]:
            last_sync_information = {
                "last_sync_status": job_status.value,
                "last_sync_error": job_error,
            }
        elif job_type is None:
            # If we don't know the job type we'll reset all sync jobs,
            # so we don't run into the risk of staying "in progress" forever

            last_sync_information = {
                "last_access_control_sync_status": job_status.value,
                "last_access_control_sync_error": job_error,
                "last_sync_status": job_status.value,
                "last_sync_error": job_error,
            }
        else:
            msg = f"Unknown job type: {job_type}"
            raise ValueError(msg)

        doc = {
            "last_synced": iso_utc(),
        } | last_sync_information

        # only update sync cursor after a successful content sync job
        if job_type != JobType.ACCESS_CONTROL and job_status == JobStatus.COMPLETED:
            doc["sync_cursor"] = cursor

        if job is not None and job.terminated:
            doc["last_indexed_document_count"] = job.indexed_document_count
            doc["last_deleted_document_count"] = job.deleted_document_count

        if self.index.feature_use_connectors_api:
            await self.index.api.connector_update_error(
                connector_id=self.id, error=job_error
            )
            await self.index.api.connector_update_last_sync_info(
                connector_id=self.id, last_sync_info=last_sync_information
            )
        else:
            doc = doc | {"status": connector_status.value, "error": job_error}
            await self.index.update(doc_id=self.id, doc=doc)

    @with_concurrency_control()
    async def prepare(self, config, sources):
        """Prepares the connector, given a configuration
        If the connector id and the service type is in the config, we want to
        populate the service type and then sets the default configuration.

        Also checks that the configuration structure is correct.
        If a field is missing, raises an error. If a property is missing, add it.

        This method will also populate the features available for the data source
        if it's different from the features in the connector document
        """
        await self.reload()

        configured_connector_id = config.get("connector_id", "")
        configured_service_type = config.get("service_type", "")
        is_main_connector = self.id == configured_connector_id

        if is_main_connector:
            if not configured_service_type:
                self.log_error("Service type is not configured")
                msg = "Service type is not configured."
                raise ServiceTypeNotConfiguredError(msg)

            if configured_service_type not in sources:
                raise ServiceTypeNotSupportedError(configured_service_type)
        else:
            if self.service_type not in sources:
                self.log_debug(
                    f"Peripheral connector has invalid service type {self.service_type}, cannot check configuration formatting."
                )
                return

        fqn = (
            sources[configured_service_type]
            if is_main_connector
            else sources[self.service_type]
        )
        try:
            source_klass = get_source_klass(fqn)
        except Exception as e:
            self.log_critical(e, exc_info=True)
            msg = f"Could not instantiate {fqn} for {configured_service_type}"
            raise DataSourceError(msg) from e

        doc = self.validated_doc(source_klass)
        if is_main_connector and self.service_type is None:
            doc["service_type"] = configured_service_type
            self.log_debug(f"Populated service type {configured_service_type}")

        if is_main_connector and self.features.features != source_klass.features():
            doc["features"] = source_klass.features()
            self.log_debug("Populated features")
        elif (
            self.native
            and self.features.features.keys() != source_klass.features().keys()
        ):
            missing_features = (
                source_klass.features().keys() - self.features.features.keys()
            )
            doc["features"] = source_klass.features() | self.features.features
            self.log_debug(
                f"Added missing features [{', '.join(missing_features)}] from Native definitions"
            )

        if not doc:
            return

        await self.index.update(
            doc_id=self.id,
            doc=doc,
            if_seq_no=self._seq_no,
            if_primary_term=self._primary_term,
        )
        await self.reload()

    def validated_doc(self, source_klass):
        simple_config = source_klass.get_simple_configuration()
        current_config = self.configuration.to_dict()

        if self.configuration.is_empty():
            # sets the defaults and the flag to NEEDS_CONFIGURATION
            self.log_debug("Populated configuration")
            return {
                "configuration": simple_config,
                "status": Status.NEEDS_CONFIGURATION.value,
            }

        missing_fields = simple_config.keys() - current_config.keys()
        fields_missing_properties = filter_nested_dict_by_keys(
            DEFAULT_CONFIGURATION.keys(), current_config
        )
        if not missing_fields and not fields_missing_properties:
            return {}

        doc = {"configuration": {}}
        if missing_fields:
            doc["configuration"] = self.updated_configuration_fields(
                missing_fields, current_config, simple_config
            )
        if fields_missing_properties:
            updated_config = self.updated_configuration_field_properties(
                fields_missing_properties, simple_config
            )
            doc["configuration"] = deep_merge_dicts(
                doc["configuration"], updated_config
            )

        return doc

    def updated_configuration_fields(
        self, missing_keys, current_config, simple_default_config
    ):
        self.log_warning(
            f"Detected an existing connector: {self.id} ({self.service_type}) that was previously {Status.CONNECTED.value} but is now missing configuration: {missing_keys}. Values for the new fields will be automatically set. Please review these configuration values as part of your upgrade."
        )
        draft_config = {
            k: simple_default_config[k] for k in missing_keys
        }  # add missing configs as they exist in the simple default

        # copy any differences (excluding differences in "value") to the draft
        # the contents of simple_default_config are used unless they are missing
        for config_name, config_obj in current_config.items():
            for k, v in config_obj.items():
                simple_default_value = simple_default_config.get(config_name, {}).get(k)
                if k != "value" and simple_default_value != v:
                    draft_config_obj = draft_config.get(config_name, {})
                    draft_config_obj[k] = simple_default_value or v
                    draft_config[config_name] = draft_config_obj
        return draft_config

    def updated_configuration_field_properties(
        self, fields_missing_properties, simple_config
    ):
        """Checks the field properties for every field in a configuration.
        If a field is missing field properties, add those field properties
        with default values.
        """
        self.log_info(
            f"Connector {self.id} ({self.service_type}) is missing configuration field properties. Generating defaults."
        )

        # filter the default config by what fields we want to update, then merge the actual config into it
        filtered_simple_config = {
            key: value
            for key, value in simple_config.items()
            if key in fields_missing_properties.keys()
        }
        return deep_merge_dicts(filtered_simple_config, fields_missing_properties)

    @with_concurrency_control()
    async def validate_filtering(self, validator):
        await self.reload()
        draft_filter = self.filtering.get_draft_filter()
        if not draft_filter.has_validation_state(FilteringValidationState.EDITED):
            self.log_debug(
                f"Filtering is in state {draft_filter.validation['state']}, skipping..."
            )
            return

        self.log_info(
            f"Filtering is in state {FilteringValidationState.EDITED.value}, validating...)"
        )
        validation_result = await validator.validate_filtering(draft_filter)
        self.log_info(f"Filtering validation result: {validation_result.state.value}")

        if self.index.feature_use_connectors_api:
            await self.index.api.connector_update_filtering_draft_validation(
                connector_id=self.id, validation_result=validation_result.to_dict()
            )
            if validation_result.state == FilteringValidationState.VALID:
                await self.index.api.connector_activate_filtering_draft(
                    connector_id=self.id
                )
        else:
            filtering = self.filtering.to_list()
            for filter_ in filtering:
                if filter_.get("domain", "") == Filtering.DEFAULT_DOMAIN:
                    filter_.get("draft", {"validation": {}})["validation"] = (
                        validation_result.to_dict()
                    )
                    if validation_result.state == FilteringValidationState.VALID:
                        filter_["active"] = filter_.get("draft")

            await self.index.update(
                doc_id=self.id,
                doc={"filtering": filtering},
                if_seq_no=self._seq_no,
                if_primary_term=self._primary_term,
            )
        await self.reload()

    async def document_count(self):
        if not self.index.serverless:
            await self.index.client.indices.refresh(
                index=self.index_name, ignore_unavailable=True
            )
        result = await self.index.client.count(
            index=self.index_name, ignore_unavailable=True
        )
        return result["count"]

    def _prefix(self):
        return f"[Connector id: {self.id}, index name: {self.index_name}]"

    def _extra(self):
        return {
            "labels.connector_id": self.id,
            "labels.index_name": self.index_name,
            "labels.service_type": self.service_type,
        }


IDLE_JOBS_THRESHOLD = 60 * 5  # 5 minutes


class SyncJobIndex(ESIndex):
    """
    Represents Elasticsearch index for sync jobs

    Args:
        elastic_config (dict): Elasticsearch configuration and credentials
    """

    def __init__(self, elastic_config):
        super().__init__(index_name=JOBS_INDEX, elastic_config=elastic_config)

    def _create_object(self, doc_source):
        """
        Args:
            doc_source (dict): A raw Elasticsearch document
        Returns:
            SyncJob
        """
        return SyncJob(
            self,
            doc_source=doc_source,
        )

    async def create(self, connector, trigger_method, job_type):
        if self.feature_use_connectors_api:
            response = await self.api.connector_sync_job_create(
                connector_id=connector.id,
                trigger_method=trigger_method.value,
                job_type=job_type.value,
            )
            return response["id"]
        else:
            filtering = connector.filtering.get_active_filter().transform_filtering()
            index_name = connector.index_name

            if job_type == JobType.ACCESS_CONTROL:
                index_name = f"{ACCESS_CONTROL_INDEX_PREFIX}{index_name}"

            job_def = {
                "connector": {
                    "id": connector.id,
                    "filtering": filtering,
                    "index_name": index_name,
                    "language": connector.language,
                    "pipeline": connector.pipeline.data,
                    "service_type": connector.service_type,
                    "configuration": connector.configuration.to_dict(),
                },
                "trigger_method": trigger_method.value,
                "job_type": job_type.value,
                "status": JobStatus.PENDING.value,
                INDEXED_DOCUMENT_COUNT: 0,
                INDEXED_DOCUMENT_VOLUME: 0,
                DELETED_DOCUMENT_COUNT: 0,
                "created_at": iso_utc(),
                "last_seen": iso_utc(),
            }
            api_response = await self.index(job_def)

            return api_response["_id"]

    async def pending_jobs(self, connector_ids, job_types):
        if not job_types:
            return
        if not isinstance(job_types, list):
            job_types = [str(job_types)]
        query = {
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
                    {"terms": {"job_type": job_types}},
                ]
            }
        }
        sort = [{"created_at": Sort.ASC.value}]
        async for job in self.get_all_docs(query=query, sort=sort):
            yield job

    async def orphaned_idle_jobs(self, connector_ids):
        query = {
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
        async for job in self.get_all_docs(query=query):
            yield job

    async def idle_jobs(self, connector_ids):
        query = {
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

        async for job in self.get_all_docs(query=query):
            yield job
