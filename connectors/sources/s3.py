#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import asyncio
import os
from contextlib import AsyncExitStack
from functools import partial

import aioboto3
import fastjsonschema
from aiobotocore.config import AioConfig
from botocore.exceptions import ClientError
from fastjsonschema import JsonSchemaValueException

from connectors.filtering.validation import (
    AdvancedRulesValidator,
    SyncRuleValidationResult,
)
from connectors.logger import logger
from connectors.source import BaseDataSource
from connectors.utils import hash_id

DEFAULT_PAGE_SIZE = 100
DEFAULT_MAX_RETRY_ATTEMPTS = 5
DEFAULT_CONNECTION_TIMEOUT = 90
DEFAULT_READ_TIMEOUT = 90

if "AWS_ENDPOINT_URL" in os.environ:
    AWS_ENDPOINT = f"{os.environ['AWS_ENDPOINT_URL']}:{os.environ['AWS_PORT']}"
else:
    AWS_ENDPOINT = None


class S3Client:
    """Amazon S3 client to handle method calls made to S3"""

    def __init__(self, configuration):
        self.configuration = configuration
        self._logger = logger
        self.session = aioboto3.Session(
            aws_access_key_id=self.configuration["aws_access_key_id"],
            aws_secret_access_key=self.configuration["aws_secret_access_key"],
        )
        self.config = AioConfig(
            read_timeout=self.configuration["read_timeout"],
            connect_timeout=self.configuration["connect_timeout"],
            retries={"max_attempts": self.configuration["max_attempts"]},
            s3={
                "addressing_style": self.configuration["s3_addressing_style"],
            },
            use_fips_endpoint=self.configuration["aws_use_fips_endpoint"],
            use_dualstack_endpoint=self.configuration["aws_use_dualstack_endpoint"],
        )

        if self.configuration["s3_endpoint_url"]:
            self.s3_endpoint_url = self.configuration["s3_endpoint_url"]
        elif AWS_ENDPOINT is not None:
            self.s3_endpoint_url = AWS_ENDPOINT
        else:
            self.s3_endpoint_url = None

        self.clients = {}
        self.client_context = []

    def set_logger(self, logger_):
        self._logger = logger_

    async def client(self, region=None):
        """This method creates context manager and client session object for s3.
        Args:
            region (str): Name of bucket region. Defaults to None
        """
        region_name = region if region else "default"

        if region_name in self.clients:
            return self.clients[region_name]

        if self.s3_endpoint_url is not None:
            self._logger.debug(f"Creating a session against {self.s3_endpoint_url}")

        # AsyncExitStack, supports asynchronous context managers, used to create client using enter_async_context and
        # these context manager will be stored in client_context list also client will be stored in clients dict with their region
        s3_context_stack = AsyncExitStack()
        s3_client = await s3_context_stack.enter_async_context(
            self.session.client(
                service_name="s3",
                config=self.config,
                endpoint_url=self.s3_endpoint_url,
                region_name=region,
            )
        )
        self.client_context.append(s3_context_stack)

        self.clients[region_name] = s3_client
        return self.clients[region_name]

    async def close_client(self):
        """Closes unclosed client session"""
        for context in self.client_context:
            await context.aclose()

    async def fetch_buckets(self):
        """This method used to list all the buckets from Amazon S3"""
        s3 = await self.client()
        await s3.list_buckets()

    async def get_bucket_list(self):
        """Returns bucket list from list_buckets response

        Returns:
            list: List of buckets
        """
        if self.configuration["buckets"] == ["*"]:
            s3 = await self.client()
            bucket_list = await s3.list_buckets()
            buckets = [bucket["Name"] for bucket in bucket_list["Buckets"]]
        else:
            buckets = self.configuration["buckets"]
        return buckets

    async def get_bucket_objects(self, bucket, **kwargs):
        """Returns bucket list from list_buckets response
        Args:
            bucket (str): Name of bucket
        Yields:
            obj_summary: Bucket objects metadata
            s3_client: S3 client object
        """
        page_size = self.configuration["page_size"]
        region_name = await self.get_bucket_region(bucket)
        s3_client = await self.client(region=region_name)
        async with self.session.resource(
            service_name="s3",
            config=self.config,
            endpoint_url=AWS_ENDPOINT,
            region_name=region_name,
        ) as s3:
            try:
                bucket_obj = await s3.Bucket(bucket)
                await asyncio.sleep(0)

                if kwargs.get("prefix"):
                    objects = bucket_obj.objects.filter(
                        Prefix=kwargs["prefix"]
                    ).page_size(page_size)
                else:
                    objects = bucket_obj.objects.page_size(page_size)

                async for obj_summary in objects:
                    yield obj_summary, s3_client
            except Exception as exception:
                self._logger.warning(
                    f"Something went wrong while fetching documents from {bucket}. Error: {exception}"
                )

    async def get_bucket_region(self, bucket_name):
        """This method return the name of region for a bucket.
        Args
            bucket_name (str): Name of bucket
        Returns:
            region: Name of region
        """
        region = None
        try:
            s3 = await self.client()
            response = await s3.get_bucket_location(
                Bucket=bucket_name,
            )
            region = response.get("LocationConstraint")
        except ClientError:
            self._logger.warning(f"Unable to fetch the region for {bucket_name}")

        return region


class S3AdvancedRulesValidator(AdvancedRulesValidator):
    RULES_OBJECT_SCHEMA_DEFINITION = {
        "type": "object",
        "properties": {
            "bucket": {"type": "string", "minLength": 1},
            "prefix": {"type": "string"},
            "extension": {"type": "array"},
        },
        "required": ["bucket"],
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
        try:
            S3AdvancedRulesValidator.SCHEMA(advanced_rules)
            return SyncRuleValidationResult.valid_result(
                rule_id=SyncRuleValidationResult.ADVANCED_RULES
            )
        except JsonSchemaValueException as e:
            return SyncRuleValidationResult(
                rule_id=SyncRuleValidationResult.ADVANCED_RULES,
                is_valid=False,
                validation_message=e.message,
            )


class S3DataSource(BaseDataSource):
    """Amazon S3"""

    name = "Amazon S3"
    service_type = "s3"
    advanced_rules_enabled = True

    def __init__(self, configuration):
        """Set up the connection to the Amazon S3.

        Args:
            configuration (DataSourceConfiguration): Object of DataSourceConfiguration class.
        """
        super().__init__(configuration=configuration)
        self.s3_client = S3Client(configuration=configuration)

    def _set_internal_logger(self):
        self.s3_client.set_logger(self._logger)

    def advanced_rules_validators(self):
        return [S3AdvancedRulesValidator(self)]

    async def ping(self):
        """Verify the connection with AWS"""
        try:
            await self.s3_client.fetch_buckets()
            self._logger.info("Successfully connected to AWS.")
        except Exception:
            self._logger.exception("Error while connecting to AWS.")
            raise

    async def format_document(self, bucket_name, bucket_object):
        """Prepare document for bucket object.

        Args:
            bucket_name: Name of bucket
            bucket_object: Response of bucket objects
        Returns:
            document: Modified document.
        """

        doc_id = hash_id(f"{bucket_name}/{bucket_object.key}")
        owner = await bucket_object.owner
        document = {
            "_id": doc_id,
            "filename": bucket_object.key,
            "size_in_bytes": await bucket_object.size,
            "bucket": bucket_name,
            "owner": owner.get("DisplayName") if owner else "",
            "storage_class": await bucket_object.storage_class,
            "_timestamp": (await bucket_object.last_modified).isoformat(),
        }
        return document

    async def advanced_sync(self, rule):
        async def process_object(obj_summary, s3_client):
            document = await self.format_document(
                bucket_name=bucket, bucket_object=obj_summary
            )
            return document, partial(
                self.get_content, doc=document, s3_client=s3_client
            )

        bucket = rule["bucket"]
        prefix = rule.get("prefix", "")
        async for obj_summary, s3_client in self.s3_client.get_bucket_objects(
            bucket=bucket, prefix=prefix
        ):
            if not rule.get("extension"):
                yield await process_object(obj_summary, s3_client)

            elif self.get_file_extension(obj_summary.key) in rule.get("extension", []):
                yield await process_object(obj_summary, s3_client)

    async def get_docs(self, filtering=None):
        """Get documents from Amazon S3

        Returns:
            dictionary: Document of file content

        Yields:
            dictionary: Document from Amazon S3.
        """
        if filtering and filtering.has_advanced_rules():
            for rule in filtering.get_advanced_rules():
                async for document, attachment in self.advanced_sync(rule=rule):
                    yield document, attachment

        else:
            bucket_list = await self.s3_client.get_bucket_list()
            for bucket in bucket_list:
                async for obj_summary, s3_client in self.s3_client.get_bucket_objects(
                    bucket=bucket
                ):
                    document = await self.format_document(
                        bucket_name=bucket, bucket_object=obj_summary
                    )
                    yield (
                        document,
                        partial(
                            self.get_content,
                            doc=document,
                            s3_client=s3_client,
                        ),
                    )

    async def get_content(self, doc, s3_client, timestamp=None, doit=None):
        if not (doit):
            return

        filename = doc["filename"]
        file_size = doc["size_in_bytes"]
        file_extension = self.get_file_extension(filename)
        if not self.can_file_be_downloaded(file_extension, filename, file_size):
            return

        bucket = doc["bucket"]
        document = {
            "_id": doc["id"],
            "_timestamp": doc["_timestamp"],
        }

        # s3 has a unique download method so we can't utilize
        # the generic download_and_extract_file func
        async with self.create_temp_file(file_extension) as async_buffer:
            await s3_client.download_fileobj(
                Bucket=bucket, Key=filename, Fileobj=async_buffer
            )
            await async_buffer.close()

            document = await self.handle_file_content_extraction(
                document, filename, async_buffer.name
            )

        return document

    async def close(self):
        """Closes unclosed client session"""
        await self.s3_client.close_client()

    @classmethod
    def get_default_configuration(cls):
        """Get the default configuration for Amazon S3.

        Returns:
            dictionary: Default configuration.
        """
        return {
            "buckets": {
                "display": "textarea",
                "label": "AWS Buckets",
                "order": 1,
                "tooltip": "AWS Buckets are ignored when Advanced Sync Rules are used.",
                "type": "list",
            },
            "aws_access_key_id": {
                "label": "AWS Access Key Id",
                "order": 2,
                "type": "str",
            },
            "aws_secret_access_key": {
                "label": "AWS Secret Key",
                "order": 3,
                "sensitive": True,
                "type": "str",
            },
            "s3_addressing_style": {
                "default_value": "auto",
                "display": "dropdown",
                "label": "S3 Bucket Addressing Style",
                "options": [
                    {"label": "Auto", "value": "auto"},
                    {"label": "Virtual", "value": "virtual"},
                    {"label": "Path", "value": "path"},
                ],
                "order": 4,
                "type": "str",
                "value": "auto",
                "tooltip": 'AWS users are recommended to use "Auto", S3-compatible users should refer to their provider.',
                "ui_restrictions": ["advanced"],
            },
            "s3_endpoint_url": {
                "label": "S3 Endpoint URL",
                "order": 5,
                "required": False,
                "type": "str",
                "tooltip": "AWS users are recommended to keep this blank, S3-compatible users should enter a complete URL from their provider.",
                "ui_restrictions": ["advanced"],
            },
            "aws_use_fips_endpoint": {
                "display": "toggle",
                "label": "Use FIPS 140-3 Endpoints",
                "order": 6,
                "tooltip": "Not available in all regions.",
                "type": "bool",
                "ui_restrictions": ["advanced"],
                "value": False,
            },
            "aws_use_dualstack_endpoint": {
                "display": "toggle",
                "label": "Use Dualstack Endpoints",
                "order": 7,
                "tooltip": "By default, only IPv4 is used. Enable to use dual IPv4/IPv6 endpoints.",
                "type": "bool",
                "ui_restrictions": ["advanced"],
                "value": False,
            },
            "read_timeout": {
                "default_value": DEFAULT_READ_TIMEOUT,
                "display": "numeric",
                "label": "Read timeout",
                "order": 4,
                "required": False,
                "type": "int",
                "ui_restrictions": ["advanced"],
            },
            "connect_timeout": {
                "default_value": DEFAULT_CONNECTION_TIMEOUT,
                "display": "numeric",
                "label": "Connection timeout",
                "order": 5,
                "required": False,
                "type": "int",
                "ui_restrictions": ["advanced"],
            },
            "max_attempts": {
                "default_value": DEFAULT_MAX_RETRY_ATTEMPTS,
                "display": "numeric",
                "label": "Maximum retry attempts",
                "order": 6,
                "required": False,
                "type": "int",
                "ui_restrictions": ["advanced"],
            },
            "page_size": {
                "default_value": DEFAULT_PAGE_SIZE,
                "display": "numeric",
                "label": "Maximum size of page",
                "order": 7,
                "required": False,
                "type": "int",
                "ui_restrictions": ["advanced"],
            },
            "use_text_extraction_service": {
                "display": "toggle",
                "label": "Use text extraction service",
                "order": 8,
                "tooltip": "Requires a separate deployment of the Elastic Text Extraction Service. Requires that pipeline settings disable text extraction.",
                "type": "bool",
                "ui_restrictions": ["advanced"],
                "value": False,
            },
        }
