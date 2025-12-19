import base64
import json
import os
from abc import ABC, abstractmethod
from typing import Dict, Type

import boto3
from bson import ObjectId, json_util
from hi_pyutils.config import AppConfig
from hi_pyutils.data_nature import DataNature, ProcessingStrategy
from hi_pyutils.enums import FileProcessingStatus
from hi_pyutils.llm_utils import ai_completion
from hi_pyutils.mongo import get_mongo_collection

import utils
from common import RetryableError, NonRetryableError, RETRYABLE_OPENAI_EXCEPTIONS
from utils import telegram_fast_log as tg_log
from . import prompt_resources
from .status_reporter import StatusReporter, guard_parent_step, guard_child_step

s3_client = boto3.client("s3")

STRATEGY_REGISTRY: Dict[ProcessingStrategy, Type["BaseStrategyImplementation"]] = {}

def register_strategy(kind: ProcessingStrategy):
    def deco(cls: Type[BaseStrategyImplementation]):
        STRATEGY_REGISTRY[kind] = cls
        return cls
    return deco

class BaseStrategyImplementation(ABC):
    """Template Method base: defines the processing pipeline skeleton.
    Concrete strategies override the small hook methods below.
    """

    def __init__(self, app_config: AppConfig, data_nature: DataNature):
        self._app_config = app_config
        self._data_nature = data_nature
        self._s3_bucket_name = os.environ["BUCKET_NAME"]
        uni_output_reqs = prompt_resources.load_universal_output_requirements()
        uni_tc_rules = prompt_resources.load_universal_text_case_rules()
        self._prompt = (
            self._data_nature.prompt
            .replace("{{UNIVERSAL_OUTPUT_REQUIREMENTS}}", uni_output_reqs)
            .replace("{{UNIVERSAL_TEXT_CASE_RULES}}", uni_tc_rules)
        )
        # Status reporter is used by children, injected here for convenience
        self._status_reporter = StatusReporter(self._app_config)

    # ---- Public API ----
    def process(self, sqs_message_body: dict, upload_obj: ObjectId):
        """Skeleton of the pipeline. Concrete classes decide whether to use parent or child guards
        and what each step actually does."""
        prepared = self.prepare_input(sqs_message_body)
        llm_output = self.call_llm(prepared)
        json_obj = self.transform_output(llm_output, sqs_message_body, upload_obj)
        self.persist(json_obj, sqs_message_body, upload_obj)

    # ---- Hooks to override in concrete strategies ----
    @abstractmethod
    def prepare_input(self, sqs_message_body: dict):
        """Prepare input for LLM (e.g., download from S3, base64-encode). Returns any object needed by call_llm()."""
        raise NotImplementedError

    @abstractmethod
    def call_llm(self, prepared):
        """Invoke LLM and return its raw output (string). Should raise RetryableError for retryable cases."""
        raise NotImplementedError

    @abstractmethod
    def transform_output(self, llm_output: str, sqs_message_body: dict, upload_obj: ObjectId) -> dict:
        """Parse and validate LLM output. Should raise NonRetryableError on structural issues."""
        raise NotImplementedError

    @abstractmethod
    def persist(self, json_obj: dict, sqs_message_body: dict, upload_obj: ObjectId):
        """Write the final structured object(s) to the DB."""
        raise NotImplementedError

    def _get_extraction_dest_table(self):
        spec_extraction_tbl = self._app_config.research.temp_extraction_table
        dest_table = spec_extraction_tbl if spec_extraction_tbl else self._data_nature.mongo_documents_collection
        return get_mongo_collection(
            db_name=self._app_config.data_storage.db_name,
            collection_name=dest_table,
        )


@register_strategy(ProcessingStrategy.ONE_FILE_ONE_OBJECT)
class OneTextFileToOneObject(BaseStrategyImplementation):
    """Reads a text-like file from S3, sends to LLM, stores ONE structured document under parent upload _id."""

    def process(self, sqs_message_body: dict, upload_obj: ObjectId):
        with guard_parent_step(
            status=self._status_reporter,
            upload_obj=upload_obj,
            on_retry_status=FileProcessingStatus.S3_READING_ERROR,
            on_fail_status=FileProcessingStatus.S3_READING_ERROR,
            step_name_for_logging="S3 reading",
        ):
            prepared = self.prepare_input(sqs_message_body)

        with guard_parent_step(
            status=self._status_reporter,
            upload_obj=upload_obj,
            on_retry_status=FileProcessingStatus.AI_EXTRACTION_RETRYABLE_ERROR,
            on_fail_status=FileProcessingStatus.AI_EXTRACTION_NON_RETRYABLE_ERROR,
            step_name_for_logging="LLM extraction",
        ):
            llm_output = self.call_llm(prepared)

        # Typically non-retryable for structural JSON issues
        with guard_parent_step(
            status=self._status_reporter,
            upload_obj=upload_obj,
            on_retry_status=FileProcessingStatus.AI_EXTRACTION_NON_RETRYABLE_ERROR,
            on_fail_status=FileProcessingStatus.AI_EXTRACTION_NON_RETRYABLE_ERROR,
            step_name_for_logging="JSON parsing",
        ):
            json_obj = self.transform_output(llm_output, sqs_message_body, upload_obj)

        with guard_parent_step(
            status=self._status_reporter,
            upload_obj=upload_obj,
            on_retry_status=FileProcessingStatus.STRUCT_DATA_WRITING_ERROR,
            on_fail_status=FileProcessingStatus.STRUCT_DATA_WRITING_ERROR,
            step_name_for_logging="JSON writing",
        ):
            self.persist(json_obj, sqs_message_body, upload_obj)

        self._status_reporter.ok_parent(upload_obj, FileProcessingStatus.AI_EXTRACTION_ENDED_SUCCESSFULLY)

    def prepare_input(self, sqs_message_body: dict) -> bytes:
        """Read S3 object and return base64-encoded content string."""
        key = sqs_message_body["key"]
        tg_log.s3_key = key
        try:
            response = s3_client.get_object(Bucket=self._s3_bucket_name, Key=key)
            return response["Body"].read()
        except Exception as e:
            # S3 reading problems are considered retryable (transient) in this pipeline
            raise RetryableError(cause_exception=e) from e

    def call_llm(self, file_binary: bytes) -> str:
        """Call completion with base64 input and return raw text output."""
        try:
            output = ai_completion(self._prompt, model=self._data_nature.model, file_binary=file_binary)
            return output
        except IndexError:
            raise NonRetryableError("No text answers from LLM! (IndexError)")
        except RETRYABLE_OPENAI_EXCEPTIONS as e:
            raise RetryableError(cause_exception=e) from e
        except Exception as e:
            raise NonRetryableError(cause_exception=e) from e

    def transform_output(self, llm_output: str, sqs_message_body: dict, upload_obj: ObjectId) -> dict:
        """Parse JSON, check for LLM-reported ERROR field."""
        try:
            json_obj = json_util.loads(llm_output)
        except json.JSONDecodeError as e:
            raise NonRetryableError(f"JSONDecodeError: {e}; llm output: {llm_output}")

        if isinstance(json_obj, dict) and "ERROR" in json_obj:
            raise NonRetryableError("LLM Error output: " + str(json_obj["ERROR"]))

        json_obj["_id"] = upload_obj
        json_obj["file_name_original"] = utils.get_upload_filename(upload_obj, self._app_config)
        return json_obj

    def persist(self, json_obj: dict, sqs_message_body: dict, upload_obj: ObjectId):
        documents_collection = self._get_extraction_dest_table()
        try:
            documents_collection.insert_one(json_obj)
        except Exception as e:
            # Persist problems are treated as retryable
            raise RetryableError(cause_exception=e) from e


@register_strategy(ProcessingStrategy.ONE_ROW_ONE_OBJECT)
class OneRowToOneObject(BaseStrategyImplementation):
    """
    Expects data from a table row (as {"column": "value"} pairs), sends it to an LLM with a strategy-specific prompt,
    saves it as a separate structured object, and reports a child reference via status update.
    """

    def process(self, sqs_message_body: dict, upload_obj: ObjectId):
        """Apply child-level guards with the source_name context (sheet:row)."""
        source_name = f"{sqs_message_body['sheet']}:{sqs_message_body['data_row']}"

        # Step 1: prepare prompt with PAIRS
        with guard_child_step(
            status=self._status_reporter,
            upload_obj=upload_obj,
            source_name=source_name,
            on_retry_status=FileProcessingStatus.AI_EXTRACTION_NON_RETRYABLE_ERROR,
            on_fail_status=FileProcessingStatus.AI_EXTRACTION_NON_RETRYABLE_ERROR,
            step_name_for_logging="prompt preparation",
        ):
            prepared_prompt = self.prepare_input(sqs_message_body)

        # Step 2: call LLM
        with guard_child_step(
            status=self._status_reporter,
            upload_obj=upload_obj,
            source_name=source_name,
            on_retry_status=FileProcessingStatus.AI_EXTRACTION_RETRYABLE_ERROR,
            on_fail_status=FileProcessingStatus.AI_EXTRACTION_NON_RETRYABLE_ERROR,
            step_name_for_logging="LLM extraction",
        ):
            llm_output = self.call_llm(prepared_prompt)

        # Step 3: transform/validate
        with guard_child_step(
            status=self._status_reporter,
            upload_obj=upload_obj,
            source_name=source_name,
            on_retry_status=FileProcessingStatus.AI_EXTRACTION_NON_RETRYABLE_ERROR,
            on_fail_status=FileProcessingStatus.AI_EXTRACTION_NON_RETRYABLE_ERROR,
            step_name_for_logging="JSON parsing",
        ):
            json_obj = self.transform_output(llm_output, sqs_message_body, upload_obj)

        # Step 4: persist
        with guard_child_step(
            status=self._status_reporter,
            upload_obj=upload_obj,
            source_name=source_name,
            on_retry_status=FileProcessingStatus.STRUCT_DATA_WRITING_ERROR,
            on_fail_status=FileProcessingStatus.STRUCT_DATA_WRITING_ERROR,
            step_name_for_logging="Mongo insert",
        ):
            inserted_id = self.persist(json_obj, sqs_message_body, upload_obj)

        # Report success for the child with produced object id
        self._status_reporter.ok_child(
            upload_obj,
            source_name=source_name,
            object_id=inserted_id,
            status=FileProcessingStatus.AI_EXTRACTION_ENDED_SUCCESSFULLY,
        )

    # ---- Hooks ----
    def prepare_input(self, sqs_message_body: dict) -> str:
        """Build prompt by injecting the PAIRS placeholder with row content."""
        pairs_text = str(sqs_message_body["item"])
        # Use base strategy prompt and replace {{PAIRS}} placeholder
        prompt = self._prompt.replace("{{PAIRS}}", pairs_text)
        return prompt

    def call_llm(self, prompt: str) -> str:
        """Calls AI"""
        try:
            return ai_completion(prompt, model=self._data_nature.model)
        except IndexError:
            raise NonRetryableError("No text answers from LLM! (IndexError)")
        except RETRYABLE_OPENAI_EXCEPTIONS as e:
            # Transient OpenAI-side errors → Retryable
            raise RetryableError(cause_exception=e) from e
        except Exception as e:
            # Everything else → NonRetryable
            raise NonRetryableError(cause_exception=e) from e

    def transform_output(self, llm_output: str, sqs_message_body: dict, upload_obj: ObjectId) -> dict:
        """Parse JSON and check for LLM-reported ERROR field."""
        try:
            json_output = json_util.loads(llm_output)
        except json.JSONDecodeError as e:
            # Keep raw output for diagnostics (you can truncate if needed)
            raise NonRetryableError(f"JSONDecodeError: {e}; llm output: {llm_output}")

        if isinstance(json_output, dict) and "ERROR" in json_output:
            raise NonRetryableError("LLM Error output: " + str(json_output["ERROR"]))

        json_output["file_name_original"] = utils.get_upload_filename(upload_obj, self._app_config)
        return json_output

    def persist(self, json_obj: dict, sqs_message_body: dict, upload_obj: ObjectId) -> ObjectId:
        """Insert a child document into the target collection and return its ObjectId."""
        collection = self._get_extraction_dest_table()
        try:
            new_id = ObjectId()
            json_obj["_id"] = new_id
            collection.insert_one(json_obj)
            return new_id
        except Exception as e:
            # Treat persistence problems as Retryable to requeue this row
            raise RetryableError(cause_exception=e) from e
