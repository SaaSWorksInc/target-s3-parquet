"""S3Parquet target sink class, which handles writing streams."""


from typing import Dict, List, Optional
import awswrangler as wr
from pandas import DataFrame
from singer_sdk import PluginBase
from singer_sdk.sinks import BatchSink
import json
from target_s3_parquet.data_type_generator import (
    generate_tap_schema,
    generate_current_target_schema,
)
from target_s3_parquet.sanitizer import (
    get_specific_type_attributes,
    apply_json_dump_to_df,
    stringify_df,
)


from datetime import datetime

STARTED_AT = datetime.now()


class S3ParquetSink(BatchSink):
    """S3Parquet target sink class."""

    def __init__(
        self,
        target: PluginBase,
        stream_name: str,
        schema: Dict,
        key_properties: Optional[List[str]],
    ) -> None:
        super().__init__(target, stream_name, schema, key_properties)

        self.store_with_glue = self.config.get("store_with_glue")
        #self._glue_schema = self._get_glue_schema()
        self._schema = self._get_schema()

    def _get_glue_schema(self):
        catalog_params = {
            "database": self.config.get("athena_database"),
            "table": self.stream_name,
        }

        if wr.catalog.does_table_exist(**catalog_params):
            return wr.catalog.table(**catalog_params)
        else:
            return DataFrame()
        
    def _get_schema(self):
        schema = DataFrame()

        if self.config.get("store_with_glue"):
            #return self._get_glue_schema()
            schema = self._get_glue_schema()
        # want to read the schema from a parquet file 
        #awswrangler.s3.read_parquet_metadata
        #path = f"{self.config.get('s3_path')}/{self.stream_name}"
        path = f"{self.config.get('s3_path')}/{self.config.get('athena_database')}/{self.stream_name}"

        # NOTE: this will not pick up on the partition columns that is within the path
        metadata = wr.s3.read_parquet_metadata(
            path=path,
            dataset=True,
            sampling=0.25,
            dtype={'_sdc_started_at': 'double'},
            s3_additional_kwargs={'Metadata': {'_sw_account_id': 'string'}}
        ) # returns Tuple({column_name: column_type}, {partition_column: type})
        # if it reads a path that does not exist, it returns ({}, None)

        self.logger.info(f"look at metadata to see if it contains schema: {metadata}")
        # combine the tuples
        if metadata[0]:
            schema = metadata[0]

            if metadata[1]:
                schema = {**schema, **metadata[1]}
            self.logger.info(f"look at the merged schema: {schema}")

        return schema


    max_size = 10000  # Max records to write in one batch

    def process_batch(self, context: dict) -> None:
        """Write out any prepped records and return once fully written."""
        # Sample:
        # ------
        # client.upload(context["file_path"])  # Upload file
        # Path(context["file_path"]).unlink()  # Delete local copy

        df = DataFrame(context["records"])
        #partition_cols = ["_sdc_started_at"]

        # support addition of partition columns
        # if self.config.get("additional_partitions"):
        #     for key, value in self.config.get("additional_partitions").items():
        #         df[key] = value
        #         partition_cols.append(key)
    
        df["_sdc_started_at"] = STARTED_AT.timestamp()

        #current_schema = generate_current_target_schema(self._get_glue_schema())
        current_schema = generate_current_target_schema(self._get_schema())
        self.logger.info(f"the Current Schema: {current_schema}")
        tap_schema = generate_tap_schema(
            self.schema["properties"], only_string=self.config.get("stringify_schema")
        )

        dtype = {**current_schema, **tap_schema}

        if self.config.get("stringify_schema"):
            attributes_names = get_specific_type_attributes(
                self.schema["properties"], "object"
            )
            df_transformed = apply_json_dump_to_df(df, attributes_names)
            df = stringify_df(df_transformed)

        self.logger.debug(f"DType Definition: {dtype}")

        athena_database_folder = ""
        if self.config.get("store_with_glue"):
            athena_database_folder = f'/{self.config.get("athena_database")}'

        full_path = f"{self.config.get('s3_path')}{athena_database_folder}/{self.stream_name}"

        wr.s3.to_parquet(
            df=df,
            index=False,
            compression="gzip",
            dataset=True,
            path=full_path,
            database=self.config.get("athena_database") if self.store_with_glue else None,
            table=self.stream_name if self.store_with_glue else None,
            mode="append",
            partition_cols=["_sdc_started_at"], # should we make this optional to pass partitions in?
            schema_evolution=True,
            dtype=dtype,
        )

        self.logger.info(f"Uploaded {len(context['records'])}")

        context["records"] = []
