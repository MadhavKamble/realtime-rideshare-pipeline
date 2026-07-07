"""Custom Airflow operators for Delta MERGE and VACUUM. Currently unused by active DAGs —
retained as reference implementations. Would require PySpark in the Airflow container to
activate, which contradicts the deltalake-only design decision."""
from __future__ import annotations

from typing import Any

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DeltaMergeOperator(BaseOperator):
    """Airflow operator that runs a PySpark Silver→Gold MERGE (upsert) on a Delta table.

    Parameters
    ----------
    source_table_path:
        Delta Lake path of the source (e.g. Silver rides table).
    target_table_path:
        Delta Lake path of the target (e.g. Gold zone_demand table).
    transform_callable:
        A callable ``(DataFrame) -> DataFrame`` applied to source before upsert.
        Import it as a dotted string (e.g. ``"processing.batch.silver_to_gold.build_hourly_kpis"``).
    key_columns:
        List of column names that uniquely identify a row in the target table.
    spark_app_name:
        Name shown in the Spark UI for this job.
    """

    template_fields = ("source_table_path", "target_table_path")

    @apply_defaults
    def __init__(
        self,
        source_table_path: str,
        target_table_path: str,
        transform_callable: str,
        key_columns: list[str],
        spark_app_name: str = "DeltaMergeOperator",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.source_table_path = source_table_path
        self.target_table_path = target_table_path
        self.transform_callable = transform_callable
        self.key_columns = key_columns
        self.spark_app_name = spark_app_name

    def execute(self, context: Any) -> None:
        from storage.spark_session import build_delta_spark
        from processing.batch.delta_utils import upsert_delta_table
        import importlib

        spark = build_delta_spark(self.spark_app_name)

        source_df = spark.read.format("delta").load(self.source_table_path)

        module_path, func_name = self.transform_callable.rsplit(".", 1)
        module = importlib.import_module(module_path)
        transform_fn = getattr(module, func_name)
        transformed_df = transform_fn(source_df)

        try:
            upsert_delta_table(spark, transformed_df, self.target_table_path, self.key_columns)
        except Exception:
            self.log.exception(
                "DeltaMergeOperator: MERGE failed for %s → %s on keys %s",
                self.source_table_path,
                self.target_table_path,
                self.key_columns,
            )
            raise
        self.log.info(
            "DeltaMergeOperator: merged %s → %s on keys %s",
            self.source_table_path,
            self.target_table_path,
            self.key_columns,
        )


class DeltaVacuumOperator(BaseOperator):
    """Airflow operator that runs VACUUM on a Delta table to reclaim old file versions.

    Parameters
    ----------
    table_path:
        Delta Lake path to vacuum.
    retention_hours:
        How many hours of history to keep. Delta default is 168 (7 days).
        Pass 0 only in dev — requires ``spark.databricks.delta.retentionDurationCheck.enabled=false``.
    """

    template_fields = ("table_path",)

    @apply_defaults
    def __init__(
        self,
        table_path: str,
        retention_hours: int = 168,
        spark_app_name: str = "DeltaVacuumOperator",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.table_path = table_path
        self.retention_hours = retention_hours
        self.spark_app_name = spark_app_name

    def execute(self, context: Any) -> None:
        from delta.tables import DeltaTable
        from storage.spark_session import build_delta_spark

        spark = build_delta_spark(self.spark_app_name)
        delta_table = DeltaTable.forPath(spark, self.table_path)
        delta_table.vacuum(self.retention_hours)
        self.log.info(
            "DeltaVacuumOperator: vacuumed %s (retention=%sh)",
            self.table_path,
            self.retention_hours,
        )
