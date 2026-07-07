from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.utils import AnalysisException

from common.logging_config import get_logger

logger = get_logger(__name__)


def upsert_delta_table(
    spark: SparkSession,
    source_df: DataFrame,
    target_path: str,
    key_columns: list[str],
    force_append: bool = False,
) -> None:
    """Perform an upsert (MERGE) from source_df into the Delta table at target_path using key_columns.

    If the target path doesn't exist as a Delta table, this writes the source as a new Delta table.

    If force_append is True, skips the MERGE entirely and appends source_df to target_path. Callers
    must opt into this explicitly — it is not used as a fallback for a failed MERGE.

    Raises whatever schema-mismatch or concurrent-modification exception the MERGE failed with;
    callers must handle it (see run_gold_standalone.py / delta_operators.py for the pattern).
    """
    if force_append:
        source_df.write.format("delta").mode("append").save(target_path)
        return

    from delta.exceptions import DeltaConcurrentModificationException
    from delta.tables import DeltaTable

    try:
        # If target is an existing Delta table, perform a MERGE
        if DeltaTable.isDeltaTable(spark, target_path):
            delta_table = DeltaTable.forPath(spark, target_path)
            # build merge condition
            src_alias = "source"
            tgt_alias = "target"
            merge_cond = " AND ".join([f"{src_alias}.{c} = {tgt_alias}.{c}" for c in key_columns])

            (
                delta_table.alias(tgt_alias)
                .merge(source_df.alias(src_alias), merge_cond)
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )
        else:
            # target not present — write as a new table
            source_df.write.format("delta").mode("overwrite").save(target_path)
    except (AnalysisException, DeltaConcurrentModificationException):
        logger.exception(
            "MERGE failed for target_path=%s key_columns=%s — not falling back to append",
            target_path,
            key_columns,
        )
        raise
