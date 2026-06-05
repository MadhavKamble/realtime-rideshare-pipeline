from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession


def upsert_delta_table(spark: SparkSession, source_df: DataFrame, target_path: str, key_columns: list[str]) -> None:
    """Perform an upsert (MERGE) from source_df into the Delta table at target_path using key_columns.

    If the target path doesn't exist as a Delta table, this writes the source as a new Delta table.
    The function uses Delta Lake's Merge API when available and falls back to append/overwrite otherwise.
    """
    try:
        # Import here so code doesn't fail if delta isn't on the PYSPARK driver classpath at import time
        from delta.tables import DeltaTable

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
    except Exception:
        # Best-effort fallback: append the data so nothing is lost.
        source_df.write.format("delta").mode("append").save(target_path)
