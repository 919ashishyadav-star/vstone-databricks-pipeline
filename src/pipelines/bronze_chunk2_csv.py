"""Lakeflow Declarative Pipeline: Bronze chunk 2 (Oct 2019 CSV) as a streaming table.

How it works:
- A streaming table fed by Auto Loader. The pipeline keeps the checkpoint, so each update loads only
  files it has not seen before; a second update with no new files writes 0 rows.
- A full refresh empties the table and reads every file again. It is only allowed when the
  `reset_allowed` setting is true (dev); prod sets `pipelines.reset.allowed = false`.
- Explicit typed schema from `rain_schema`; values that do not fit are kept in `_rescued_data`.
- Expectations only WARN for data problems (Bronze keeps raw rows) and FAIL only if the load itself is
  broken (missing audit values). The checks job task after the pipeline fails the job on any warning.

All settings come from the pipeline `configuration` in resources/bronze_chunk2_pipeline.yml.
The pipeline's root_path is `src/`, so `common.*` is importable.
"""

from pyspark import pipelines as dp

from common.auto_loader import volume_path
from common.chunk_plan import find_chunk, load_chunk_plan
from common.declarative_pipeline import (
    FAIL_EXPECTATIONS,
    pipeline_run_id,
    reader_options,
    select_expressions,
    table_schema_ddl,
    warn_expectations,
)
from common.rain_schema import spark_schema

_SETTINGS = ["catalog", "bronze_schema", "raw_volume", "chunk_name", "table_name", "cluster_by",
             "max_files_per_trigger", "reset_allowed", "chunk_plan_path"]


def _setting(name: str) -> str:
    value = (spark.conf.get(name, "") or "").strip()  # noqa: F821 - `spark` is provided by the pipeline
    if not value:
        raise ValueError(f"Pipeline configuration {name!r} must not be empty")
    return value


settings = {name: _setting(name) for name in _SETTINGS}
if settings["reset_allowed"] not in ("true", "false"):
    raise ValueError("reset_allowed must be 'true' or 'false'")

source_dir = volume_path(settings["catalog"], settings["bronze_schema"], settings["raw_volume"],
                         "chunks", settings["chunk_name"])
months = find_chunk(load_chunk_plan(settings["chunk_plan_path"]), settings["chunk_name"])["months"]
cluster_by = [c.strip() for c in settings["cluster_by"].split(",") if c.strip()]
run_id = pipeline_run_id(
    spark.conf.get("pipelines.updateId", ""),  # noqa: F821 - update ID, if the runtime exposes it
    spark.conf.get("pipelines.id", ""),  # noqa: F821
)

table_comment = (
    f"Bronze: Rain Forecast {settings['chunk_name']} loaded as-is by a Lakeflow Declarative Pipeline "
    f"(Auto Loader) from {source_dir}. One row per grid cell per hour; natural key row, col, year, month, "
    "day, hour. Values kept exactly as in the source (CIN = 9999 and satellite band = 0 mean missing). "
    "Values that did not fit the schema are kept in _rescued_data. Owned by the pipeline: only its "
    "updates write to this table. Chunk 2 for this method is an assumption pending trainer confirmation (Q2)."
)


@dp.table(
    name=settings["table_name"],
    comment=table_comment,
    schema=table_schema_ddl(),
    cluster_by=cluster_by,
    table_properties={"pipelines.reset.allowed": settings["reset_allowed"]},
)
@dp.expect_all(warn_expectations(months))
@dp.expect_all_or_fail(FAIL_EXPECTATIONS)
def bronze_chunk2_csv():
    return (
        spark.readStream.format("cloudFiles")  # noqa: F821
        .options(**reader_options(settings["max_files_per_trigger"]))
        .schema(spark_schema())
        .load(source_dir)
        .selectExpr(*select_expressions(run_id))
    )
