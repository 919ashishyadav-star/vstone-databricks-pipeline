"""Settings for loading a chunk into a Bronze table with Auto Loader.

The schema is explicit (no inference, no evolution). Values that do not fit it are kept in
`_rescued_data` instead of being dropped. The checkpoint remembers which files were loaded, so
running again only picks up new files.
"""

import re

from common.bronze_audit import audit_expressions
from common.rain_schema import DATA_COLUMNS, RESCUED_COLUMN, quote_identifier

INGESTION_METHOD = "AUTO_LOADER"
RESCUED_DATA_COLUMN = RESCUED_COLUMN[0]
SUPPORTED_FORMATS = {"json", "csv", "xml", "parquet"}
_PATH_PART = re.compile(r"^[A-Za-z0-9_][A-Za-z0-9_.-]*$")


def volume_path(catalog: str, schema: str, volume: str, *parts: str) -> str:
    """/Volumes/<catalog>/<schema>/<volume>/<parts...> with every part validated."""
    for name in (catalog, schema, volume):
        quote_identifier(name)  # raises on anything that is not a simple identifier
    for part in parts:
        if not _PATH_PART.match(part or "") or ".." in part:
            raise ValueError(f"Invalid path part: {part!r}")
    return "/".join(["/Volumes", catalog, schema, volume, *parts])


def checkpoint_paths(catalog: str, schema: str, checkpoint_volume: str, table_name: str) -> tuple:
    """(checkpoint location, schema location) for one table: one checkpoint folder per table."""
    checkpoint = volume_path(catalog, schema, checkpoint_volume, table_name)
    return checkpoint, f"{checkpoint}/_schema"


def auto_loader_options(file_format: str, max_files_per_trigger, schema_location: str) -> dict:
    """Reader options for `spark.readStream.format("cloudFiles")`."""
    if file_format not in SUPPORTED_FORMATS:
        raise ValueError(f"Unsupported format {file_format!r}")
    max_files = int(max_files_per_trigger)
    if max_files < 1:
        raise ValueError("max_files_per_trigger must be at least 1")
    return {
        "cloudFiles.format": file_format,
        "cloudFiles.schemaLocation": schema_location,
        "cloudFiles.schemaEvolutionMode": "none",  # the schema is fixed; never add columns
        "cloudFiles.includeExistingFiles": "true",  # first run loads the files already there
        "cloudFiles.maxFilesPerTrigger": str(max_files),
        "rescuedDataColumn": RESCUED_DATA_COLUMN,
    }


def select_expressions(run_id: str, ingestion_method: str = INGESTION_METHOD, columns=DATA_COLUMNS) -> list:
    """Data columns (already typed by the explicit schema), audit columns, then `_rescued_data`."""
    return (
        [quote_identifier(n) for n, _t, _d in columns]
        + audit_expressions(run_id, ingestion_method)
        + [quote_identifier(RESCUED_DATA_COLUMN)]
    )
