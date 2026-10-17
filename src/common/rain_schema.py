"""Single source of truth for the Rain Forecast table columns: names, SQL types and descriptions.

Used to create Bronze tables with a description on the table and on every column, and to build
the explicit casts used when loading. Column meanings marked "Likely:" are our best reading of the
(undocumented) Kaggle dataset: ERA5 reanalysis variables and Himawari-8 satellite bands.
"""

import re

_ERA5 = "Likely ERA5 variable"
_HIMAWARI = "Likely Himawari-8 brightness temperature, K; 0 = missing; null when the source file has no satellite columns"

# (name, SQL type, description) in source-file column order.
DATA_COLUMNS = [
    ("row", "INT", "Grid row index (0-89) of the 90 x 250 grid. Part of the natural key."),
    ("col", "INT", "Grid column index (0-249) of the 90 x 250 grid. Part of the natural key."),
    ("year", "INT", "Observation year. Part of the natural key."),
    ("month", "INT", "Observation month (1-12). Part of the natural key."),
    ("day", "INT", "Observation day of month. Part of the natural key."),
    ("hour", "INT", "Observation hour of day (0-23). Part of the natural key."),
    ("CAPE", "DOUBLE", f"Convective available potential energy, J/kg. {_ERA5}."),
    ("CIN", "DOUBLE", f"Convective inhibition, J/kg; 9999 = no value. {_ERA5}."),
    ("EWSS", "DOUBLE", f"Eastward turbulent surface stress, N m-2 s. {_ERA5}."),
    ("IE", "DOUBLE", f"Moisture flux, kg m-2 s-1. {_ERA5}."),
    ("ISOR", "DOUBLE", f"Anisotropy of sub-gridscale orography (dimensionless). {_ERA5}."),
    ("KX", "DOUBLE", f"K index (thunderstorm potential), K. {_ERA5}."),
    ("PEV", "DOUBLE", f"Potential evaporation, m. {_ERA5}."),
    ("R250", "DOUBLE", f"Relative humidity at 250 hPa, %. {_ERA5}."),
    ("R500", "DOUBLE", f"Relative humidity at 500 hPa, %. {_ERA5}."),
    ("R850", "DOUBLE", f"Relative humidity at 850 hPa, %. {_ERA5}."),
    ("SLHF", "DOUBLE", f"Surface latent heat flux, J m-2. {_ERA5}."),
    ("SLOR", "DOUBLE", f"Slope of sub-gridscale orography (dimensionless). {_ERA5}."),
    ("SSHF", "DOUBLE", f"Surface sensible heat flux, J m-2. {_ERA5}."),
    ("TCLW", "DOUBLE", f"Total column cloud liquid water, kg m-2. {_ERA5}."),
    ("TCW", "DOUBLE", f"Total column water, kg m-2. {_ERA5}."),
    ("TCWV", "DOUBLE", f"Total column water vapour, kg m-2. {_ERA5}."),
    ("U250", "DOUBLE", f"Eastward wind component at 250 hPa, m/s. {_ERA5}."),
    ("U850", "DOUBLE", f"Eastward wind component at 850 hPa, m/s. {_ERA5}."),
    ("V250", "DOUBLE", f"Northward wind component at 250 hPa, m/s. {_ERA5}."),
    ("V850", "DOUBLE", f"Northward wind component at 850 hPa, m/s. {_ERA5}."),
    ("B09B", "DOUBLE", f"Band 9 (6.9 um, mid-level water vapour). {_HIMAWARI}."),
    ("B10B", "DOUBLE", f"Band 10 (7.3 um, low-level water vapour). {_HIMAWARI}."),
    ("B11B", "DOUBLE", f"Band 11 (8.6 um, cloud phase). {_HIMAWARI}."),
    ("B12B", "DOUBLE", f"Band 12 (9.6 um, ozone). {_HIMAWARI}."),
    ("B14B", "DOUBLE", f"Band 14 (11.2 um, longwave infrared window). {_HIMAWARI}."),
    ("B16B", "DOUBLE", f"Band 16 (13.3 um, CO2 / cloud-top height). {_HIMAWARI}."),
    ("I2B", "DOUBLE", f"IR2 channel (~12.4 um, dirty longwave window). {_HIMAWARI}."),
    ("IRB", "DOUBLE", f"IR1 channel (~10.4 um, clean longwave window). {_HIMAWARI}."),
    ("WVB", "DOUBLE", f"Water vapour channel (~6.2 um, upper-level water vapour). {_HIMAWARI}."),
    ("label", "INT", "Rain flag: 1 = rain, 0 = no rain (about 0.14% of rows are rain)."),
]

# Audit columns added by the Bronze loads.
AUDIT_COLUMNS = [
    ("source_file", "STRING", "Audit: name of the file the row was loaded from (from file metadata)."),
    ("source_file_path", "STRING", "Audit: full path of the file the row was loaded from (from file metadata)."),
    ("source_file_modified_dt", "TIMESTAMP", "Audit: last-modified time of the source file when it was loaded."),
    ("load_dt", "TIMESTAMP", "Audit: time the row was loaded into Bronze."),
    ("load_run_id", "STRING", "Audit: Databricks job run ID of the load, or 'manual' for a manual notebook run."),
    ("ingestion_method", "STRING", "Audit: how the row was ingested into Bronze, e.g. COPY_INTO."),
]

ALL_COLUMNS = DATA_COLUMNS + AUDIT_COLUMNS
NATURAL_KEY = ["row", "col", "year", "month", "day", "hour"]
SATELLITE_BANDS = ["B09B", "B10B", "B11B", "B12B", "B14B", "B16B", "I2B", "IRB", "WVB"]

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def column_names(columns=DATA_COLUMNS):
    return [name for name, _type, _desc in columns]


def quote_identifier(name: str) -> str:
    """Backtick-quote a simple identifier (needed for names like `row`); reject anything unusual."""
    if not _IDENTIFIER.match(name or ""):
        raise ValueError(f"Invalid identifier: {name!r}")
    return f"`{name}`"


def full_table_name(catalog: str, schema: str, table: str) -> str:
    return ".".join(quote_identifier(part) for part in (catalog, schema, table))


def sql_string(value: str) -> str:
    """A SQL string literal with backslashes and single quotes escaped."""
    return "'" + str(value).replace("\\", "\\\\").replace("'", "\\'") + "'"


def create_table_sql(table: str, table_comment: str, cluster_by, columns=ALL_COLUMNS) -> str:
    """CREATE TABLE IF NOT EXISTS with a comment on every column, the table comment and Liquid Clustering."""
    names = column_names(columns)
    unknown = [c for c in cluster_by if c not in names]
    if not cluster_by or unknown:
        raise ValueError(f"Invalid clustering columns {list(cluster_by)}; unknown: {unknown}")
    column_lines = ",\n".join(
        f"  {quote_identifier(name)} {sql_type} COMMENT {sql_string(desc)}" for name, sql_type, desc in columns
    )
    clustering = ", ".join(quote_identifier(c) for c in cluster_by)
    return (
        f"CREATE TABLE IF NOT EXISTS {table} (\n{column_lines}\n)\n"
        f"CLUSTER BY ({clustering})\n"
        f"COMMENT {sql_string(table_comment)}"
    )


def comment_sql(table: str, table_comment: str, columns=ALL_COLUMNS) -> list:
    """Statements that (re)apply the table and column descriptions, so an existing table stays in sync."""
    statements = [f"COMMENT ON TABLE {table} IS {sql_string(table_comment)}"]
    statements += [
        f"ALTER TABLE {table} ALTER COLUMN {quote_identifier(name)} COMMENT {sql_string(desc)}"
        for name, _type, desc in columns
    ]
    return statements
