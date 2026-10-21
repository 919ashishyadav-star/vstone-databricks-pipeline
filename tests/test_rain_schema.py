import pytest
from pyspark.sql import types as T

from common.rain_schema import (
    ALL_COLUMNS,
    AUDIT_COLUMNS,
    DATA_COLUMNS,
    NATURAL_KEY,
    RESCUED_COLUMN,
    SATELLITE_BANDS,
    bronze_columns,
    column_names,
    spark_schema,
    comment_sql,
    create_table_sql,
    full_table_name,
    quote_identifier,
    sql_string,
)

SOURCE_HEADER = (
    "row,col,year,month,day,hour,CAPE,CIN,EWSS,IE,ISOR,KX,PEV,R250,R500,R850,SLHF,SLOR,SSHF,"
    "TCLW,TCW,TCWV,U250,U850,V250,V850,B09B,B10B,B11B,B12B,B14B,B16B,I2B,IRB,WVB,label"
).split(",")
TABLE = "`workspace`.`vstone_dev_bronze`.`rain_forecast_chunk1_csv`"


def test_data_columns_match_the_source_header_order():
    assert column_names(DATA_COLUMNS) == SOURCE_HEADER


def test_types_int_for_keys_and_label_double_for_measurements():
    types = {name: sql_type for name, sql_type, _ in DATA_COLUMNS}
    assert all(types[c] == "INT" for c in NATURAL_KEY + ["label"])
    assert all(types[c] == "DOUBLE" for c in SOURCE_HEADER if c not in NATURAL_KEY + ["label"])


def test_every_column_has_a_description():
    assert all(desc and len(desc) > 10 for _n, _t, desc in ALL_COLUMNS)


def test_audit_columns_are_complete():
    assert column_names(AUDIT_COLUMNS) == [
        "source_file", "source_file_path", "source_file_modified_dt", "load_dt", "load_run_id", "ingestion_method",
    ]


def test_satellite_bands_and_natural_key_are_data_columns():
    names = column_names(DATA_COLUMNS)
    assert set(SATELLITE_BANDS) <= set(names) and set(NATURAL_KEY) <= set(names)


def test_quote_identifier_quotes_reserved_names_and_rejects_injection():
    assert quote_identifier("row") == "`row`"
    for bad in ["", "a b", "x`y", "t; DROP TABLE t", "1abc"]:
        with pytest.raises(ValueError):
            quote_identifier(bad)


def test_full_table_name():
    assert full_table_name("workspace", "vstone_dev_bronze", "rain_forecast_chunk1_csv") == TABLE


def test_sql_string_escapes_quotes_and_backslashes():
    assert sql_string("it's a \\ path") == "'it\\'s a \\\\ path'"


def test_create_table_sql_has_every_column_comment_and_clustering():
    sql = create_table_sql(TABLE, "Bronze test table", ["day", "hour"])
    assert sql.startswith(f"CREATE TABLE IF NOT EXISTS {TABLE} (")
    assert "CLUSTER BY (`day`, `hour`)" in sql
    assert sql.rstrip().endswith("COMMENT 'Bronze test table'")
    for name, sql_type, _desc in ALL_COLUMNS:
        assert f"`{name}` {sql_type} COMMENT '" in sql


@pytest.mark.parametrize("cluster_by", [[], ["day", "not_a_column"]])
def test_create_table_sql_rejects_bad_clustering(cluster_by):
    with pytest.raises(ValueError):
        create_table_sql(TABLE, "t", cluster_by)


def test_bronze_columns_add_rescued_data_only_when_asked():
    assert bronze_columns() == ALL_COLUMNS
    with_rescued = bronze_columns(include_rescued=True)
    assert with_rescued[:-1] == ALL_COLUMNS and with_rescued[-1] == RESCUED_COLUMN
    assert RESCUED_COLUMN[0] == "_rescued_data" and len(RESCUED_COLUMN[2]) > 10


def test_create_table_sql_with_rescued_column():
    sql = create_table_sql(TABLE, "t", ["day", "hour"], bronze_columns(include_rescued=True))
    assert "`_rescued_data` STRING COMMENT '" in sql


def test_spark_schema_types():
    schema = spark_schema(ALL_COLUMNS)
    assert schema["load_dt"].dataType == T.TimestampType()
    assert schema["source_file"].dataType == T.StringType()
    assert all(f.nullable for f in schema)


def test_comment_sql_covers_table_and_every_column():
    statements = comment_sql(TABLE, "Bronze test table")
    assert statements[0] == f"COMMENT ON TABLE {TABLE} IS 'Bronze test table'"
    assert len(statements) == 1 + len(ALL_COLUMNS)
    assert all(s.startswith(f"ALTER TABLE {TABLE} ALTER COLUMN `") for s in statements[1:])
