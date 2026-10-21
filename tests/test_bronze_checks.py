import pytest

from common.bronze_checks import (
    audit_check_sql,
    compare_file_stats,
    duplicate_keys_sql,
    load_batches_sql,
    schema_problems,
    table_file_stats_sql,
    values_match,
)

TABLE = "`workspace`.`vstone_dev_bronze`.`t`"


def tstats(rows, label_sum, all_null=0, any_null=0, bad_label=0, **extra):
    return {"rows": rows, "label_sum": label_sum, "rows_all_bands_null": all_null,
            "rows_any_band_null": any_null, "rows_bad_label": bad_label, **extra}


GOOD_SOURCE = {"a.csv": (100, 3), "short.csv": (50, 1)}
GOOD_TABLE = {"a.csv": tstats(100, 3), "short.csv": tstats(50, 1, all_null=50, any_null=50)}
HAS_BANDS = {"a.csv": True, "short.csv": False}


def test_matching_stats_pass():
    rows, problems = compare_file_stats(GOOD_SOURCE, GOOD_TABLE, HAS_BANDS)
    assert problems == []
    assert rows == [
        ("a.csv", "yes", 100, 100, 3, 3, 0, "OK"),
        ("short.csv", "no", 50, 50, 1, 1, 50, "OK"),
    ]


@pytest.mark.parametrize(
    "source, table, message",
    [
        (GOOD_SOURCE, {**GOOD_TABLE, "a.csv": tstats(99, 3)}, "rows 99 != source 100"),
        (GOOD_SOURCE, {**GOOD_TABLE, "a.csv": tstats(100, 2)}, "sum(label) 2 != source 3"),
        (GOOD_SOURCE, {"short.csv": GOOD_TABLE["short.csv"]}, "a.csv: not loaded"),
        (GOOD_SOURCE, {**GOOD_TABLE, "extra.csv": tstats(1, 0)}, "extra.csv: in table but not in chunk folder"),
        (GOOD_SOURCE, {**GOOD_TABLE, "a.csv": tstats(100, 3, bad_label=2)}, "2 rows with null/invalid label"),
        (GOOD_SOURCE, {**GOOD_TABLE, "a.csv": tstats(100, 3, any_null=4)}, "4 rows with null bands"),
        (GOOD_SOURCE, {**GOOD_TABLE, "short.csv": tstats(50, 1, all_null=49, any_null=50)}, "bands not null for a file without"),
        (GOOD_SOURCE, {**GOOD_TABLE, "a.csv": tstats(100, 3, rows_rescued=1)}, "1 rows with rescued data"),
        (GOOD_SOURCE, {**GOOD_TABLE, "a.csv": tstats(100, 3, rows_all_data_null=1)}, "malformed"),
    ],
)
def test_each_problem_is_reported(source, table, message):
    rows, problems = compare_file_stats(source, table, HAS_BANDS)
    assert any(message in p for p in problems), problems
    assert any(r[-1] != "OK" for r in rows)


def test_value_sums_within_rounding_pass_and_real_differences_fail():
    table = {**GOOD_TABLE, "a.csv": tstats(100, 3, sum_CAPE=1234.5000000001, sum_B14B=99.0)}
    _rows, ok = compare_file_stats(GOOD_SOURCE, table, HAS_BANDS, {"a.csv": {"CAPE": 1234.5, "B14B": 99.0}})
    assert ok == []
    _rows, bad = compare_file_stats(GOOD_SOURCE, table, HAS_BANDS, {"a.csv": {"CAPE": 1234.6, "B14B": 99.0}})
    assert any("sum(CAPE)" in p for p in bad)


def test_corrupt_records_are_reported():
    table = {**GOOD_TABLE, "a.csv": tstats(100, 3, rows_corrupt=2)}
    _rows, problems = compare_file_stats(GOOD_SOURCE, table, HAS_BANDS)
    assert any("2 corrupt records" in p for p in problems)


def test_values_match():
    assert values_match(None, None) and not values_match(None, 1.0)
    assert values_match(1e6, 1e6 * (1 + 1e-12)) and not values_match(1.0, 1.001)


def test_schema_problems_detects_order_and_type_differences():
    cols = [("row", "INT", "d"), ("CAPE", "DOUBLE", "d")]
    assert schema_problems([("row", "int"), ("CAPE", "double")], cols) == []
    assert any("order" in p for p in schema_problems([("CAPE", "double"), ("row", "int")], cols))
    assert any("CAPE: type string" in p for p in schema_problems([("row", "int"), ("CAPE", "string")], cols))


def test_table_file_stats_sql_with_corrupt_and_sums():
    sql = table_file_stats_sql(TABLE, include_rescued=True, include_corrupt=True, sum_columns=["CAPE", "B14B"])
    assert "count_if(_corrupt_record IS NOT NULL) AS rows_corrupt" in sql
    assert "sum(`CAPE`) AS `sum_CAPE`" in sql and "sum(`B14B`) AS `sum_B14B`" in sql


def test_rescued_columns_are_optional_in_table_stats():
    _rows, problems = compare_file_stats(GOOD_SOURCE, GOOD_TABLE, HAS_BANDS)
    assert problems == []


def test_table_file_stats_sql_with_and_without_rescued():
    plain = table_file_stats_sql(TABLE)
    assert f"FROM {TABLE}" in plain and "GROUP BY source_file" in plain
    assert "rows_rescued" not in plain
    rescued = table_file_stats_sql(TABLE, include_rescued=True)
    assert "count_if(_rescued_data IS NOT NULL) AS rows_rescued" in rescued
    assert "AS rows_all_data_null" in rescued


def test_load_batches_sql_filters_this_run_and_groups_by_batch():
    sql = load_batches_sql(TABLE, "877938057802640", "2026-10-21 13:27:30.123")
    assert "WHERE load_run_id = '877938057802640'" in sql
    assert "load_dt >= CAST('2026-10-21 13:27:30.123' AS TIMESTAMP)" in sql
    assert "GROUP BY load_dt" in sql and "count(DISTINCT source_file) AS files" in sql


def test_duplicate_and_audit_sql():
    assert "HAVING count(*) > 1" in duplicate_keys_sql(TABLE)
    assert "`row`, `col`, `year`, `month`, `day`, `hour`" in duplicate_keys_sql(TABLE)
    assert "ingestion_method <> 'AUTO_LOADER'" in audit_check_sql(TABLE, "AUTO_LOADER")
