import gzip

import pytest
from pyspark.testing import assertDataFrameEqual

from common.rain_schema import AUDIT_COLUMNS, DATA_COLUMNS, column_names
from common.xml_loader import (
    CORRUPT_RECORD_COLUMN,
    INGESTION_METHOD,
    RESCUED_DATA_COLUMN,
    ChangedFileError,
    files_to_load,
    read_schema,
    replace_where_predicate,
    select_expressions,
    xml_read_options,
)

A, B, C = "combined_2020_10_01.xml.gz", "combined_2020_10_02.xml.gz", "combined_2020_10_03.xml.gz"


def test_ingestion_method_is_pyspark_xml():
    assert INGESTION_METHOD == "PYSPARK_XML"


def test_xml_read_options_are_permissive_with_both_quality_columns():
    assert xml_read_options("record") == {
        "rowTag": "record",
        "mode": "PERMISSIVE",
        "rescuedDataColumn": "_rescued_data",
        "columnNameOfCorruptRecord": "_corrupt_record",
    }


@pytest.mark.parametrize("tag", ["", "re cord", "<record>", "1record"])
def test_xml_read_options_reject_bad_row_tags(tag):
    with pytest.raises(ValueError):
        xml_read_options(tag)


def test_read_schema_is_data_columns_plus_corrupt_record():
    schema = read_schema()
    assert [f.name for f in schema] == column_names(DATA_COLUMNS) + [CORRUPT_RECORD_COLUMN]
    assert schema[CORRUPT_RECORD_COLUMN].dataType.simpleString() == "string"


def test_select_expressions_order():
    exprs = select_expressions("42")
    n = len(DATA_COLUMNS)
    assert exprs[:n] == [f"`{c}`" for c in column_names(DATA_COLUMNS)]
    assert [e.rsplit(" AS ", 1)[1] for e in exprs[n: n + len(AUDIT_COLUMNS)]] == [f"`{c}`" for c in column_names(AUDIT_COLUMNS)]
    assert "'PYSPARK_XML' AS `ingestion_method`" in exprs
    assert exprs[-2:] == [f"`{RESCUED_DATA_COLUMN}`", f"`{CORRUPT_RECORD_COLUMN}`"]


def test_first_run_loads_every_file():
    assert files_to_load({A: 1000, B: 2000}, {}) == {"load": [A, B], "skipped": [], "replace": False}


def test_rerun_skips_every_file():
    assert files_to_load({A: 1000, B: 2000}, {A: 1000, B: 2000}) == {"load": [], "skipped": [A, B], "replace": False}


def test_only_new_files_are_loaded():
    assert files_to_load({A: 1000, B: 2000, C: 3000}, {A: 1000, B: 2000})["load"] == [C]


def test_sub_second_differences_are_not_changes():
    assert files_to_load({A: 1_000_999}, {A: 1_000_000})["load"] == []


def test_changed_file_stops_the_run():
    with pytest.raises(ChangedFileError, match="mode = replace"):
        files_to_load({A: 5000, B: 2000}, {A: 1000, B: 2000})


def test_replace_mode_reloads_chosen_files_or_all():
    assert files_to_load({A: 5000, B: 2000}, {A: 1000, B: 2000}, "replace", [A]) == {
        "load": [A], "skipped": [B], "replace": True,
    }
    assert files_to_load({A: 1, B: 2}, {A: 1}, "replace")["load"] == [A, B]


def test_replace_mode_rejects_files_not_in_the_folder():
    with pytest.raises(ValueError, match="not in the chunk folder"):
        files_to_load({A: 1}, {}, "replace", [C])


@pytest.mark.parametrize("mode", ["append", "DROPMALFORMED", ""])
def test_unknown_mode_is_rejected(mode):
    with pytest.raises(ValueError):
        files_to_load({A: 1}, {}, mode)


def test_replace_where_predicate_quotes_file_names():
    assert replace_where_predicate([B, A]) == f"source_file IN ('{A}', '{B}')"


@pytest.mark.parametrize("files", [[], ["a'; DROP TABLE t; --"], ["../x.xml"]])
def test_replace_where_predicate_rejects_bad_input(files):
    with pytest.raises(ValueError):
        replace_where_predicate(files)


def test_xml_with_alphabetical_elements_loads_by_name_with_real_types(spark, tmp_path):
    """XML lesson: elements in any (here alphabetical) order land in the right typed columns."""
    names = column_names(DATA_COLUMNS)
    values = {n: "1.5" for n in names}
    values.update({"row": "7", "col": "3", "year": "2020", "month": "10", "day": "1", "hour": "4",
                   "label": "1", "B09B": "250.25", "CIN": "9999.0"})
    body = "".join(f"<{n}>{values[n]}</{n}>" for n in sorted(names, key=str.lower))
    with gzip.open(tmp_path / A, "wt", encoding="utf-8") as f:
        f.write(f"<records><record>{body}</record></records>")

    df = (
        spark.read.format("xml").options(**xml_read_options("record")).schema(read_schema())
        .load(str(tmp_path / A))
    )

    got = df.select("row", "col", "hour", "label", "B09B", "CIN", "CAPE")
    expected = spark.createDataFrame(
        [(7, 3, 4, 1, 250.25, 9999.0, 1.5)],
        "row int, col int, hour int, label int, B09B double, CIN double, CAPE double",
    )
    assertDataFrameEqual(got, expected)
    assert df.filter(df[CORRUPT_RECORD_COLUMN].isNotNull()).count() == 0
