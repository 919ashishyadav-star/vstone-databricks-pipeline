import json
from pathlib import Path

import pytest

from common.chunk_plan import find_chunk, load_chunk_plan, output_file_name, source_csv_for, validate_chunk_plan

REPO_ROOT = Path(__file__).resolve().parents[1]
SOURCES = [
    "combined_2019_04_01.csv",
    "combined_2019_04_02.csv",
    "combined_2019_10_01.csv",
    "combined_2020_04_01.csv",
    "combined_2020_10_01.csv",
]


def plan_with(*chunks):
    return {"source_file_pattern": "combined_*.csv", "chunks": list(chunks)}


def chunk(name, months, fmt="csv", method="copy", compression="none", **extra):
    return {"name": name, "months": months, "format": fmt, "method": method, "compression": compression, **extra}


VALID_PLAN = plan_with(
    chunk("chunk1_csv", ["2019-04"]),
    chunk("chunk2_csv", ["2019-10"]),
    chunk("chunk3_json", ["2020-04"], "json", "convert", "gzip"),
    chunk("chunk4_xml", ["2020-10"], "xml", "convert", "gzip", xml_root_tag="records", xml_row_tag="record"),
)


def test_valid_plan_assigns_every_file_to_one_chunk():
    assignment = validate_chunk_plan(VALID_PLAN, SOURCES)
    assert assignment == {
        "chunk1_csv": ["combined_2019_04_01.csv", "combined_2019_04_02.csv"],
        "chunk2_csv": ["combined_2019_10_01.csv"],
        "chunk3_json": ["combined_2020_04_01.csv"],
        "chunk4_xml": ["combined_2020_10_01.csv"],
    }


def test_repo_config_is_valid_for_all_four_months():
    plan = load_chunk_plan(str(REPO_ROOT / "src" / "config" / "chunk_plan.json"))
    assignment = validate_chunk_plan(plan, SOURCES)
    assert sorted(assignment) == ["chunk1_csv", "chunk2_csv", "chunk3_json", "chunk4_xml"]


@pytest.mark.parametrize(
    "plan, message",
    [
        (plan_with(chunk("a", ["2019-04", "2019-10"]), chunk("b", ["2019-10", "2020-04", "2020-10"])), "in both"),
        (plan_with(chunk("a", ["2019-04", "2019-10", "2020-04"])), "not assigned"),
        (plan_with(chunk("a", ["2019-04", "2019-10", "2020-04", "2020-10", "2021-01"])), "no source files for month 2021-01"),
        (plan_with(chunk("a", ["2019-04", "2019-10", "2020-04", "2020-10"], fmt="parquet")), "unsupported format"),
        (plan_with(chunk("a", ["2019-04", "2019-10", "2020-04", "2020-10"], fmt="json")), "'copy' only works for csv"),
        (plan_with(chunk("a", ["2019-04", "2019-10", "2020-04", "2020-10"], compression="gzip")), "compression must be 'none'"),
        (plan_with(chunk("a", ["2019-04", "2019-10", "2020-04", "2020-10"], fmt="xml", method="convert")), "xml_root_tag"),
        (plan_with(), "no chunks"),
    ],
)
def test_invalid_plans_are_rejected(plan, message):
    with pytest.raises(ValueError, match=message):
        validate_chunk_plan(plan, SOURCES)


def test_unexpected_source_file_name_is_rejected():
    with pytest.raises(ValueError, match="does not match the expected pattern"):
        validate_chunk_plan(VALID_PLAN, SOURCES + ["combined_2019_04.csv"])


@pytest.mark.parametrize(
    "fmt, compression, expected",
    [
        ("csv", "none", "combined_2019_04_01.csv"),
        ("json", "gzip", "combined_2019_04_01.json.gz"),
        ("xml", "gzip", "combined_2019_04_01.xml.gz"),
        ("json", "none", "combined_2019_04_01.json"),
    ],
)
def test_output_file_name_keeps_the_source_day(fmt, compression, expected):
    assert output_file_name("combined_2019_04_01.csv", fmt, compression) == expected


@pytest.mark.parametrize(
    "output, expected",
    [
        ("combined_2020_10_01.xml.gz", "combined_2020_10_01.csv"),
        ("combined_2020_04_30.json.gz", "combined_2020_04_30.csv"),
        ("combined_2019_04_27.csv", "combined_2019_04_27.csv"),
        ("combined_test_bad.xml", None),
    ],
)
def test_source_csv_for(output, expected):
    assert source_csv_for(output) == expected


def test_find_chunk_returns_the_xml_row_tag_from_the_repo_config():
    plan = load_chunk_plan(str(REPO_ROOT / "src" / "config" / "chunk_plan.json"))
    assert find_chunk(plan, "chunk4_xml")["xml_row_tag"] == "record"
    with pytest.raises(ValueError):
        find_chunk(plan, "chunk9")


def test_load_chunk_plan_reads_json(tmp_path):
    path = tmp_path / "plan.json"
    path.write_text(json.dumps(VALID_PLAN), encoding="utf-8")
    assert load_chunk_plan(str(path)) == VALID_PLAN
