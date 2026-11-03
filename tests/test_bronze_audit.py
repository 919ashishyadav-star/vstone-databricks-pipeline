import pytest

from common.bronze_audit import MANUAL_RUN_ID, audit_expressions, resolve_run_id
from common.copy_into import build_copy_into_sql
from common.rain_schema import AUDIT_COLUMNS, column_names


def test_audit_expressions_follow_audit_column_order():
    exprs = audit_expressions("42", "COPY_INTO")
    assert [e.rsplit(" AS ", 1)[1] for e in exprs] == [f"`{c}`" for c in column_names(AUDIT_COLUMNS)]
    assert "'42' AS `load_run_id`" in exprs and "'COPY_INTO' AS `ingestion_method`" in exprs


def test_audit_expressions_validate_run_id():
    with pytest.raises(ValueError):
        audit_expressions("bad id", "COPY_INTO")


def test_resolve_run_id_manual_default():
    assert resolve_run_id("") == MANUAL_RUN_ID == "manual"


def test_copy_into_uses_the_shared_audit_expressions():
    sql = build_copy_into_sql("`c`.`s`.`t`", "/Volumes/c/s/v/chunks/chunk1_csv", "42")
    for expr in audit_expressions("42", "COPY_INTO"):
        assert expr in sql
