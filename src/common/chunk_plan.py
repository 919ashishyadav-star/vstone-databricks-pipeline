"""Load and validate the chunk plan (which months go to which chunk, in which format)."""

import json

from common.source_reader import file_month

SUPPORTED_FORMATS = {"csv", "json", "xml"}
SUPPORTED_METHODS = {"copy", "convert"}
SUPPORTED_COMPRESSION = {"none", "gzip"}
FILE_EXTENSIONS = {"csv": ".csv", "json": ".json", "xml": ".xml"}
COMPRESSION_SUFFIX = {"none": "", "gzip": ".gz"}


def load_chunk_plan(path: str) -> dict:
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def validate_chunk_plan(plan: dict, source_file_names) -> dict:
    """Check the plan against the source files and return {chunk name: [source file names]}.

    Raises ValueError listing every problem found, so a bad config fails before any data is written.
    """
    errors = []
    chunks = plan.get("chunks") or []
    if not chunks:
        errors.append("The plan has no chunks.")

    files_by_month = {}
    for name in sorted(source_file_names):
        month = file_month(name)
        if month is None:
            errors.append(f"Source file name does not match the expected pattern: {name}")
        else:
            files_by_month.setdefault(month, []).append(name)

    seen_names, month_owner, assignment = set(), {}, {}
    for chunk in chunks:
        name = chunk.get("name")
        fmt = chunk.get("format")
        method = chunk.get("method")
        compression = chunk.get("compression", "none")

        if not name:
            errors.append(f"A chunk has no name: {chunk}")
            continue
        if name in seen_names:
            errors.append(f"Duplicate chunk name: {name}")
        seen_names.add(name)
        if fmt not in SUPPORTED_FORMATS:
            errors.append(f"{name}: unsupported format {fmt!r} (expected one of {sorted(SUPPORTED_FORMATS)})")
        if method not in SUPPORTED_METHODS:
            errors.append(f"{name}: unsupported method {method!r} (expected one of {sorted(SUPPORTED_METHODS)})")
        if method == "copy" and fmt != "csv":
            errors.append(f"{name}: method 'copy' only works for csv (source files are CSV)")
        if method == "copy" and compression != "none":
            errors.append(f"{name}: method 'copy' keeps files as-is, so compression must be 'none'")
        if compression not in SUPPORTED_COMPRESSION:
            errors.append(f"{name}: unsupported compression {compression!r}")
        if fmt == "xml" and not (chunk.get("xml_root_tag") and chunk.get("xml_row_tag")):
            errors.append(f"{name}: xml chunks need xml_root_tag and xml_row_tag")

        months = chunk.get("months") or []
        if not months:
            errors.append(f"{name}: no months listed")
        files = []
        for month in months:
            if month in month_owner:
                errors.append(f"Month {month} is in both {month_owner[month]} and {name}")
                continue
            month_owner[month] = name
            if month not in files_by_month:
                errors.append(f"{name}: no source files for month {month}")
            files += files_by_month.get(month, [])
        assignment[name] = sorted(files)

    unassigned = sorted(set(files_by_month) - set(month_owner))
    if unassigned:
        errors.append(f"Source months not assigned to any chunk (every file must reach Gold): {unassigned}")

    if errors:
        raise ValueError("Invalid chunk plan:\n- " + "\n- ".join(errors))
    return assignment


def output_file_name(source_file_name: str, fmt: str, compression: str = "none") -> str:
    """Fixed output name for a source day, e.g. combined_2020_04_01.csv -> combined_2020_04_01.json.gz."""
    if not source_file_name.endswith(".csv"):
        raise ValueError(f"Expected a .csv source file name, got {source_file_name!r}")
    stem = source_file_name[: -len(".csv")]
    return stem + FILE_EXTENSIONS[fmt] + COMPRESSION_SUFFIX[compression]
