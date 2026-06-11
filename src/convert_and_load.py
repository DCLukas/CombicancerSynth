#!/usr/bin/env python3
"""Convert synthetic-generator CSV output into the parquet layout combicancer reads.

The generator emits one ``;``-separated CSV per generic SNDS table (PMSI names carry
the literal ``aa`` placeholder, e.g. ``T_MCOaaC.csv``; DCIR tables have no year).
combicancer instead reads **per-year parquet** files (``T_MCO22C.parquet``,
``ER_PRS_F_2022.parquet``, ...) named via its own ``add_year_to_table_name`` rule, and
joins everything on ``NUM_ENQ`` / ``NUM_ENQ_IDT``.

This module bridges the two, per table:
  * normalises every date column to ``dd/MM/yyyy`` (combicancer's parse format) and
    **remaps the year into combicancer's [MIN_YEAR, MAX_YEAR] window** so rows actually
    land in the requested years;
  * derives a consistent ``NUM_ENQ`` (and ``NUM_ENQ_IDT`` = ``NUM_ENQ``) from the native
    patient pseudonym, using ``patient_key_map.json`` (BEN_NIR_PSA / NIR_ANO_17 collapse
    directly; KI tables map BEN_IDT_ANO -> BEN_NIR_PSA via IR_BEN_R);
  * splits year-bearing tables (PMSI ``aa`` names + DCIR ``ER_`` tables) by year -- using
    the row's driving date where available, else round-robin -- and writes each shard to
    the combicancer per-year filename. Tables unioned downstream rejoin cleanly because
    the keys are preserved across shards.
  * materialises ``IR_PHA_R`` (a generator *nomenclature*, but a *table* for combicancer)
    from the nomenclature CSV.

Pure pandas/pyarrow — runs in the generator venv, no Spark or Azure dependencies.
"""
import argparse
import hashlib
import json
import os
import re
from datetime import datetime

import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(HERE)
RES_ROOT = os.path.join(HERE, "snds_resources")
SCHEMAS = os.path.join(RES_ROOT, "schemas")
NOMENC = os.path.join(
    REPO_ROOT, "synthetic-generator", "src", "resources", "nomenclatures"
)
DEFAULT_CSV_IN = os.path.join(HERE, "out", "generated_csv")
DEFAULT_OUT = os.path.join(HERE, "out", "final")

# Stay-identifying key per PMSI table. combicancer's OMOP modules join a stay's
# sub-tables to its stay/control table on these keys AND on YEAR (YEAR is literally part
# of the join key, e.g. stays_pmsi joins T_MCOaaC <-> T_MCOaaUM on [ETA_NUM, RSA_NUM,
# YEAR]). So every table of one stay must land in the SAME year shard. We therefore
# derive each PMSI row's year deterministically from a stable hash of its stay key
# (shared across the stay's tables via the generator's foreign keys), instead of from an
# independent per-table date.
PMSI_STAY_KEYS = {
    # MCO inpatient family  (visit_id = mco_YEAR_ETA_NUM_RSA_NUM)
    "T_MCOaaA": ["ETA_NUM", "RSA_NUM"], "T_MCOaaB": ["ETA_NUM", "RSA_NUM"],
    "T_MCOaaC": ["ETA_NUM", "RSA_NUM"], "T_MCOaaD": ["ETA_NUM", "RSA_NUM"],
    "T_MCOaaUM": ["ETA_NUM", "RSA_NUM"], "T_MCOaaDMIP": ["ETA_NUM", "RSA_NUM"],
    "T_MCOaaMED": ["ETA_NUM", "RSA_NUM"], "T_MCOaaMEDATU": ["ETA_NUM", "RSA_NUM"],
    "T_MCOaaMEDTHROMBO": ["ETA_NUM", "RSA_NUM"],
    # MCO ACE/external family  (visit_id = mco_YEAR_ace_ETA_NUM_SEQ_NUM)
    "T_MCOaaCSTC": ["ETA_NUM", "SEQ_NUM"], "T_MCOaaFBSTC": ["ETA_NUM", "SEQ_NUM"],
    "T_MCOaaFHSTC": ["ETA_NUM", "SEQ_NUM"], "T_MCOaaFLSTC": ["ETA_NUM", "SEQ_NUM"],
    "T_MCOaaFMSTC": ["ETA_NUM", "SEQ_NUM"], "T_MCOaaFPSTC": ["ETA_NUM", "SEQ_NUM"],
    # SSR inpatient family  (visit_id = ssr_YEAR_ETA_NUM_RHA_NUM)
    "T_SSRaaB": ["ETA_NUM", "RHA_NUM"], "T_SSRaaC": ["ETA_NUM", "RHA_NUM"],
    "T_SSRaaCCAM": ["ETA_NUM", "RHA_NUM"], "T_SSRaaCSARR": ["ETA_NUM", "RHA_NUM"],
    "T_SSRaaD": ["ETA_NUM", "RHA_NUM"], "T_SSRaaMED": ["ETA_NUM", "RHA_NUM"],
    "T_SSRaaMEDATU": ["ETA_NUM", "RHA_NUM"],
    # SSR ACE family
    "T_SSRaaCSTC": ["ETA_NUM", "SEQ_NUM"], "T_SSRaaFLSTC": ["ETA_NUM", "SEQ_NUM"],
    "T_SSRaaFMSTC": ["ETA_NUM", "SEQ_NUM"],
    # HAD family  (visit_id = had_YEAR_ETA_NUM_EPMSI_RHAD_NUM)
    "T_HADaaA": ["ETA_NUM_EPMSI", "RHAD_NUM"], "T_HADaaB": ["ETA_NUM_EPMSI", "RHAD_NUM"],
    "T_HADaaC": ["ETA_NUM_EPMSI", "RHAD_NUM"], "T_HADaaD": ["ETA_NUM_EPMSI", "RHAD_NUM"],
    "T_HADaaDMPA": ["ETA_NUM_EPMSI", "RHAD_NUM"], "T_HADaaDMPP": ["ETA_NUM_EPMSI", "RHAD_NUM"],
    "T_HADaaMED": ["ETA_NUM_EPMSI", "RHAD_NUM"], "T_HADaaMEDATU": ["ETA_NUM_EPMSI", "RHAD_NUM"],
    "T_HADaaMEDCHL": ["ETA_NUM_EPMSI", "RHAD_NUM"],
}

# Demographic columns IR_BEN_R_uniq carries (attached to flattened anchors downstream).
IR_BEN_R_UNIQ_COLS = [
    "NUM_ENQ", "NUM_ENQ_IDT", "BEN_NAI_ANN", "BEN_SEX_COD", "ORG_AFF_BEN",
    "BEN_DTE_MAJ", "BEN_DCD_MAJ", "BEN_RES_DPT", "BEN_RES_COM",
]

DATE_INPUT_FORMATS = ["%d/%m/%Y", "%d%b%Y:%H:%M:%S", "%d%b%Y", "%Y%m%d", "%Y-%m-%d"]
DATE_OUTPUT_FORMAT = "%d/%m/%Y"

# combicancer columns that the converter (not the generator) provides for IR_PHA_R
IR_PHA_R_COLUMNS = [
    "PHA_CIP_UCD",
    "PHA_ACT_CLA",
    "PHA_ACT_LIB",
    "PHA_PRS_IDE",
    "PHA_CIP_C13",
]


def add_year_to_table_name(table_name, year):
    """Port of combicancer's snds_utils_spark.add_year_to_table_name (no pyspark dep)."""
    year = str(year)
    if "aa" in table_name:  # PMSI -> 2-digit year
        yy = year[2:] if len(year) == 4 else year
        return table_name.replace("aa", yy)
    if table_name.startswith("ER_"):  # DCIR -> 4-digit year suffix
        return f"{table_name}_{year}"
    return table_name  # IR_*, KI_* : unchanged


def is_year_split(table):
    return ("aa" in table) or table.startswith("ER_")


def load_date_columns():
    """{table: [date columns]} from the built schemas (type date/datetime)."""
    out = {}
    for fn in os.listdir(SCHEMAS):
        if not fn.endswith(".json"):
            continue
        with open(os.path.join(SCHEMAS, fn), encoding="utf-8") as fh:
            schema = json.load(fh)
        cols = [f["name"] for f in schema["fields"] if f.get("type") in ("date", "datetime")]
        out[fn[:-5]] = cols
    return out


def remap_year(year, min_year, max_year):
    span = max_year - min_year + 1
    return min_year + (int(year) % span)


def parse_date(value):
    if value is None or value == "" or (isinstance(value, float) and pd.isna(value)):
        return None
    s = str(value).strip()
    for fmt in DATE_INPUT_FORMATS:
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    return None


def normalise_date_series(series, min_year, max_year):
    """Parse -> remap year into window -> reformat dd/MM/yyyy. Returns (out, years)."""
    out, years = [], []
    for v in series:
        dt = parse_date(v)
        if dt is None:
            out.append(None)
            years.append(None)
            continue
        y = remap_year(dt.year, min_year, max_year)
        # guard against invalid day (e.g. 29 Feb in a non-leap remap target)
        try:
            dt2 = dt.replace(year=y)
        except ValueError:
            dt2 = dt.replace(year=y, day=28)
        out.append(dt2.strftime(DATE_OUTPUT_FORMAT))
        years.append(y)
    return out, years


def build_patient_maps(csv_in):
    """Return {BEN_IDT_ANO -> BEN_NIR_PSA} from IR_BEN_R (NUM_ENQ == BEN_NIR_PSA);
    used to resolve KI tables that key patients on BEN_IDT_ANO."""
    ben = pd.read_csv(os.path.join(csv_in, "IR_BEN_R.csv"), sep=";", dtype=str)
    idt_to_numenq = {}
    if "BEN_IDT_ANO" in ben.columns and "BEN_NIR_PSA" in ben.columns:
        idt_to_numenq = dict(zip(ben["BEN_IDT_ANO"], ben["BEN_NIR_PSA"]))
    return idt_to_numenq


def add_num_enq(df, table, patient_key_map, idt_to_numenq):
    """Add consistent NUM_ENQ / NUM_ENQ_IDT to a table that links to a patient."""
    info = patient_key_map.get(table)
    if not info:
        return df
    if table == "IR_BEN_R":
        df["NUM_ENQ"] = df["BEN_NIR_PSA"]
    else:
        col, ref = info["col"], info["ref_col"]
        if col not in df.columns:
            return df
        if ref == "BEN_NIR_PSA":
            df["NUM_ENQ"] = df[col]
        else:  # BEN_IDT_ANO -> NUM_ENQ
            df["NUM_ENQ"] = df[col].map(idt_to_numenq)
    df["NUM_ENQ_IDT"] = df["NUM_ENQ"]
    return df


def write_parquet(df, out_dir, name):
    os.makedirs(out_dir, exist_ok=True)
    df.to_parquet(os.path.join(out_dir, f"{name}.parquet"), index=False)


def _stable_int(s):
    return int(hashlib.md5(s.encode("utf-8")).hexdigest()[:8], 16)


def assign_years(df, table, driver_years, min_year, max_year):
    """Return a per-row Series of years in [min_year, max_year].

    PMSI tables: hash the stay key so every table of one stay shares a year (YEAR is
    part of combicancer's join keys). DCIR/other date tables: use the driving date.
    Otherwise spread round-robin so every year shard is populated.
    """
    span = max_year - min_year + 1
    keys = PMSI_STAY_KEYS.get(table)
    if keys and all(k in df.columns for k in keys):
        joined = df[keys[0]].astype(str)
        for k in keys[1:]:
            joined = joined + "|" + df[k].astype(str)
        return joined.map(lambda s: min_year + (_stable_int(s) % span))
    if driver_years is not None and any(y is not None for y in driver_years):
        return pd.Series(
            [y if y is not None else min_year for y in driver_years], index=df.index
        )
    return pd.Series([min_year + (i % span) for i in range(len(df))], index=df.index)


def transform(csv_in, out_dir, min_year, max_year):
    """Convert the generator CSV tree into the single OMOP-module-ready parquet layout:
    PMSI year-shards as ``YY_<name>`` (with a 2-digit ``YEAR`` column), DCIR ``ER_`` files
    as per-year ``<name>_YYYY``, IR_*/KI_* whole, plus ``IR_PHA_R`` and ``IR_BEN_R_uniq``.
    The OMOP builders read this directly (see run_combicancer.py)."""
    date_cols = load_date_columns()
    with open(os.path.join(RES_ROOT, "patient_key_map.json"), encoding="utf-8") as fh:
        patient_key_map = json.load(fh)
    idt_to_numenq = build_patient_maps(csv_in)

    os.makedirs(out_dir, exist_ok=True)
    summary = {}
    ben_uniq = None

    csv_files = sorted(f for f in os.listdir(csv_in) if f.endswith(".csv"))
    for fn in csv_files:
        table = fn[:-4]
        df = pd.read_csv(os.path.join(csv_in, fn), sep=";", dtype=str)

        # date normalisation (rewrites to dd/MM/yyyy, years remapped into window)
        tcols = [c for c in date_cols.get(table, []) if c in df.columns]
        driver_years = None
        for c in tcols:
            normalised, years = normalise_date_series(df[c], min_year, max_year)
            df[c] = normalised
            if driver_years is None and (c == "EXE_SOI_DTD" or c == tcols[0]):
                driver_years = years

        df = add_num_enq(df, table, patient_key_map, idt_to_numenq)

        # IR_BEN_R_uniq is derived from IR_BEN_R (the combicancer flattening hub)
        if table == "IR_BEN_R":
            cols = [c for c in IR_BEN_R_UNIQ_COLS if c in df.columns]
            ben_uniq = df[cols].drop_duplicates(subset=["NUM_ENQ"])

        if not is_year_split(table):
            write_parquet(df, out_dir, add_year_to_table_name(table, min_year))  # IR_*/KI_* as-is
            summary[table] = {"rows": len(df), "shards": 1}
            continue

        ys = assign_years(df, table, driver_years, min_year, max_year)
        shards = 0
        for year, chunk in df.groupby(ys):
            name = add_year_to_table_name(table, year)
            # OMOP modules read PMSI as YY_<name> with a 2-digit YEAR column;
            # DCIR (ER_) per-year files are read directly.
            if "aa" in table:
                write_parquet(chunk.assign(YEAR=str(year)[2:]), out_dir, f"YY_{name}")
            else:
                write_parquet(chunk, out_dir, name)
            shards += 1
        summary[table] = {"rows": len(df), "shards": shards}

    # IR_PHA_R: a nomenclature in the generator, a table for combicancer
    materialise_ir_pha_r(out_dir)
    summary["IR_PHA_R"] = {"rows": "from nomenclature", "shards": 1}

    if ben_uniq is not None:
        write_parquet(ben_uniq, out_dir, "IR_BEN_R_uniq")
        summary["IR_BEN_R_uniq"] = {"rows": len(ben_uniq), "shards": 1}

    with open(os.path.join(out_dir, "_transform_summary.json"), "w") as fh:
        json.dump(summary, fh, indent=2)
    print(f"Transformed {len(csv_files)} tables -> {out_dir}")
    print(json.dumps(summary, indent=2))
    return summary


def materialise_ir_pha_r(out_dir):
    src = os.path.join(NOMENC, "DREES", "IR_PHA_R.csv")
    if not os.path.exists(src):
        print(f"WARNING: IR_PHA_R nomenclature not found at {src}")
        return
    df = pd.read_csv(src, sep=";", dtype=str)
    for c in IR_PHA_R_COLUMNS:
        if c not in df.columns:
            df[c] = None
    write_parquet(df, out_dir, "IR_PHA_R")


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--csv-in", default=DEFAULT_CSV_IN)
    p.add_argument("--out", default=DEFAULT_OUT)
    p.add_argument("--min-year", type=int, default=2010)
    p.add_argument("--max-year", type=int, default=2024)
    args = p.parse_args()
    transform(args.csv_in, args.out, args.min_year, args.max_year)
