#!/usr/bin/env python3
"""Drive the combicancer OMOP-isation on the synthetic data living in Azurite.

combicancer's own entrypoints are two Azure/Curie-coupled notebooks. Rather than
papermill those (they assume a real SNDS delivery: multi-shard ER_PRS_F, IR_BEN_R_ARC,
a NUM_ENQ_ANO/NUM_ENQ_IDT mapping table, etc.), we call combicancer's OMOP table
*modules directly* with the same transform plans the notebooks use, but reading from /
writing to our local Azurite via the shared :mod:`combicancer_env` wiring.

We control the synthetic data, so the Curie-specific preprocessing (twin removal, ARC
merge, ANO->IDT mapping, ER_PRS_F yearly fusion) is unnecessary: the generator already
emits clean IR_BEN_R with NUM_ENQ/NUM_ENQ_IDT and the converter writes the per-year
parquet combicancer expects.

Tables are built in dependency order (``READY_STEPS``); ``--steps all`` builds every one.
``person``/``causes_of_death`` need only the referential/CepiDC tables; the PMSI/DCIR
tables join to ``stays_pmsi`` (on ``visit_id``) or ``prestations`` (on ``dcir_key``),
which the converter's ``final`` layout makes available without a separate flatten stage.
"""
import argparse
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(HERE)
COMBI_ROOT = os.path.join(REPO_ROOT, "combicancer")
sys.path.insert(0, HERE)
sys.path.insert(0, COMBI_ROOT)  # so `import src...` and `import conf` resolve

import combicancer_env as env  # noqa: E402


# ---------------------------------------------------------------------------
# shared helpers
# ---------------------------------------------------------------------------

def _read_opt(spark, path):
    """Read a parquet glob, returning None if it matches nothing / errors / is empty."""
    try:
        df = spark.read.option("mergeSchema", "true").parquet(path)
        return df if len(df.head(1)) else None
    except Exception:
        return None


def _union(dfs):
    from functools import reduce
    dfs = [d for d in dfs if d is not None]
    if not dfs:
        return None
    return reduce(lambda a, b: a.unionByName(b), dfs)


def _visit_id(family, *key_cols, ace=False):
    """The PMSI visit/stay id column: <family>_<YEAR>[_ace]_<key>... — the convention
    shared by stays_pmsi and every table that joins to it."""
    from pyspark.sql.functions import col, lit, concat_ws
    parts = [lit(family), col("YEAR")] + ([lit("ace")] if ace else []) + [col(k) for k in key_cols]
    return concat_ws("_", *parts)


# The 9 DCIR flux columns, in the exact order prestations concatenates them: this is
# the contract that makes every DCIR sub-table's dcir_key join back to prestations.
DCIR_KEY_COLS = ["FLX_DIS_DTD", "FLX_TRT_DTD", "FLX_EMT_TYP", "FLX_EMT_NUM", "FLX_EMT_ORD",
                 "ORG_CLE_NUM", "DCT_ORD_NUM", "PRS_ORD_NUM", "REM_TYP_AFF"]


def _dcir_key():
    from pyspark.sql.functions import col, concat_ws
    return concat_ws("_", *[col(c) for c in DCIR_KEY_COLS])


def _write(df, path):
    """Collect a Spark DataFrame and write it as a single parquet file."""
    df.toPandas().to_parquet(path, index=False)


def _prestations(spark, path_output, *extra):
    """Read the whole prestations table (all years) for a DCIR dcir_key join.

    The notebook loops per year and reads `prestations.parquet/FLX_DIS_DTD=*<year>`,
    but our prestations are partitioned by the raw `dd/MM/yyyy` `FLX_DIS_DTD` (slashes
    don't glob cleanly). Joining on `dcir_key` is exact regardless of year — the key
    embeds `FLX_DIS_DTD` — so we read the whole (tiny) table once and join on it."""
    cols = ["dcir_key", "person_id"] + list(extra)
    return spark.read.parquet(path_output + "prestations.parquet").select(*cols).dropDuplicates()


# ---------------------------------------------------------------------------
# OMOP table builders
# ---------------------------------------------------------------------------

def build_person(spark, path_input, path_output, mode_write=True):
    from pyspark.sql.functions import col
    from src.combicancer_pipeline.person.person import (
        process_person,
        clean_data,
        keep_unique_patients,
    )
    from src.combicancer_pipeline.fonc import convert_to_date_type

    path_ir_ben_r = path_input + "IR_BEN_R.parquet"
    path_ki_cci_r = path_input + "KI_CCI_R.parquet"
    paths = [path_ir_ben_r, path_ki_cci_r]
    transform_plans = {
        path_ir_ben_r: [
            lambda df: clean_data(df),
            lambda df: convert_to_date_type(df, "BEN_DTE_MAJ"),
            lambda df: convert_to_date_type(df, "BEN_DCD_DTE"),
            lambda df: keep_unique_patients(df),
        ],
        path_ki_cci_r: [
            lambda df: df.select("NUM_ENQ_IDT", col("BEN_DCD_DTE").alias("DCD_KI")),
            lambda df: convert_to_date_type(df, "DCD_KI"),
        ],
    }
    df = process_person(paths, transform_plans, spark)
    if mode_write:
        _write(df, path_output + "person.parquet")
    return df


def build_causes_of_death(spark, path_input, path_output, mode_write=True):
    from pyspark.sql.functions import col, lit
    from src.combicancer_pipeline.causes_of_death.causes_of_death import process_death

    path_person = path_output + "person.parquet"
    path_ki_cci_r = path_input + "KI_CCI_R.parquet"
    path_ki_ecd_r = path_input + "KI_ECD_R.parquet"
    paths = [path_person, path_ki_cci_r, path_ki_ecd_r]
    transform_plans = {
        path_person: [lambda df: df.select("person_id")],
        path_ki_cci_r: [
            lambda df: df.select(
                "NUM_ENQ_IDT", col("DCD_CIM_COD").alias("cause_of_death")
            ).withColumn("cause_rank", lit("0"))
        ],
        path_ki_ecd_r: [
            lambda df: df.select(
                "NUM_ENQ_IDT",
                col("ECD_CIM_COD").alias("cause_of_death"),
                col("ECD_CAU_RNG").alias("cause_rank"),
            )
        ],
    }
    df = process_death(paths, transform_plans, spark)
    if mode_write:
        _write(df, path_output + "causes_of_death.parquet")
    return df


def build_prestations(spark, path_input, path_output, mode_write=True):
    from pyspark.sql.functions import col, to_date, coalesce
    from src.combicancer_pipeline.prestation.prestation import process_prestation

    path_er_prs_f = path_input + "ER_PRS_F_*.parquet"
    paths = [path_er_prs_f]
    dcir_key = _dcir_key()
    transform_plans = {
        path_er_prs_f: [
            lambda df: df.select(
                *DCIR_KEY_COLS,
                "NUM_ENQ_IDT", "EXE_SOI_DTD", "EXE_SOI_DTF",
                "PRS_NAT_REF", "PSE_SPE_COD", "PSE_ACT_NAT", "PSP_SPE_COD",
                "PRS_ACT_QTE", "PSP_ACT_NAT", "ETB_PRE_FIN",
            ),
            lambda df: df.select(
                col("FLX_DIS_DTD"),
                dcir_key.alias("dcir_key"),
                col("NUM_ENQ_IDT").alias("person_id"),
                to_date("EXE_SOI_DTD", "dd/MM/yyyy").alias("presta_start_date"),
                to_date("EXE_SOI_DTF", "dd/MM/yyyy").alias("presta_end_date"),
                col("PRS_NAT_REF").alias("prestation_type"),
                coalesce(col("PSE_SPE_COD"), col("PSE_ACT_NAT")).alias("specialty_performing_professional"),
                coalesce(col("PSP_SPE_COD"), col("PSP_ACT_NAT")).alias("specialty_prescribing_professional"),
                col("ETB_PRE_FIN").alias("finess_geo"),
                col("PRS_ACT_QTE").cast("integer").alias("quantity"),
            ),
        ]
    }
    df = process_prestation(paths, transform_plans, spark)
    if mode_write:
        _write(df, path_output + "prestations.parquet")
    return df


def build_stays_pmsi(spark, path_input, path_output, mode_write=True):
    """Build the PMSI visit (stays) table. Replicates notebook 2's stays step, but each
    source branch is guarded: branches whose input tables/columns are absent in the
    synthetic subset are skipped instead of failing the whole step."""
    from pyspark.sql.functions import col, to_date, lit, when

    def read(glob):
        return _read_opt(spark, path_input + glob)

    def stay_cols(df, stay_id):
        finess = when(col("ETA_NUM_GEO").isNotNull(), col("ETA_NUM_GEO")).otherwise(col("ETA_NUM")) \
            if "ETA_NUM_GEO" in df.columns else col("ETA_NUM")
        return (
            df.withColumn("finess_geo", finess)
            .withColumn("stay_start_date", to_date("EXE_SOI_DTD", "dd/MM/yyyy"))
            .withColumn("stay_end_date", to_date("EXE_SOI_DTF", "dd/MM/yyyy"))
            .withColumn("stay_id", stay_id)
            .select("stay_id", "finess_geo", col("NUM_ENQ_IDT").alias("person_id"),
                    "stay_start_date", "stay_end_date")
        )

    branches = []

    # MCO inpatient: C ⋈ UM on [ETA_NUM, RSA_NUM, YEAR]
    c, um = read("YY_T_MCO*C.parquet"), read("YY_T_MCO*UM.parquet")
    if c is not None and um is not None:
        c = c.select("ETA_NUM", "RSA_NUM", "EXE_SOI_DTD", "EXE_SOI_DTF", "NUM_ENQ_IDT", "YEAR").dropDuplicates()
        um = um.select("ETA_NUM", "RSA_NUM", "ETA_NUM_GEO", "UM_ORD_NUM", "RUM_ORD_NUM", "YEAR")
        j = c.join(um, on=["ETA_NUM", "RSA_NUM", "YEAR"]).filter(
            ((col("UM_ORD_NUM") == "1") & col("RUM_ORD_NUM").isNull())
            | (col("RUM_ORD_NUM") == "1")
        )
        branches.append(stay_cols(j, _visit_id("mco", "ETA_NUM", "RSA_NUM")))

    # MCO ACE: CSTC ⋈ FBSTC on [ETA_NUM, SEQ_NUM, YEAR]
    cstc, fbstc = read("YY_T_MCO*CSTC.parquet"), read("YY_T_MCO*FBSTC.parquet")
    if cstc is not None and fbstc is not None:
        cstc2 = cstc.select("ETA_NUM", "SEQ_NUM", "EXE_SOI_DTD", "EXE_SOI_DTF", "NUM_ENQ_IDT", "YEAR").dropDuplicates()
        fbstc2 = fbstc.select("ETA_NUM", "SEQ_NUM", "ETA_NUM_GEO", "YEAR").dropDuplicates()
        j = cstc2.join(fbstc2, on=["ETA_NUM", "SEQ_NUM", "YEAR"])
        branches.append(stay_cols(j, _visit_id("mco", "ETA_NUM", "SEQ_NUM", ace=True)))

    # SSR inpatient: C ⋈ B on [ETA_NUM, RHA_NUM, YEAR]
    sc, sb = read("YY_T_SSR*C.parquet"), read("YY_T_SSR*B.parquet")
    if sc is not None and sb is not None:
        sc = sc.select("ETA_NUM", "RHA_NUM", "EXE_SOI_DTD", "EXE_SOI_DTF", "NUM_ENQ_IDT", "YEAR").dropDuplicates()
        sbcols = ["ETA_NUM", "RHA_NUM", "YEAR"] + [x for x in ["ETA_NUM_GEO", "RHS_NUM"] if x in sb.columns]
        sb = sb.select(*sbcols)
        j = sc.join(sb, on=["ETA_NUM", "RHA_NUM", "YEAR"])
        if "RHS_NUM" in j.columns:
            j = j.filter(col("RHS_NUM") == "001")
        branches.append(stay_cols(j, _visit_id("ssr", "ETA_NUM", "RHA_NUM")))

    # HAD: C ⋈ B on [ETA_NUM_EPMSI, RHAD_NUM, YEAR]
    hc, hb = read("YY_T_HAD*C.parquet"), read("YY_T_HAD*B.parquet")
    if hc is not None and hb is not None:
        hc = hc.select("ETA_NUM_EPMSI", "RHAD_NUM", "EXE_SOI_DTD", "EXE_SOI_DTF",
                       "NUM_ENQ_IDT", "YEAR",
                       *([c2 for c2 in ["ETA_NUM_GEO", "ETA_NUM_TWO"] if c2 in hc.columns]))
        hb = hb.select("ETA_NUM_EPMSI", "RHAD_NUM", "YEAR").dropDuplicates()
        j = hc.join(hb, on=["ETA_NUM_EPMSI", "RHAD_NUM", "YEAR"])
        finess = when(col("ETA_NUM_GEO").isNotNull(), col("ETA_NUM_GEO")).otherwise(col("ETA_NUM_TWO")) \
            if "ETA_NUM_GEO" in j.columns and "ETA_NUM_TWO" in j.columns else lit(None)
        j = (j.withColumn("finess_geo", finess)
             .withColumn("stay_start_date", to_date("EXE_SOI_DTD", "dd/MM/yyyy"))
             .withColumn("stay_end_date", to_date("EXE_SOI_DTF", "dd/MM/yyyy"))
             .withColumn("stay_id", _visit_id("had", "ETA_NUM_EPMSI", "RHAD_NUM"))
             .select("stay_id", "finess_geo", col("NUM_ENQ_IDT").alias("person_id"), "stay_start_date", "stay_end_date"))
        branches.append(j)

    df_union = _union(branches)
    if df_union is None:
        raise RuntimeError("stays_pmsi: no usable PMSI source branch found")
    df_person = spark.read.parquet(path_output + "person.parquet").select("person_id")
    df_final = df_union.join(df_person, "person_id").dropDuplicates()
    if mode_write:
        _write(df_final, path_output + "stays_pmsi.parquet")
    return df_final


def build_procedure_pmsi(spark, path_input, path_output, mode_write=True):
    """PMSI procedures (CCAM/CSARR), joined to stays_pmsi on visit_id. Replicates
    notebook 2's MCO/SSR branches via combicancer's procedure.py transforms; each branch
    is guarded so a missing source table just drops that branch."""
    from pyspark.sql.functions import col, lit
    from src.combicancer_pipeline.procedure import procedure as P

    stays = spark.read.parquet(path_output + "stays_pmsi.parquet").select(
        "person_id", col("stay_id").alias("visit_id"), "stay_start_date",
        "stay_end_date", "finess_geo")

    def common(df, code, voc, src):
        return P.create_pmsi_procedure_common_cols(df, code, voc, src)

    def finish(df, delay_col, qty_col=None, fmstc=False):
        df = (df.transform(
                lambda d: P.create_date_origin_for_fmstc(d, delay_col) if fmstc
                else P.create_date_origin(d, delay_col))
              .transform(lambda d: P.create_delay(d, delay_col))
              .transform(P.calculate_procedure_start_date)
              .transform(P.create_procedure_id)
              .transform(P.create_procedure_end_date))
        if qty_col:
            df = df.transform(lambda d: P.create_quantity(d, qty_col))
        else:
            df = df.withColumn("quantity", lit(1).cast("int"))
        return df.transform(P.sum_lines).transform(P.final_select)

    branches = []
    # MCO inpatient (T_MCOaaA)
    a = _read_opt(spark, path_input + "YY_T_MCO*A.parquet")
    if a is not None:
        a = (common(a, "CDC_ACT", "CCAM", "MCO")
             .filter(col("PHA_ACT").isin(["0", "1"])).filter(col("ACV_ACT") == 1)
             .withColumn("visit_id", _visit_id("mco", "ETA_NUM", "RSA_NUM"))
             .join(stays, "visit_id"))
        branches.append(finish(a, "ENT_DAT_DEL", "NBR_EXE_ACT"))
    # MCO ACE (T_MCOaaFMSTC)
    fm = _read_opt(spark, path_input + "YY_T_MCO*FMSTC.parquet")
    if fm is not None:
        fm = (common(fm, "CCAM_COD", "CCAM", "MCO ACE")
              .filter(col("PHA_ACT").isin(["0", "1"])).filter(col("ACV_ACT") == 1)
              .withColumn("visit_id", _visit_id("mco", "ETA_NUM", "SEQ_NUM", ace=True))
              .join(stays, "visit_id"))
        branches.append(finish(fm, "DEL_DAT_ENT", fmstc=True))
    # SSR CCAM
    cc = _read_opt(spark, path_input + "YY_T_SSR*CCAM.parquet")
    if cc is not None:
        cc = (common(cc, "CCAM_ACT", "CCAM", "SSR")
              .filter(col("CCAM_PHA_ACT").isin(["0", "1"])).filter(col("CCAM_COD_ACT") == 1)
              .withColumn("visit_id", _visit_id("ssr", "ETA_NUM", "RHA_NUM"))
              .join(stays, "visit_id"))
        branches.append(finish(cc, "CCAM_DEL_ENT_UM", "CCAM_NBR_REA"))
    # SSR CSARR
    cs = _read_opt(spark, path_input + "YY_T_SSR*CSARR.parquet")
    if cs is not None:
        cs = (common(cs, "CSARR_COD", "CSARR", "SSR")
              .withColumn("visit_id", _visit_id("ssr", "ETA_NUM", "RHA_NUM"))
              .join(stays, "visit_id"))
        branches.append(finish(cs, "ENT_DAT_DEL_UM", "NBR_CSARR"))

    df = _union(branches)
    if df is None:
        raise RuntimeError("procedure_pmsi: no usable source branch")
    df = df.dropDuplicates()
    if mode_write:
        _write(df, path_output + "procedure_pmsi.parquet")
    return df


def build_condition_dcir(spark, path_input, path_output, mode_write=True):
    """DCIR conditions (ALD) from IR_IMB_R, restricted to clean patients (person)."""
    from pyspark.sql.functions import col, md5, lit, first, last, concat_ws, to_date
    from pyspark.sql.window import Window

    imb = spark.read.parquet(path_input + "IR_IMB_R.parquet").select(
        "NUM_ENQ_IDT", "IMB_ALD_DTD", "IMB_ALD_DTF", "UPD_DTE", "MED_MTF_COD",
        "IMB_ETM_NAT").dropDuplicates()
    person = spark.read.parquet(path_output + "person.parquet").select(
        col("person_id").alias("NUM_ENQ_IDT"))
    df = imb.join(person, "NUM_ENQ_IDT")
    df = (df.withColumn("IMB_ALD_DTD", to_date("IMB_ALD_DTD", "dd/MM/yyyy"))
            .withColumn("IMB_ALD_DTF", to_date("IMB_ALD_DTF", "dd/MM/yyyy"))
            .withColumn("UPD_DTE", to_date("UPD_DTE", "dd/MM/yyyy")))
    w = (Window.partitionBy("NUM_ENQ_IDT", "MED_MTF_COD").orderBy(col("UPD_DTE").desc())
         .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing))
    df = (df.withColumn("condition_end_date", first("IMB_ALD_DTF").over(w))
            .withColumn("condition_start_date", last("IMB_ALD_DTD").over(w))
            .dropDuplicates(["NUM_ENQ_IDT", "MED_MTF_COD", "condition_end_date"]))
    out = df.select(
        md5(concat_ws("_", col("NUM_ENQ_IDT"), col("IMB_ALD_DTD"), col("MED_MTF_COD"))).alias("condition_id"),
        col("NUM_ENQ_IDT").alias("person_id"), "condition_start_date", "condition_end_date",
        col("MED_MTF_COD").alias("condition_code"), lit("CIM10").alias("condition_vocabulary"),
        lit(None).cast("string").alias("finess"), lit(None).cast("string").alias("visit_id"),
        col("IMB_ETM_NAT").alias("condition_type"), lit("DCIR").alias("condition_source"))
    if mode_write:
        _write(out, path_output + "condition_dcir.parquet")
    return out


def build_condition_pmsi(spark, path_input, path_output, mode_write=True):
    """PMSI diagnoses (principal/relié/associé) from MCO/SSR/HAD, joined to stays."""
    from pyspark.sql.functions import col, md5, lit, concat_ws, to_date

    stays = spark.read.parquet(path_output + "stays_pmsi.parquet").select(
        "person_id", "stay_start_date", "stay_end_date",
        col("stay_id").alias("visit_id"), "finess_geo")

    def diag(df, code_col, source, ctype, vid):
        return (df.withColumn("visit_id", vid)
                .select("visit_id", col(code_col).alias("condition_code"),
                        lit(source).alias("condition_source"), lit(ctype).alias("condition_type")))

    mco_vid = _visit_id("mco", "ETA_NUM", "RSA_NUM")
    ssr_vid = _visit_id("ssr", "ETA_NUM", "RHA_NUM")
    had_vid = _visit_id("had", "ETA_NUM_EPMSI", "RHAD_NUM")

    parts = []
    mb = _read_opt(spark, path_input + "YY_T_MCO*B.parquet")
    if mb is not None:
        parts.append(diag(mb, "DGN_PAL", "MCO", "Diagnostic principal", mco_vid))
        if "DGN_REL" in mb.columns:
            parts.append(diag(mb, "DGN_REL", "MCO", "Diagnostic relié", mco_vid))
    md = _read_opt(spark, path_input + "YY_T_MCO*D.parquet")
    if md is not None:
        parts.append(diag(md, "ASS_DGN", "MCO", "Diagnostic associé", mco_vid))
    sd = _read_opt(spark, path_input + "YY_T_SSR*D.parquet")
    if sd is not None:
        parts.append(diag(sd, "DGN_COD", "SSR", "Diagnostic associé", ssr_vid))
    hb = _read_opt(spark, path_input + "YY_T_HAD*B.parquet")
    if hb is not None:
        parts.append(diag(hb, "DGN_PAL", "HAD", "Diagnostic principal", had_vid))
    hd = _read_opt(spark, path_input + "YY_T_HAD*D.parquet")
    if hd is not None:
        parts.append(diag(hd, "DGN_ASS", "HAD", "Diagnostic associé", had_vid))

    u = _union(parts)
    if u is None:
        raise RuntimeError("condition_pmsi: no usable source branch")
    j = u.join(stays, "visit_id").filter(col("condition_code").isNotNull())
    out = j.select(
        md5(concat_ws("_", col("person_id"), col("stay_start_date"), col("condition_code"))).alias("condition_id"),
        "person_id",
        to_date("stay_start_date", "dd/MM/yyyy").alias("condition_start_date"),
        to_date("stay_end_date", "dd/MM/yyyy").alias("condition_end_date"),
        "condition_code", lit("CIM-10").alias("condition_vocabulary"),
        "finess_geo", "visit_id", "condition_type", "condition_source").dropDuplicates()
    if mode_write:
        _write(out, path_output + "condition_pmsi.parquet")
    return out


def build_measurement_pmsi(spark, path_input, path_output, mode_write=True):
    """PMSI biology measurements (NABM) from FLSTC tables, joined to stays."""
    from pyspark.sql.functions import col, md5, lit, concat_ws, lpad
    from pyspark.sql.types import DateType

    stays = spark.read.parquet(path_output + "stays_pmsi.parquet").select(
        "person_id", col("stay_id").alias("visit_id"), "stay_start_date")
    parts = []
    for fam, glob in [("mco", "YY_T_MCO*FLSTC.parquet"), ("ssr", "YY_T_SSR*FLSTC.parquet")]:
        d = _read_opt(spark, path_input + glob)
        if d is not None:
            parts.append(
                d.select("ETA_NUM", "SEQ_NUM", "NABM_COD", "ACT_NBR", "YEAR")
                 .withColumn("visit_id", _visit_id(fam, "ETA_NUM", "SEQ_NUM", ace=True))
                 .withColumn("measurement_source", lit(f"{fam.upper()} ACE")))
    u = _union(parts)
    if u is None:
        raise RuntimeError("measurement_pmsi: no usable source branch")
    j = u.join(stays, "visit_id")
    out = j.select(
        md5(concat_ws("_", col("visit_id"), col("stay_start_date"), col("NABM_COD"))).alias("measurement_id"),
        "person_id", col("stay_start_date").alias("measurement_start_date"),
        lit(None).cast(DateType()).alias("measurement_end_date"),
        col("ACT_NBR").cast("int").alias("quantity"),
        lpad(col("NABM_COD").cast("int").cast("string"), 4, "0").alias("measurement_code"),
        lit("NABM").alias("measurement_vocabulary"), "visit_id", "measurement_source")
    if mode_write:
        _write(out, path_output + "measurement_pmsi.parquet")
    return out


def build_medical_units(spark, path_input, path_output, mode_write=True):
    """Medical units from MCO (T_MCOaaUM) and SSR (T_SSRaaB), joined to stays."""
    from pyspark.sql.functions import col, lit, when

    stays = spark.read.parquet(path_output + "stays_pmsi.parquet").select(
        "person_id", col("stay_id").alias("visit_id"))
    parts = []
    um = _read_opt(spark, path_input + "YY_T_MCO*UM.parquet")
    if um is not None:
        parts.append(um.select(
            when(col("RUM_ORD_NUM").isNull(),
                 _visit_id("mco", "ETA_NUM", "RSA_NUM", "UM_ORD_NUM"))
            .otherwise(_visit_id("mco", "ETA_NUM", "RSA_NUM", "RUM_ORD_NUM")).alias("unit_id"),
            _visit_id("mco", "ETA_NUM", "RSA_NUM").alias("visit_id"),
            when(col("RUM_ORD_NUM").isNull(), col("UM_ORD_NUM")).otherwise(col("RUM_ORD_NUM")).alias("unit_nb"),
            col("AUT_TYP1_UM").alias("unit_authorization"), col("PAR_DUR_SEJ")))
    sb = _read_opt(spark, path_input + "YY_T_SSR*B.parquet")
    if sb is not None and "AUT_TYP_UM" in sb.columns and "UM_ORD_NUM" in sb.columns:
        parts.append(sb.select(
            _visit_id("ssr", "ETA_NUM", "RHA_NUM", "UM_ORD_NUM").alias("unit_id"),
            _visit_id("ssr", "ETA_NUM", "RHA_NUM").alias("visit_id"),
            col("UM_ORD_NUM").alias("unit_nb"),
            col("AUT_TYP_UM").alias("unit_authorization"), lit(None).alias("PAR_DUR_SEJ")))
    u = _union(parts)
    if u is None:
        raise RuntimeError("medical_units: no usable source branch")
    out = u.join(stays, "visit_id").select(
        "unit_id", "visit_id", "person_id", "unit_nb", "unit_authorization", "PAR_DUR_SEJ")
    if mode_write:
        _write(out, path_output + "medical_units.parquet")
    return out


def build_drug_dcir(spark, path_input, path_output, mode_write=True):
    """DCIR drugs from ER_PHA_F (city pharmacy, CIP13/CIP7) + ER_UCD_F (hospital
    retrocession, UCD), enriched with ATC from IR_PHA_R, joined to prestations on
    dcir_key. Replicates notebook 2 cell 59 (without the per-year sharding loop).

    The converter doesn't run combicancer's `withdraw_facturations_multiples`, so the
    `*_QSN_summed` regularised columns don't exist; on synthetic data each line is its
    own facturation, so the summed quantity is just the per-line quantity."""
    from pyspark.sql.functions import (
        col, lit, when, concat_ws, sum as Fsum, md5, broadcast, regexp_replace)

    pha = _read_opt(spark, path_input + "ER_PHA_F_*.parquet")
    ucd = _read_opt(spark, path_input + "ER_UCD_F_*.parquet")
    ir = _read_opt(spark, path_input + "IR_PHA_R.parquet")
    if pha is None and ucd is None:
        raise RuntimeError("drug_dcir: no ER_PHA_F / ER_UCD_F source")
    key = _dcir_key()

    branches = []
    if pha is not None:
        df_pha = pha.select(
            col("PHA_ACT_QSN").cast("int").alias("PHA_ACT_QSN"),
            when(col("PHA_PRS_C13") != "0", col("PHA_PRS_C13"))
            .when(col("PHA_PRS_IDE") != "0", col("PHA_PRS_IDE")).otherwise(lit(None)).alias("drug_code"),
            when(col("PHA_PRS_C13") != "0", lit("CIP13"))
            .when(col("PHA_PRS_IDE") != "0", lit("CIP7")).otherwise(lit(None)).alias("drug_vocabulary"),
            key.alias("dcir_key"))
        cip7 = (df_pha.filter(col("drug_vocabulary") == "CIP7")
                .groupby("dcir_key", "drug_code", "drug_vocabulary")
                .agg(Fsum("PHA_ACT_QSN").alias("quantity")))
        cip13 = (df_pha.filter(col("drug_vocabulary") == "CIP13")
                 .groupby("dcir_key", "drug_code", "drug_vocabulary")
                 .agg(Fsum("PHA_ACT_QSN").alias("quantity")))
        if ir is not None:
            ir_cip7 = ir.select(col("PHA_PRS_IDE").alias("drug_code"), "PHA_ATC_CLA", "PHA_ATC_LIB").dropDuplicates()
            ir_cip13 = ir.select(col("PHA_CIP_C13").alias("drug_code"), "PHA_ATC_CLA", "PHA_ATC_LIB").dropDuplicates()
            cip7 = cip7.join(broadcast(ir_cip7), "drug_code", "left")
            cip13 = cip13.join(broadcast(ir_cip13), "drug_code", "left")
        branches += [cip7, cip13]
    if ucd is not None:
        df_ucd = (ucd.select(
            key.alias("dcir_key"),
            regexp_replace("UCD_UCD_COD", r"^[0]*", "").alias("drug_code"),
            lit("UCD7").alias("drug_vocabulary"),
            col("UCD_DLV_NBR").cast("int").alias("quantity"))
            .groupby("dcir_key", "drug_code", "drug_vocabulary").agg(Fsum("quantity").alias("quantity")))
        if ir is not None:
            ir_ucd = ir.select(col("PHA_CIP_UCD").alias("drug_code"), "PHA_ATC_CLA", "PHA_ATC_LIB").dropDuplicates()
            df_ucd = df_ucd.join(broadcast(ir_ucd), "drug_code", "left")
        branches.append(df_ucd)

    med = _union(branches)
    if "PHA_ATC_CLA" not in med.columns:
        med = med.withColumn("PHA_ATC_CLA", lit(None).cast("string")).withColumn("PHA_ATC_LIB", lit(None).cast("string"))
    presta = _prestations(spark, path_output, "presta_start_date")
    j = med.join(presta, "dcir_key")
    # NB: the notebook's drug_id md5(concat_ws("dcir_key","drug_code")) is a bug (uses
    # "dcir_key" as the separator, hashing drug_code alone); we key it on dcir_key+date+
    # code so each delivery line gets a distinct id, matching the device/measurement form.
    out = j.select(
        md5(concat_ws("_", col("dcir_key"), col("presta_start_date"), col("drug_code"))).alias("drug_id"),
        "person_id", col("presta_start_date").alias("drug_date"), "drug_code", "drug_vocabulary",
        col("PHA_ATC_CLA").alias("drug_code_atc"), col("PHA_ATC_LIB").alias("molecule"),
        "quantity", col("dcir_key").alias("visit_id"), lit("DCIR").alias("drug_source"))
    if mode_write:
        _write(out, path_output + "drug_dcir.parquet")
    return out


def build_drug_pmsi(spark, path_input, path_output, mode_write=True):
    """PMSI drugs from the MCO/HAD/SSR medication tables (FHSTC, MED, MEDATU, MEDTHROMBO,
    MEDCHL), joined to stays_pmsi on visit_id and enriched with ATC from IR_PHA_R.
    Replicates notebook 2 cells 62-65 using combicancer's own drug.py transforms."""
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window
    from src.combicancer_pipeline.drug import drug as D

    # drug.py ships without its imports; inject the names its functions reference.
    for name in ("col", "lit", "when", "length", "md5", "concat_ws", "expr", "sum"):
        setattr(D, name, getattr(F, name))
    D.Window = Window

    stays = spark.read.parquet(path_output + "stays_pmsi.parquet").select(
        "stay_id", "person_id", "stay_start_date", "stay_end_date")
    ir = spark.read.parquet(path_input + "IR_PHA_R.parquet").select(
        "PHA_CIP_UCD", "PHA_ATC_CLA", "PHA_ATC_LIB").dropDuplicates(["PHA_CIP_UCD"])

    # (glob, family, ace, stay-keys, mode, delay_col, qty_col, filter_ucd_notnull)
    specs = [
        ("YY_T_MCO*FHSTC.parquet", "mco", True, ["ETA_NUM", "SEQ_NUM"], "date", None, "QUA", True),
        ("YY_T_MCO*MEDTHROMBO.parquet", "mco", False, ["ETA_NUM", "RSA_NUM"], "delay", "DAT_DELAI", "ADM_NBR", False),
        ("YY_T_MCO*MED.parquet", "mco", False, ["ETA_NUM", "RSA_NUM"], "delay", "DELAI", "ADM_NBR", True),
        ("YY_T_MCO*MEDATU.parquet", "mco", False, ["ETA_NUM", "RSA_NUM"], "delay", "DAT_DELAI", "ADM_NBR", False),
        ("YY_T_HAD*MEDATU.parquet", "had", False, ["ETA_NUM_EPMSI", "RHAD_NUM"], "delay", "DAT_DELAI", "ADM_NBR", False),
        ("YY_T_HAD*MEDCHL.parquet", "had", False, ["ETA_NUM_EPMSI", "RHAD_NUM"], "delay", "DAT_DELAI", "ADM_NBR", False),
        ("YY_T_HAD*MED.parquet", "had", False, ["ETA_NUM_EPMSI", "RHAD_NUM"], "delay", "DAT_DELAI", "ADM_NBR", False),
        ("YY_T_SSR*MEDATU.parquet", "ssr", False, ["ETA_NUM", "RHA_NUM"], "delay", "DAT_DELAI", "ADM_NBR", False),
        ("YY_T_SSR*MED.parquet", "ssr", False, ["ETA_NUM", "RHA_NUM"], "delay", "DAT_DELAI", "ADM_NBR", False),
    ]
    from pyspark.sql.functions import col
    branches = []
    for glob, fam, ace, keys, mode, delay_col, qty_col, ucd_notnull in specs:
        df = _read_opt(spark, path_input + glob)
        if df is None:
            continue
        if ucd_notnull:
            df = df.filter(col("UCD_UCD_COD").isNotNull())
        if delay_col is not None and delay_col not in df.columns:
            continue
        df = (df.withColumn("visit_id", _visit_id(fam, *keys, ace=ace))
              .transform(lambda d: D.create_drug_source(d, fam.upper()))
              .transform(D.create_drug_code_and_vocabulary)
              .transform(lambda d: D.join_with_stays_pmsi(d, stays))
              .transform(lambda d: D.join_with_ir_pha_r(d, ir)))
        if mode == "date":
            df = df.transform(D.create_drug_date)
        else:
            df = (df.transform(lambda d: D.create_date_origin(d, delay_col))
                  .transform(lambda d: D.create_delay(d, delay_col))
                  .transform(D.create_drug_date_with_delay))
        df = (df.transform(D.create_drug_id)
              .transform(D.create_drug_code_atc)
              .transform(D.create_drug_molecule)
              .transform(lambda d: D.regulate_quantity(d, qty_col))
              .transform(D.order_drug_table_cols))
        branches.append(df)

    out = _union(branches)
    if out is None:
        raise RuntimeError("drug_pmsi: no usable source branch")
    if mode_write:
        _write(out, path_output + "drug_pmsi.parquet")
    return out


def build_device_dcir(spark, path_input, path_output, mode_write=True):
    """DCIR medical devices (LPP) from ER_TIP_F, joined to prestations on dcir_key.
    Replicates notebook 2 cell 76 (no per-year loop). `TIP_ACT_QSN_summed` is synthesised
    from `TIP_ACT_QSN`; the generator names the end-date `TIP_ALC_DTF` (the notebook
    expects `TIP_ACL_DTF`) so we resolve whichever is present."""
    from pyspark.sql.functions import col, lit, when, concat_ws, md5, to_date

    tip = _read_opt(spark, path_input + "ER_TIP_F_*.parquet")
    if tip is None:
        raise RuntimeError("device_dcir: no ER_TIP_F source")
    dtf = "TIP_ACL_DTF" if "TIP_ACL_DTF" in tip.columns else "TIP_ALC_DTF"
    key = _dcir_key()
    tip = (tip.select(key.alias("dcir_key"), "TIP_ACL_DTD", "TIP_PRS_TYP", col(dtf).alias("TIP_DTF"),
                      "TIP_PRS_IDE", col("TIP_ACT_QSN").cast("int").alias("TIP_ACT_QSN_summed"))
           .dropDuplicates()
           .filter(col("TIP_PRS_TYP").isin(["1", "3", "6"])))
    presta = _prestations(spark, path_output)
    j = tip.join(presta, "dcir_key")
    out = (j.withColumn("device_start_date", to_date("TIP_ACL_DTD", "dd/MM/yyyy"))
           .withColumn("device_end_date",
                       when(col("TIP_PRS_TYP") == 3, to_date("TIP_DTF", "dd/MM/yyyy"))
                       .when(col("TIP_PRS_TYP") == 1, lit(None))
                       .when(col("TIP_PRS_TYP") == 6, to_date("TIP_ACL_DTD", "dd/MM/yyyy"))
                       .otherwise(lit(None)))
           .select(
               md5(concat_ws("_", col("dcir_key"), col("device_start_date"), col("TIP_PRS_IDE"))).alias("device_id"),
               "person_id", "device_start_date", "device_end_date",
               col("TIP_ACT_QSN_summed").alias("quantity"),
               col("TIP_PRS_IDE").alias("device_code"), lit("LPP").alias("device_vocabulary"),
               col("dcir_key").alias("visit_id"), lit("DCIR").alias("device_source")))
    if mode_write:
        _write(out, path_output + "device_dcir.parquet")
    return out


def build_device_pmsi(spark, path_input, path_output, mode_write=True):
    """PMSI medical devices (LPP) from MCO DMIP (inpatient) and FPSTC (ACE), joined to
    stays_pmsi on visit_id. Replicates notebook 2 cell 78."""
    from pyspark.sql.functions import col, lit, when, concat_ws, md5, expr, sum as Fsum
    from pyspark.sql.types import DateType

    parts = []
    dmip = _read_opt(spark, path_input + "YY_T_MCO*DMIP.parquet")
    if dmip is not None:
        d = (dmip.withColumn("visit_id", _visit_id("mco", "ETA_NUM", "RSA_NUM"))
             .select("visit_id", "DELAI", "LPP_COD", col("NBR_POS").alias("quantity"))
             .filter(col("LPP_COD").isNotNull())
             .groupby("visit_id", "DELAI", "LPP_COD").agg(Fsum(col("quantity")).alias("quantity"))
             .withColumn("device_source", lit("MCO")))
        parts.append(d)
    fpstc = _read_opt(spark, path_input + "YY_T_MCO*FPSTC.parquet")
    if fpstc is not None and "DEL_DAT_ENT" in fpstc.columns:
        d = (fpstc.withColumn("visit_id", _visit_id("mco", "ETA_NUM", "SEQ_NUM", ace=True))
             .select("visit_id", col("DEL_DAT_ENT").alias("DELAI"), "LPP_COD", col("LPP_QUA").alias("quantity"))
             .filter(col("LPP_COD").isNotNull())
             .groupby("visit_id", "DELAI", "LPP_COD").agg(Fsum(col("quantity")).alias("quantity"))
             .withColumn("device_source", lit("MCO ACE")))
        parts.append(d)
    u = _union(parts)
    if u is None:
        raise RuntimeError("device_pmsi: no usable source branch")
    stays = spark.read.parquet(path_output + "stays_pmsi.parquet").select(
        col("stay_id").alias("visit_id"), "person_id", "stay_start_date", "stay_end_date")
    j = (u.join(stays, "visit_id")
         .withColumn("delay", when(col("DELAI").isNull(), lit(0)).otherwise(col("DELAI").cast("int")))
         .withColumn("device_start_date", expr("date_add(stay_start_date, delay)")))
    out = j.select(
        md5(concat_ws("_", col("visit_id"), col("device_start_date"), col("LPP_COD"))).alias("device_id"),
        "person_id", "device_start_date", lit(None).cast(DateType()).alias("device_end_date"),
        "quantity", col("LPP_COD").alias("device_code"), lit("LPP").alias("device_vocabulary"),
        "visit_id", "device_source")
    if mode_write:
        _write(out, path_output + "device_pmsi.parquet")
    return out


def build_measurement_dcir(spark, path_input, path_output, mode_write=True):
    """DCIR biology measurements (NABM) from ER_BIO_F, joined to prestations on dcir_key.
    Replicates notebook 2 cell 80; `BIO_ACT_QSN_summed` synthesised from `BIO_ACT_QSN`."""
    from pyspark.sql.functions import col, lit, concat_ws, md5

    bio = _read_opt(spark, path_input + "ER_BIO_F_*.parquet")
    if bio is None:
        raise RuntimeError("measurement_dcir: no ER_BIO_F source")
    key = _dcir_key()
    bio = bio.select(key.alias("dcir_key"), "BIO_PRS_IDE",
                     col("BIO_ACT_QSN").cast("int").alias("BIO_ACT_QSN_summed")).dropDuplicates()
    presta = _prestations(spark, path_output, "presta_start_date", "presta_end_date")
    j = bio.join(presta, "dcir_key")
    out = j.select(
        md5(concat_ws("_", col("dcir_key"), col("presta_start_date"), col("BIO_PRS_IDE"))).alias("measurement_id"),
        "person_id", col("presta_start_date").alias("measurement_start_date"),
        col("presta_end_date").alias("measurement_end_date"),
        col("BIO_ACT_QSN_summed").alias("quantity"),
        col("BIO_PRS_IDE").alias("measurement_code"), lit("NABM").alias("measurement_vocabulary"),
        col("dcir_key").alias("visit_id"), lit("DCIR").alias("measurement_source"))
    if mode_write:
        _write(out, path_output + "measurement_dcir.parquet")
    return out


# OMOP tables built by combicancer modules on our Azurite data.
READY_STEPS = {
    "person": build_person,
    "causes_of_death": build_causes_of_death,
    "prestations": build_prestations,
    "stays_pmsi": build_stays_pmsi,
    "procedure_pmsi": build_procedure_pmsi,
    "condition_dcir": build_condition_dcir,
    "condition_pmsi": build_condition_pmsi,
    "measurement_pmsi": build_measurement_pmsi,
    "medical_units": build_medical_units,
    "drug_dcir": build_drug_dcir,
    "drug_pmsi": build_drug_pmsi,
    "device_dcir": build_device_dcir,
    "device_pmsi": build_device_pmsi,
    "measurement_dcir": build_measurement_dcir,
}


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--in", dest="path_in",
                   default=os.path.join(HERE, "out", "final", ""),
                   help="input directory (default: glue/out/final/)")
    p.add_argument("--out", dest="path_out",
                   default=os.path.join(HERE, "out", "omop", ""),
                   help="output directory (default: glue/out/omop/)")
    p.add_argument("--steps", default="all",
                   help="comma-separated table names, or 'all' for every READY_STEPS table in order")
    args = p.parse_args()

    if args.steps.strip() == "all":
        steps = list(READY_STEPS)
    else:
        steps = [s.strip() for s in args.steps.split(",") if s.strip()]

    path_input = args.path_in if args.path_in.endswith("/") else args.path_in + "/"
    path_output = args.path_out if args.path_out.endswith("/") else args.path_out + "/"
    os.makedirs(path_output, exist_ok=True)
    spark = env.get_spark("combicancer-omop")
    spark.sparkContext.setLogLevel("ERROR")

    results = {}
    for step in steps:
        if step not in READY_STEPS:
            print(f"UNKNOWN step {step}")
            continue
        print(f"\n=== building OMOP table: {step} ===")
        df = READY_STEPS[step](spark, path_input, path_output)
        n = df.count()
        results[step] = n
        print(f"  {step}: {n} rows  (cols: {df.columns})")
        df.show(5, truncate=False)

    print("\n=== OMOP build summary ===")
    for k, v in results.items():
        print(f"  {k}: {v} rows")
    spark.stop()


if __name__ == "__main__":
    main()
