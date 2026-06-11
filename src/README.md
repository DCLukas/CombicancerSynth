# Glue: synthetic SNDS → OMOP-ified tables

Bridges two Health Data Hub repositories into one repeatable pipeline:

1. **[synthetic-generator](../synthetic-generator)** — generates a synthetic SNDS (random,
   so *repeatable* not bit-reproducible).
2. **[combicancer](../combicancer)** — transforms native SNDS into OMOP-like tables
   (`person`, `causes_of_death`, `condition`, `procedure`, `drug`, `device`,
   `measurement`, `visit_occurrence/stays`, `prestations`, `medical_units`).

The two were never meant to interoperate. This glue reconciles every mismatch and runs
the whole thing with one command.

```bash
make init     # one-time: Java 17, Node, Azurite, two uv envs, WASB jars
make run      # generate → convert → load → OMOP-ise   (re-run = fresh data)
```

Tunables: `make run N_BEN=200 MIN_YEAR=2015 MAX_YEAR=2024 STEPS=person,causes_of_death`.

---

## What it reconciles

| Aspect | generator emits | combicancer expects | handled by |
|---|---|---|---|
| Format | `;`-CSV | parquet | `convert_and_load.py` |
| Year split + naming | one generic file (`T_MCOaaC`), dates 1900s–2100s | flat per-year files `T_MCO22C` / `ER_PRS_F_2022`, 2010–2024 | `convert_and_load.py`: per-row year (stay-key hash / date remap into window) → group → write each shard under `add_year_to_table_name` |
| Patient key | `BEN_NIR_PSA` / `NIR_ANO_17` / `BEN_IDT_ANO` | `NUM_ENQ` / `NUM_ENQ_IDT` | `patient_key_map.json` collapse to one `NUM_ENQ` |
| Date format | mixed (`%d%b%Y:...` and `%d/%m/%Y`) | `dd/MM/yyyy` | converter normalises all date columns |
| Tables | full SNDS (~100) | a documented subset | `build_resources.py` (driven by combicancer's own var lists) |
| Storage | local FS | Azure Blob (`wasbs://`, `adlfs`) | local **Azurite** emulator + downloaded WASB jars |

## Pipeline stages

```
build_resources.py   combicancer variables/links  ->  tailored generator schemas + links.csv
                     (reuses default schemas; keeps native FKs so internal joins work;
                      adds 7 *STC sibling links so no table is left isolated)
        │
generate_data.py     synthetic-generator  ->  glue/out/generated_csv/*.csv   (in .venv-generator)
        │
convert_and_load.py  CSV -> glue/out/final/*.parquet  --upload-> Azurite 'final'
                     · normalise dates to dd/MM/yyyy, remap year into [MIN,MAX]
                     · derive consistent NUM_ENQ / NUM_ENQ_IDT (patient_key_map.json)
                     · split year-bearing tables per year -> combicancer filenames
                       (PMSI as YY_<name> + YEAR column; ER_ as <name>_YYYY)
                     · materialise IR_PHA_R + IR_BEN_R_uniq
                     (transform + upload in one pass, both in .venv-generator)
        │
run_combicancer.py   calls combicancer's OMOP modules on the Azurite 'final' data
                     ->  Azurite 'data' container: the 14 OMOP tables
```

`combicancer_env.py` is the single source of truth for the local runtime: the Spark
session (local master, in-memory catalog, WASB jars), the adlfs filesystem, the Azurite
connection string, and the two containers (`final` input, `data` output).

## Status

**Working end-to-end** (verified with `N_BEN=50`, `make run` green and repeatable):
- generator → converter → Azurite load: all 44 input tables, joins intact
  (`NUM_ENQ` 100 % ⊆ `IR_BEN_R`; `T_MCOaaC ⋈ T_MCOaaB` returns all rows; a stay's
  sub-tables all land in the same year shard).
- **All 14 OMOP tables** built by combicancer modules and written to the Azurite `data`
  container, all with referential integrity (`person_id ⊆ person`, PMSI `visit_id ⊆ stays`):
  `person`, `causes_of_death`, `prestations`, `stays_pmsi` (visit_occurrence backbone,
  MCO/SSR/HAD/ACE branches), `procedure_pmsi`, `condition_dcir`, `condition_pmsi`,
  `measurement_pmsi`, `medical_units`, `drug_dcir`, `drug_pmsi`, `device_dcir`,
  `device_pmsi`, `measurement_dcir`. Example counts at `N_BEN=50`: person 50,
  causes_of_death 100, prestations ~250, stays_pmsi ~950, procedure_pmsi ~580,
  condition_pmsi ~1460, measurement_pmsi ~230, medical_units ~480, drug_dcir ~500,
  drug_pmsi ~2150, device_dcir ~90, device_pmsi ~490, measurement_dcir ~250.

  Each is a `build_*` in `run_combicancer.py:READY_STEPS`, ported from the notebook with
  defensive per-branch reads (a source table/column absent from the synthetic subset
  drops just that branch). PMSI tables join to `stays_pmsi` on `visit_id`
  (`family_YEAR_keys`); `condition_dcir` is self-contained from `IR_IMB_R`.

Flattening insight: combicancer's `graph.pkl` is a **star** — every per-year anchor
(`T_MCO22C`, `T_MCO22CSTC`, `ER_PRS_F_2022`, `T_HAD22C`, `T_SSR22C/CSTC`, `IR_IMB_R`)
joins directly to `IR_BEN_R_uniq` on `NUM_ENQ`. Our converter already attaches
`NUM_ENQ`/`NUM_ENQ_IDT` to every anchor, so the heavyweight `Flattenizer` is unnecessary;
instead the converter emits a **`final`** container (anchors carry the patient keys, PMSI
tables get a `YY_` prefix + 2-digit `YEAR` column, plus `IR_BEN_R_uniq`) that the OMOP
modules read directly. The crucial fix was deriving each PMSI row's year from a stable
hash of its **stay key** (`PMSI_STAY_KEYS`) so a stay's tables share a `YEAR` — `YEAR` is
part of combicancer's join keys.

Realism fix: combicancer filters PMSI rows on coded indicator/order columns
(`UM_ORD_NUM=='1'`, `ACV_ACT==1`, `RHS_NUM=='001'`, …) that the generator would otherwise
fill with uniform-random values — filtering out ~everything. `build_resources.py`
(`INDICATOR_VALUES`) pins those columns to realistic value sets via `possible_values`.

DCIR-derived tables (`drug_dcir`, `device_dcir`, `measurement_dcir`) join their sub-table
(`ER_PHA_F`/`ER_UCD_F`, `ER_TIP_F`, `ER_BIO_F`) to `prestations` on `dcir_key` — the
9-column DCIR flux key (`FLX_*`, `ORG_CLE_NUM`, `DCT_ORD_NUM`, `PRS_ORD_NUM`,
`REM_TYP_AFF`) that the generator's FKs already make consistent across these tables. The
notebook shards this with a `prestations.parquet/FLX_DIS_DTD=*<year>` glob; since our
`FLX_DIS_DTD` is `dd/MM/yyyy` (slashes don't glob), we instead read the whole (tiny)
`prestations` once and join on `dcir_key` — exact regardless of year, because the key
embeds the date. `drug_pmsi`/`device_pmsi` join their PMSI medication/device tables to
`stays_pmsi` on `visit_id`. Three wrinkles handled: (1) combicancer's `drug.py` ships
without its `pyspark.sql.functions` imports, so `build_drug_pmsi` injects the names into
the module before applying its transforms; (2) the `*_QSN_summed` regularised columns come
from combicancer's `withdraw_facturations_multiples` preprocessing we don't run — on
synthetic data each line is its own facturation, so they're synthesised from the base
`*_QSN`/`UCD_DLV_NBR`; (3) `IR_PHA_R`'s ATC columns (`PHA_ATC_CLA`/`PHA_ATC_LIB`) *are*
present in the generator nomenclature, so `drug` enrichment works (left-joined, null-safe).
The quantity/delay/code columns feeding these tables (`PHA_ACT_QSN`, `UCD_DLV_NBR`,
`ADM_NBR`, `QUA`, `TIP_ACT_QSN`, `NBR_POS`, `LPP_QUA`, `BIO_ACT_QSN`, `DELAI`, `DAT_DELAI`,
`DEL_DAT_ENT`, `LPP_COD`) would otherwise come from the generator's unconstrained ranges
(negative/billion-scale quantities, random alpha `LPP_COD`, delays that push device/drug
dates years past the stay), so `build_resources.py` (`REALISTIC_VALUES`) pins them via
`possible_values`: counts 1–12, day-delays 0–15, plausible 7-digit LPP codes.

## Design notes / decisions

- **Reuse, don't re-key.** `build_resources.py` copies the generator's *existing* schema
  field definitions (keeping native foreign keys) rather than re-keying tables on
  `NUM_ENQ`. This preserves the generator's internally consistent joins; the patient-key
  collapse to `NUM_ENQ` happens once, in the converter, via `patient_key_map.json`.
- **Year by partition, not duplication.** Each row's year comes from its driving date
  column (remapped into the window); date-less PMSI sub-tables are spread round-robin.
  combicancer unions years before joining, so keys still match across shards.
- **Azurite over real Azure.** Keeps combicancer's `wasbs://` code unchanged. The
  `hadoop-azure` connector jars aren't on PyPI and Maven-over-TLS is intercepted in this
  environment, so `combicancer_env.download_jars()` fetches them with `curl -k` into
  `glue/jars/` and puts them on Spark's classpath via `spark.jars`.
- **Curie-specific preprocessing skipped.** Twin removal, `IR_BEN_R_ARC` merge, the
  `NUM_ENQ_ANO→NUM_ENQ_IDT` mapping and the multi-shard `ER_PRS_F` fusion all address
  messy real deliveries; the synthetic `IR_BEN_R` is already clean with both keys.
- Two `uv` environments because the generator (pandas&lt;2, networkx&lt;3) and combicancer
  (pyspark) stacks conflict. Both use the system `/usr/bin/python3` (3.9) since this
  sandbox can't download managed Pythons.

## Files

| File | Role |
|---|---|
| `build_resources.py` | combicancer var lists → generator schemas + `links.csv` + `patient_key_map.json` + config |
| `convert_and_load.py` | CSV → per-year parquet (dates, `NUM_ENQ`, `IR_PHA_R`); `--upload` to Azurite |
| `combicancer_env.py` | Spark/adlfs/Azurite wiring, container creation, WASB jar download |
| `run_combicancer.py` | drives combicancer OMOP modules; `READY_STEPS` registry |
| `run.sh` / `Makefile` | orchestration (`init`, `run`, `azurite`) |
| `requirements-*.txt` | the two environments |
