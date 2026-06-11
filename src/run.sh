#!/usr/bin/env bash
# Orchestrator for the synthetic-SNDS -> OMOP (combicancer) glue.
#
#   ./glue/run.sh init     one-time environment setup (Java 17, uv venv)
#   ./glue/run.sh run      full pipeline: generate -> convert -> OMOP-ise
#
# Parameters (env vars):
#   N_BEN=50  MIN_YEAR=2010  MAX_YEAR=2024  STEPS=person,causes_of_death
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(dirname "$HERE")"
VENV="$HERE/.venv"

N_BEN="${N_BEN:-50}"
MIN_YEAR="${MIN_YEAR:-2010}"
MAX_YEAR="${MAX_YEAR:-2024}"
STEPS="${STEPS:-all}"

export JAVA_HOME="${JAVA_HOME:-$(brew --prefix openjdk@17 2>/dev/null)/libexec/openjdk.jdk/Contents/Home}"

log() { printf '\n\033[1;36m==> %s\033[0m\n' "$*"; }

cmd_init() {
  log "Installing Java 17 via brew (needed by Spark)"
  command -v java >/dev/null 2>&1 && "$JAVA_HOME/bin/java" -version >/dev/null 2>&1 || brew install openjdk@17

  log "Creating uv environment"
  uv venv "$VENV" --python /usr/bin/python3
  uv pip install --python "$VENV" -r "$HERE/requirements.txt"
  log "init complete"
}

cmd_run() {
  log "1/4 Build generator resources (N_BEN=$N_BEN)"
  "$VENV/bin/python" "$HERE/build_resources.py" --n-beneficiaires "$N_BEN"

  log "2/4 Generate synthetic SNDS CSVs"
  rm -rf "$HERE/out/generated_csv"
  ( cd "$ROOT/synthetic-generator" && "$VENV/bin/python" -W ignore \
      src/generate_data.py --config "$HERE/snds_resources/combicancer.config" )

  log "3/4 Convert CSV -> per-year parquet (years $MIN_YEAR-$MAX_YEAR)"
  rm -rf "$HERE/out/final"
  "$VENV/bin/python" "$HERE/convert_and_load.py" \
      --min-year "$MIN_YEAR" --max-year "$MAX_YEAR"

  log "4/4 Run combicancer OMOP-isation (steps: $STEPS)"
  rm -rf "$HERE/out/omop"
  "$VENV/bin/python" "$HERE/run_combicancer.py" --steps "$STEPS"

  log "pipeline complete -- OMOP tables written to out/omop/"
}

case "${1:-run}" in
  init) cmd_init ;;
  run) cmd_run ;;
  *) echo "usage: $0 {init|run}"; exit 1 ;;
esac
