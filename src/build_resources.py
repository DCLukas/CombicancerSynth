#!/usr/bin/env python3
"""Write the synthetic-generator config for this run.

The generator schemas, links.csv, and patient_key_map.json under snds_resources/
are static artifacts (derived once from combicancer's variable lists; stable as long
as the combicancer tables and columns don't change). Only the generator config needs
to be written per run because it embeds n_beneficiaires and the absolute export path.
"""
import argparse
import os

HERE = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(HERE)
OUT_ROOT = os.path.join(HERE, "snds_resources")


def build(n_beneficiaires, sep):
    # nomenclatures: the generator resolves codes against these CSVs; we point it at
    # the generator's own folder so we never need to copy or maintain a duplicate.
    out_nomenc = os.path.join(OUT_ROOT, "nomenclatures")
    gen_nomenc = os.path.join(REPO_ROOT, "synthetic-generator", "src", "resources", "nomenclatures")
    if not os.path.exists(out_nomenc):
        os.symlink(os.path.abspath(gen_nomenc), out_nomenc)

    export_path = os.path.join(HERE, "out", "generated_csv")
    cfg = f"""[BASE]
base_name = SNDS
roots = IR_BEN_R
n_beneficiaires = {n_beneficiaires}
export_path = {export_path}
path2resources = {OUT_ROOT}
sep = {sep}

[SCHEMA MODIFIER]
"""
    with open(os.path.join(OUT_ROOT, "combicancer.config"), "w") as fh:
        fh.write(cfg)
    print(f"Generator config written (N={n_beneficiaires}, out={export_path})")


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--n-beneficiaires", type=int, default=50)
    p.add_argument("--sep", default=";")
    args = p.parse_args()
    build(args.n_beneficiaires, args.sep)
