"""
scouse_summary_pdf.py
----------------------
Write (or rewrite) the summary PDF of a complete SCOUSE + ACORNS unit: fit maps,
ACORNS structures per component set, the cluster table and example spectra
(alma_ops.imaging.scouse_summary). manual_acorns_run.py already writes it at the end
of every ACORNS run; this regenerates it, e.g. after scouse_cluster rows change.

Launch it with scouse_run.sh from the project root, in a CASA container terminal
(it needs the structure_id_env venv for acorns):

    alma-sails-codebase/scripts/scouse_run.sh --stage pdf --unit-id 6
    alma-sails-codebase/scripts/scouse_run.sh --stage pdf \
        --source S68Nc --line-label "HC3N(10-9)" [--cont FITS] [--out PDF] [--png]

Default output: <output_dir>/<source>_<line>_scouse_summary.pdf.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

_casa_prefix = "/opt/casa"
sys.path = [p for p in sys.path if not p.startswith(_casa_prefix)]
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from alma_ops.config import DB_PATH
from alma_ops.imaging.scouse_summary import write_summary_pdf
from alma_ops.imaging.scouse_utils import fetch_scouse_unit, scouse_connection


def main() -> None:
    parser = argparse.ArgumentParser(description="Summary PDF of a complete SCOUSE + ACORNS unit.")
    parser.add_argument("--unit-id", type=int, default=None, help="scouse_unit_id")
    parser.add_argument("--source", default=None)
    parser.add_argument("--line-label", default=None)
    parser.add_argument("--cont", default=None,
                        help="Continuum FITS whose peak defines (0,0) (default: from the DB)")
    parser.add_argument("--out", default=None, help="PDF path (default: in the unit's output dir)")
    parser.add_argument("--png", action="store_true", help="Also write each page as a PNG")
    parser.add_argument("--db-path", default=str(DB_PATH))
    args = parser.parse_args()
    if args.unit_id is None and not (args.source and args.line_label):
        sys.exit("[ERROR] give --unit-id, or both --source and --line-label")

    with scouse_connection(args.db_path) as conn:
        row = fetch_scouse_unit(conn, args.unit_id, args.source, args.line_label)
        if row is None:
            sys.exit("[ERROR] no such SCOUSE unit")
        out = write_summary_pdf(conn, row["scouse_unit_id"], out_path=args.out,
                                cont_path=args.cont, png=args.png)
    print(f"[OK] {row['source_name']} / {row['line_label']} → {out}")


if __name__ == "__main__":
    main()
