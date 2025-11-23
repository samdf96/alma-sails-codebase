# alma_ops/splits/run_headless_split.py

"""
Only works in CASA environment, casatask or casatools imports not needed.
"""
import argparse

# ---------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Spins a headless CASA session for splitting a MOUS."
    )
    parser.add_argument("--vis", required=True)
    parser.add_argument("--outputvis", required=True)
    parser.add_argument("--field", required=True)
    parser.add_argument("--spws", required=True)
    args = parser.parse_args()

    split(
        vis=args.vis,
        outputvis=args.outputvis,
        field=args.field,
        spw=args.spws,
        datacolumn="data",
    )
