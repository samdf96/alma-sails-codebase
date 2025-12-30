"""
db_populate_metadata.py
------------------
This script populates an initialized database with metadata downloaded
from an ALMiner request.

The database path must exist, and is typically created from the following:
sqlite3 database_name.db < ./path_to/database_schema.sql
"""

# ---------------------------------------------------------------------
# Bootstrap (allow importing alma_ops when running from scripts/)
# ---------------------------------------------------------------------
from bootstrap import setup_path

setup_path()

# ---------------------------------------------------------------------
# imports
# ---------------------------------------------------------------------
import argparse  # noqa: E402
import sqlite3  # noqa: E402

import pandas as pd  # noqa: E402

from alma_ops.config import DB_PATH  # noqa: E402
from alma_ops.logging import get_logger  # noqa: E402

# ---------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------
log = get_logger("db_populate_metadata")


def create_db(
    csv_path: str,
    database_path: str,
):
    """Populate the database with metadata from the given CSV file.

    Parameters
    ----------
    csv_path : str
        Path to the metadata CSV file.
    database_path : str
        Path to the SQLite database file (must exist).

    Raises
    ------
    ValueError
        If required columns are missing from the CSV file.
    """
    # load csv file into dataframe
    df = pd.read_csv(csv_path)

    # Normalize column names: strip whitespace and keep original case except specific renames
    df.columns = df.columns.str.strip()

    # Defensive renames to match DB column names
    rename_map = {
        "MOUS_id": "mous_id",
        "ALMA_source_name": "alma_source_name",
        "RAJ2000": "ra_deg",
        "DEJ2000": "dec_deg",
        "LAS_arcsec": "las_arcsec",
        "FoV_arcsec": "fov_arcsec",
        "s_fov": "s_fov_deg",
        "type": "obs_type",  # prevents clashes with type keyword
    }
    df = df.rename(columns=rename_map)

    # Required columns check (use the post-rename names)
    required = ["mous_id", "project_code", "alma_source_name"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(
            f"❌ Missing required columns in CSV after normalization/rename: {missing}"
        )
    log.info("✅ All required columns found!")

    # Connect to DB and enable foreign keys
    conn = sqlite3.connect(database_path)
    conn.execute("PRAGMA foreign_keys = ON;")

    # --------------------------
    # Projects
    # --------------------------
    projects_cols = [
        "project_code",
        "proposal_id",
        "proposal_abstract",
        "proposal_authors",
        "pi_userid",
        "pi_name",
        "scientific_category",
        "bib_reference",
        "pub_title",
        "first_author",
        "authors",
        "pub_abstract",
        "publication_year",
    ]
    # Only keep those that exist in the CSV to avoid KeyError for absent optional columns
    projects_present = [c for c in projects_cols if c in df.columns]
    projects_df = df[projects_present].drop_duplicates(subset=["project_code"])
    projects_df.to_sql("projects", conn, if_exists="append", index=False)

    # --------------------------
    # MOUS / observations
    # --------------------------
    mous_cols = [
        "mous_id",
        "project_code",
        "group_ous_uid",
        "obs_collection",
        "collections",
        "obs_publisher_did",
        "facility_name",
        "instrument_name",
        "antenna_arrays",
        "dataproduct_type",
        "calib_level",
        "access_url",
        "access_format",
        "data_rights",
        "obs_title",
        "scan_intent",
        "obs_type",
        "obs_creator_name",
        "obs_release_date",
        "qa2_passed",
        "pwv",
        "scientific_category",
        "science_keyword",
    ]
    mous_present = [c for c in mous_cols if c in df.columns]
    mous_df = df[mous_present].drop_duplicates(subset=["mous_id"])
    # If the DB column is 'mous_id' (lowercase) but your schema expects 'mous_id', this will match
    mous_df.to_sql("mous", conn, if_exists="append", index=False)

    # --------------------------
    # Targets
    # --------------------------
    targets_cols_original = [
        "mous_id",
        "alma_source_name",
        "obs_id",
        "ra_deg",
        "dec_deg",
        "gal_longitude",
        "gal_latitude",
        "ang_res_arcsec",
        "las_arcsec",
        "fov_arcsec",
        "is_mosaic",
        "min_freq_GHz",
        "max_freq_GHz",
        "central_freq_GHz",
        "bandwidth_GHz",
        "freq_res_kHz",
        "vel_res_kms",
        "em_min",
        "em_max",
        "em_res_power",
        "em_xel",
        "pwv",
        "asdm_uid",
        "cont_sens_bandwidth",
        "line_sens_10kms",
        "line_sens_native",
        "t_min",
        "t_max",
        "t_exptime",
        "t_resolution",
        "spatial_scale_max",
        "s_fov_deg",
        "s_region",
        "s_resolution",
        "band_list",
        "frequency_support",
        "pol_states",
        "scientific_category",
        "science_keyword",
        "qa2_passed",
        "obs_type",
        "scan_intent",
    ]
    targets_present = [c for c in targets_cols_original if c in df.columns]

    # If any renames changed column names, they are already in df so this list will be correct.
    targets_df = df[targets_present].copy()

    # Ensure column order matches DB if you want (not strictly necessary)
    targets_df.to_sql("targets", conn, if_exists="append", index=False)

    # --------------------------
    # Pipeline State
    # --------------------------
    # Create rows for each MOUS with default 'pending' statuses
    pipeline_state_df = df[["mous_id"]].drop_duplicates()

    # Add default values for status columns (others will use schema defaults)
    pipeline_state_df["download_status"] = "pending"
    pipeline_state_df["pre_selfcal_split_status"] = "pending"
    pipeline_state_df["pre_selfcal_listobs_status"] = "pending"
    pipeline_state_df["selfcal_status"] = "pending"
    pipeline_state_df["imaging_status"] = "pending"
    pipeline_state_df["cleanup_status"] = "pending"

    pipeline_state_df.to_sql("pipeline_state", conn, if_exists="append", index=False)

    # Finalize
    conn.commit()
    conn.close()
    log.info(f"✅ Data successfully loaded into {database_path}")


# =====================================================================
# Command-line interface
# =====================================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ALMA-SAILS database creator.")
    parser.add_argument("csv_path", help="Path to the metadata CSV file.")
    parser.add_argument("--db-path", default=DB_PATH)
    args = parser.parse_args()

    create_db(csv_path=args.csv_path, database_path=args.db_path)
