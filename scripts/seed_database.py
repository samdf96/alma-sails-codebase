"""
seed_database.py
----------------
Populates a freshly created v2 database from a normalized ALMiner CSV export.

The database must already exist and have been initialized from the schema:
    sqlite3 <db_path> < schemas/serpens_pipeline_v2.sql

Seeding order:
    1.  alminer_metadata         — raw CSV rows, one per (MOUS, target, spectral window)
    2.  projects                 — unique project_code rows
    3.  mous                     — unique MOUS rows, with band and array_config inferred
    4.  targets                  — unique (mous_id, alma_source_name) rows
    5.  pipeline_state           — one pending row per MOUS
    6.  state_transitions        — one initial NULL→pending entry per MOUS
    7.  imaging_unit             — unique (target_name, band, config_type) combinations
    8.  mous_imaging_unit        — junction rows linking MOUSes to imaging units
    9.  imaging_pipeline_state   — one pending row per imaging unit
    10. imaging_state_transitions — one initial NULL→pending entry per imaging unit

Usage:
    python alma-sails-codebase/scripts/seed_database.py <csv_path> --db-path <db_path>
"""

import argparse
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from alma_ops.utils import infer_array_config


# -----------------------------------------------------------------------------
# Column rename map: CSV names → schema names
# -----------------------------------------------------------------------------
RENAME_MAP = {
    "MOUS_id":          "mous_id",
    "member_ous_uid":   "mous_id",      # fallback if MOUS_id absent
    "ALMA_source_name": "alma_source_name",
    "RAJ2000":          "ra_deg",
    "DEJ2000":          "dec_deg",
    "LAS_arcsec":       "las_arcsec",
    "FoV_arcsec":       "fov_arcsec",
    "s_fov":            "s_fov_deg",
    "type":             "obs_type",
    "cont_sensitivity_bandwidth": "cont_sens_bandwidth",
    "sensitivity_10kms":          "line_sens_10kms",
}

# Columns to carry into the mous table (post-rename, MOUS-level fields only)
MOUS_COLS = [
    "mous_id",
    "project_code",
    "group_ous_uid",
    "obs_type",
    "scan_intent",
    "qa2_passed",
    "obs_release_date",
    "obs_title",
    "obs_creator_name",
    "access_url",
    "ang_res_arcsec",
    "las_arcsec",
    "t_exptime",
    "pwv",
    "scientific_category",
    "science_keyword",
]

# Columns to carry into the targets table (post-rename, target-level fields)
TARGET_COLS = [
    "mous_id",
    "alma_source_name",
    "band",
    "array_config",
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
    "cont_sens_bandwidth",
    "line_sens_10kms",
    "line_sens_native",
    "t_exptime",
    "frequency_support",
    "pol_states",
]

PROJECT_COLS = [
    "project_code",
    "proposal_id",
    "pi_name",
    "pi_userid",
    "proposal_authors",
    "proposal_abstract",
    "scientific_category",
    "bib_reference",
    "pub_title",
    "first_author",
    "authors",
    "pub_abstract",
    "publication_year",
]

ALMINER_METADATA_COLS = [
    "mous_id", "project_code", "proposal_id", "group_ous_uid",
    "obs_id", "asdm_uid", "obs_collection", "obs_publisher_did",
    "obs_title", "obs_type", "scan_intent", "obs_creator_name",
    "obs_release_date", "facility_name", "instrument_name",
    "antenna_arrays", "dataproduct_type", "calib_level",
    "access_url", "access_format", "data_rights", "qa2_passed",
    "pwv", "scientific_category", "science_keyword",
    "alma_source_name", "ra_deg", "dec_deg", "gal_longitude",
    "gal_latitude", "ang_res_arcsec", "las_arcsec", "fov_arcsec",
    "is_mosaic", "min_freq_GHz", "max_freq_GHz", "central_freq_GHz",
    "bandwidth_GHz", "freq_res_kHz", "vel_res_kms", "em_min", "em_max",
    "em_res_power", "em_xel", "cont_sens_bandwidth", "line_sens_10kms",
    "line_sens_native", "t_min", "t_max", "t_exptime", "t_resolution",
    "spatial_scale_max", "s_fov_deg", "s_region", "s_resolution",
    "band_list", "frequency_support", "pol_states",
    "proposal_abstract", "proposal_authors", "pi_userid", "pi_name",
    "bib_reference", "pub_title", "first_author", "authors",
    "pub_abstract", "publication_year", "collections",
]


# -----------------------------------------------------------------------------
# Inference helpers
# -----------------------------------------------------------------------------
# infer_array_config() lives in alma_ops.utils (imported above), not here.

def infer_band(band_list) -> str:
    """Normalise band_list to a plain band string, e.g. '7'."""
    if pd.isna(band_list):
        return "unknown"
    return str(int(float(band_list)))


# -----------------------------------------------------------------------------
# Main seeding function
# -----------------------------------------------------------------------------

def seed(csv_path: str, db_path: str) -> None:
    df = pd.read_csv(csv_path)
    df.columns = df.columns.str.strip()

    # Apply renames; handle the MOUS_id / member_ous_uid ambiguity
    if "MOUS_id" in df.columns:
        df = df.rename(columns=RENAME_MAP)
    elif "member_ous_uid" in df.columns:
        df = df.rename(columns=RENAME_MAP)
    else:
        raise ValueError("CSV has neither 'MOUS_id' nor 'member_ous_uid' column")

    required = ["mous_id", "project_code", "alma_source_name"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns after rename: {missing}")

    # Infer derived MOUS-level columns
    df["array_config"] = df["antenna_arrays"].apply(infer_array_config)
    df["band"] = df["band_list"].apply(infer_band)

    now = datetime.now(timezone.utc).isoformat()

    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON;")

    try:
        # ------------------------------------------------------------------
        # 1. alminer_metadata — raw dump, one row per CSV row
        # ------------------------------------------------------------------
        meta_cols = [c for c in ALMINER_METADATA_COLS if c in df.columns]
        df[meta_cols].to_sql("alminer_metadata", conn, if_exists="append", index=False)
        print(f"alminer_metadata: {len(df)} rows inserted")

        # ------------------------------------------------------------------
        # 2. projects
        # ------------------------------------------------------------------
        proj_cols = [c for c in PROJECT_COLS if c in df.columns]
        projects_df = df[proj_cols].drop_duplicates(subset=["project_code"])
        projects_df.to_sql("projects", conn, if_exists="append", index=False)
        print(f"projects: {len(projects_df)} rows inserted")

        # ------------------------------------------------------------------
        # 3. mous — one row per unique mous_id
        # ------------------------------------------------------------------
        mous_extra = ["band", "array_config"]
        mous_cols = [c for c in MOUS_COLS + mous_extra if c in df.columns]
        mous_df = df[mous_cols].drop_duplicates(subset=["mous_id"])
        mous_df.to_sql("mous", conn, if_exists="append", index=False)
        print(f"mous: {len(mous_df)} rows inserted")

        # ------------------------------------------------------------------
        # 4. targets — one row per unique (mous_id, alma_source_name)
        # ------------------------------------------------------------------
        tgt_cols = [c for c in TARGET_COLS if c in df.columns]
        targets_df = df[tgt_cols].drop_duplicates(subset=["mous_id", "alma_source_name"])
        targets_df.to_sql("targets", conn, if_exists="append", index=False)
        print(f"targets: {len(targets_df)} rows inserted")

        # ------------------------------------------------------------------
        # 5. pipeline_state — one pending row per MOUS
        # ------------------------------------------------------------------
        pipeline_df = mous_df[["mous_id"]].copy()
        pipeline_df["current_state"] = "pending"
        pipeline_df["updated_at"] = now
        pipeline_df["failure_count"] = 0
        pipeline_df.to_sql("pipeline_state", conn, if_exists="append", index=False)
        print(f"pipeline_state: {len(pipeline_df)} rows inserted")

        # ------------------------------------------------------------------
        # 6. state_transitions — initial NULL→pending entry per MOUS
        # ------------------------------------------------------------------
        transitions_df = mous_df[["mous_id"]].copy()
        transitions_df["from_state"] = None
        transitions_df["to_state"] = "pending"
        transitions_df["at"] = now
        transitions_df["note"] = "seeded"
        transitions_df.to_sql("state_transitions", conn, if_exists="append", index=False)
        print(f"state_transitions: {len(transitions_df)} initial rows inserted")

        # ------------------------------------------------------------------
        # 7. imaging_unit — unique (alma_source_name, band, array_config)
        # ------------------------------------------------------------------

        imaging_keys = df[["alma_source_name", "band", "array_config"]].drop_duplicates()
        imaging_keys = imaging_keys.rename(columns={
            "alma_source_name": "target_name",
            "array_config":     "config_type",
        })
        imaging_keys.to_sql("imaging_unit", conn, if_exists="append", index=False)

        # Reload with assigned IDs for the junction and state tables
        imaging_units = pd.read_sql(
            "SELECT imaging_unit_id, target_name, band, config_type FROM imaging_unit",
            conn,
        )
        print(f"imaging_unit: {len(imaging_units)} rows inserted")

        # ------------------------------------------------------------------
        # 8. mous_imaging_unit — junction table
        # ------------------------------------------------------------------
        mous_keys = df[["mous_id", "alma_source_name", "band", "array_config"]].drop_duplicates()
        mous_keys = mous_keys.rename(columns={
            "alma_source_name": "target_name",
            "array_config":     "config_type",
        })
        junction_df = mous_keys.merge(imaging_units, on=["target_name", "band", "config_type"])
        junction_df = junction_df[["mous_id", "imaging_unit_id"]].drop_duplicates()
        junction_df.to_sql("mous_imaging_unit", conn, if_exists="append", index=False)
        print(f"mous_imaging_unit: {len(junction_df)} rows inserted")

        # ------------------------------------------------------------------
        # 9. imaging_pipeline_state — one pending row per imaging unit
        # ------------------------------------------------------------------
        imaging_state_df = imaging_units[["imaging_unit_id", "target_name", "band", "config_type"]].copy()
        imaging_state_df["current_state"] = "pending"
        imaging_state_df["updated_at"] = now
        imaging_state_df["failure_count"] = 0
        imaging_state_df.to_sql("imaging_pipeline_state", conn, if_exists="append", index=False)
        print(f"imaging_pipeline_state: {len(imaging_state_df)} rows inserted")

        # ------------------------------------------------------------------
        # 10. imaging_state_transitions — initial NULL→pending entry per imaging unit
        # ------------------------------------------------------------------
        imaging_transitions_df = imaging_units[["imaging_unit_id", "target_name", "band", "config_type"]].copy()
        imaging_transitions_df["from_state"] = None
        imaging_transitions_df["to_state"] = "pending"
        imaging_transitions_df["at"] = now
        imaging_transitions_df["note"] = "seeded"
        imaging_transitions_df.to_sql("imaging_state_transitions", conn, if_exists="append", index=False)
        print(f"imaging_state_transitions: {len(imaging_transitions_df)} initial rows inserted")

        conn.commit()
        print(f"\nDatabase seeded successfully: {db_path}")

    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# -----------------------------------------------------------------------------
# CLI
# -----------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Seed the v2 ALMA-SAILS database from an ALMiner CSV export.")
    parser.add_argument("csv_path", help="Path to the ALMiner metadata CSV file")
    parser.add_argument("--db-path", required=True, help="Path to the initialized SQLite database")
    args = parser.parse_args()

    seed(csv_path=args.csv_path, db_path=args.db_path)
