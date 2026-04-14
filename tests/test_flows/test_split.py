"""
test_split.py
-------------
Layer 2 integration tests for split_mous_flow (mous_split.py).

What is tested:
  - Status validation rejects wrong preconditions
  - Happy path: DB transitions to in_progress, Session.create called, split
    product paths written to DB, JSON payload file created on disk
  - Error path: Session failure causes DB to transition to error
"""

import json
import sqlite3
from pathlib import Path
from unittest.mock import call, patch

import pytest

from alma_ops.db import get_db_connection, get_pipeline_state_record
from conftest import TEST_MOUS_ID
from mous_split import split_mous_flow


# ---------------------------------------------------------------------------
# Status validation
# ---------------------------------------------------------------------------


class TestSplitFlowValidation:
    def test_rejects_non_pending_split_status(self, split_flow_env):
        db = split_flow_env["db_path"]
        conn = sqlite3.connect(db)
        conn.execute(
            "UPDATE pipeline_state SET pre_selfcal_split_status='complete' WHERE mous_id=?",
            (TEST_MOUS_ID,),
        )
        conn.commit()
        conn.close()

        with pytest.raises(ValueError, match="expected 'pending'"):
            split_mous_flow(
                mous_id=TEST_MOUS_ID,
                db_path=db,
                download_dir=split_flow_env["datasets_dir"],
            )

    def test_rejects_incomplete_download_status(self, split_flow_env):
        db = split_flow_env["db_path"]
        conn = sqlite3.connect(db)
        conn.execute(
            "UPDATE pipeline_state SET download_status='pending' WHERE mous_id=?",
            (TEST_MOUS_ID,),
        )
        conn.commit()
        conn.close()

        with pytest.raises(ValueError, match="expected 'complete'"):
            split_mous_flow(
                mous_id=TEST_MOUS_ID,
                db_path=db,
                download_dir=split_flow_env["datasets_dir"],
            )

    def test_rejects_unknown_mous_id(self, split_flow_env):
        db = split_flow_env["db_path"]
        with pytest.raises(ValueError, match="not found"):
            split_mous_flow(
                mous_id="uid://A001/Xbad/Xbad",
                db_path=db,
                download_dir=split_flow_env["datasets_dir"],
            )


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


class TestSplitFlowHappyPath:
    @pytest.fixture(autouse=True)
    def mock_session(self):
        with patch("canfar.sessions.Session.create", return_value=["mock-split-job-001"]) as m:
            yield m

    def test_sets_status_to_in_progress(self, split_flow_env):
        split_mous_flow(
            mous_id=TEST_MOUS_ID,
            db_path=split_flow_env["db_path"],
            download_dir=split_flow_env["datasets_dir"],
        )
        with get_db_connection(split_flow_env["db_path"]) as conn:
            row = get_pipeline_state_record(conn, TEST_MOUS_ID)
        assert row["pre_selfcal_split_status"] == "in_progress"

    def test_session_create_is_called_once(self, split_flow_env, mock_session):
        split_mous_flow(
            mous_id=TEST_MOUS_ID,
            db_path=split_flow_env["db_path"],
            download_dir=split_flow_env["datasets_dir"],
        )
        mock_session.assert_called_once()

    def test_split_products_written_to_db(self, split_flow_env):
        split_mous_flow(
            mous_id=TEST_MOUS_ID,
            db_path=split_flow_env["db_path"],
            download_dir=split_flow_env["datasets_dir"],
        )
        with get_db_connection(split_flow_env["db_path"]) as conn:
            raw = conn.execute(
                "SELECT split_products_path FROM pipeline_state WHERE mous_id=?",
                (TEST_MOUS_ID,),
            ).fetchone()[0]
        products = json.loads(raw)
        assert isinstance(products, list)
        assert len(products) > 0
        assert all("_targets.ms" in p for p in products)

    def test_json_payload_file_is_written(self, split_flow_env):
        split_mous_flow(
            mous_id=TEST_MOUS_ID,
            db_path=split_flow_env["db_path"],
            download_dir=split_flow_env["datasets_dir"],
        )
        from alma_ops.utils import to_dir_mous_id
        json_file = (
            split_flow_env["mous_dir"] / f"{to_dir_mous_id(TEST_MOUS_ID)}_splits.json"
        )
        assert json_file.exists()
        payload = json.loads(json_file.read_text())
        assert payload["mous_id"] == TEST_MOUS_ID
        assert "tasks" in payload


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------


class TestSplitFlowErrorHandling:
    def test_session_failure_sets_error_status(self, split_flow_env):
        with patch(
            "mous_split.launch_split_job_task",
            side_effect=RuntimeError("Session refused to launch"),
        ):
            with pytest.raises(RuntimeError):
                split_mous_flow(
                    mous_id=TEST_MOUS_ID,
                    db_path=split_flow_env["db_path"],
                    download_dir=split_flow_env["datasets_dir"],
                )

        with get_db_connection(split_flow_env["db_path"]) as conn:
            row = get_pipeline_state_record(conn, TEST_MOUS_ID)
        assert row["pre_selfcal_split_status"] == "error"