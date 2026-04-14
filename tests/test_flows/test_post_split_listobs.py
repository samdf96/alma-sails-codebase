"""
test_post_split_listobs.py
--------------------------
Layer 2 integration tests for post_split_listobs_flow (mous_post_split_listobs.py).

What is tested:
  - Status validation rejects wrong preconditions
  - Happy path: DB transitions to in_progress, Session.create called,
    JSON payload file created with correct task structure
  - Error path: Session failure causes DB to transition to error
"""

import json
import sqlite3
from unittest.mock import patch

import pytest

from alma_ops.db import get_db_connection, get_pipeline_state_record
from conftest import TEST_MOUS_ID
from mous_post_split_listobs import post_split_listobs_flow


# ---------------------------------------------------------------------------
# Status validation
# ---------------------------------------------------------------------------


class TestListobsFlowValidation:
    def test_rejects_non_complete_split_status(self, listobs_flow_env):
        db = listobs_flow_env["db_path"]
        conn = sqlite3.connect(db)
        conn.execute(
            "UPDATE pipeline_state SET pre_selfcal_split_status='pending' WHERE mous_id=?",
            (TEST_MOUS_ID,),
        )
        conn.commit()
        conn.close()

        with pytest.raises(ValueError, match="expected 'complete'"):
            post_split_listobs_flow(
                mous_id=TEST_MOUS_ID,
                db_path=db,
                datasets_dir=listobs_flow_env["datasets_dir"],
            )

    def test_rejects_non_pending_listobs_status(self, listobs_flow_env):
        db = listobs_flow_env["db_path"]
        conn = sqlite3.connect(db)
        conn.execute(
            "UPDATE pipeline_state SET pre_selfcal_listobs_status='complete' WHERE mous_id=?",
            (TEST_MOUS_ID,),
        )
        conn.commit()
        conn.close()

        with pytest.raises(ValueError, match="expected 'pending'"):
            post_split_listobs_flow(
                mous_id=TEST_MOUS_ID,
                db_path=db,
                datasets_dir=listobs_flow_env["datasets_dir"],
            )


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


class TestListobsFlowHappyPath:
    @pytest.fixture(autouse=True)
    def mock_session(self):
        with patch(
            "canfar.sessions.Session.create", return_value=["mock-listobs-job-001"]
        ) as m:
            yield m

    def test_sets_status_to_in_progress(self, listobs_flow_env):
        post_split_listobs_flow(
            mous_id=TEST_MOUS_ID,
            db_path=listobs_flow_env["db_path"],
            datasets_dir=listobs_flow_env["datasets_dir"],
        )
        with get_db_connection(listobs_flow_env["db_path"]) as conn:
            row = get_pipeline_state_record(conn, TEST_MOUS_ID)
        assert row["pre_selfcal_listobs_status"] == "in_progress"

    def test_session_create_is_called_once(self, listobs_flow_env, mock_session):
        post_split_listobs_flow(
            mous_id=TEST_MOUS_ID,
            db_path=listobs_flow_env["db_path"],
            datasets_dir=listobs_flow_env["datasets_dir"],
        )
        mock_session.assert_called_once()

    def test_json_payload_contains_both_product_types(self, listobs_flow_env):
        post_split_listobs_flow(
            mous_id=TEST_MOUS_ID,
            db_path=listobs_flow_env["db_path"],
            datasets_dir=listobs_flow_env["datasets_dir"],
        )
        from alma_ops.utils import to_dir_mous_id
        json_file = (
            listobs_flow_env["mous_dir"]
            / f"{to_dir_mous_id(TEST_MOUS_ID)}_listobs.json"
        )
        assert json_file.exists()
        payload = json.loads(json_file.read_text())
        assert payload["mous_id"] == TEST_MOUS_ID
        # Should have tasks for both calibrated and split products
        tasks = payload["tasks"]
        assert len(tasks) == 2  # 1 calibrated + 1 split from the fixture
        assert all(t["task"] == "listobs" for t in tasks)
        assert all(t["listfile"].endswith(".listobs.txt") for t in tasks)


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------


class TestListobsFlowErrorHandling:
    def test_session_failure_sets_error_status(self, listobs_flow_env):
        with patch(
            "mous_post_split_listobs.launch_listobs_job_task",
            side_effect=RuntimeError("Session refused to launch"),
        ):
            with pytest.raises(RuntimeError):
                post_split_listobs_flow(
                    mous_id=TEST_MOUS_ID,
                    db_path=listobs_flow_env["db_path"],
                    datasets_dir=listobs_flow_env["datasets_dir"],
                )

        with get_db_connection(listobs_flow_env["db_path"]) as conn:
            row = get_pipeline_state_record(conn, TEST_MOUS_ID)
        assert row["pre_selfcal_listobs_status"] == "error"