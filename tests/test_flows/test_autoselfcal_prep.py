"""
test_autoselfcal_prep.py
------------------------
Layer 2 integration tests for autoselfcal_prep_flow (mous_autoselfcal_prep.py).

What is tested:
  - Status validation rejects wrong preconditions
  - Happy path: auto_selfcal directory created on disk, Session.create called
  - Error path: Session failure causes DB to transition to error
"""

import sqlite3
from pathlib import Path
from unittest.mock import patch

import pytest

from alma_ops.db import get_db_connection, get_pipeline_state_record
from conftest import TEST_MOUS_ID
from mous_autoselfcal_prep import autoselfcal_prep_flow


# ---------------------------------------------------------------------------
# Status validation
# ---------------------------------------------------------------------------


class TestAutoselfcalPrepFlowValidation:
    def test_rejects_non_complete_listobs_status(self, autoselfcal_prep_flow_env):
        db = autoselfcal_prep_flow_env["db_path"]
        conn = sqlite3.connect(db)
        conn.execute(
            "UPDATE pipeline_state SET pre_selfcal_listobs_status='pending' WHERE mous_id=?",
            (TEST_MOUS_ID,),
        )
        conn.commit()
        conn.close()

        with pytest.raises(ValueError, match="expected 'complete'"):
            autoselfcal_prep_flow(
                mous_id=TEST_MOUS_ID,
                db_path=db,
            )

    def test_rejects_non_pending_selfcal_status(self, autoselfcal_prep_flow_env):
        db = autoselfcal_prep_flow_env["db_path"]
        conn = sqlite3.connect(db)
        conn.execute(
            "UPDATE pipeline_state SET selfcal_status='in_progress' WHERE mous_id=?",
            (TEST_MOUS_ID,),
        )
        conn.commit()
        conn.close()

        with pytest.raises(ValueError, match="expected 'pending'"):
            autoselfcal_prep_flow(
                mous_id=TEST_MOUS_ID,
                db_path=db,
            )


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


class TestAutoselfcalPrepFlowHappyPath:
    @pytest.fixture(autouse=True)
    def mock_session(self):
        with patch(
            "canfar.sessions.Session.create", return_value=["mock-prep-job-001"]
        ) as m:
            yield m

    def test_creates_autoselfcal_directory(self, autoselfcal_prep_flow_env):
        autoselfcal_prep_flow(
            mous_id=TEST_MOUS_ID,
            db_path=autoselfcal_prep_flow_env["db_path"],
        )
        expected_dir = autoselfcal_prep_flow_env["mous_dir"] / "auto_selfcal"
        assert expected_dir.is_dir()

    def test_session_create_is_called_once(self, autoselfcal_prep_flow_env, mock_session):
        autoselfcal_prep_flow(
            mous_id=TEST_MOUS_ID,
            db_path=autoselfcal_prep_flow_env["db_path"],
        )
        mock_session.assert_called_once()

    def test_split_products_passed_to_session(self, autoselfcal_prep_flow_env, mock_session):
        autoselfcal_prep_flow(
            mous_id=TEST_MOUS_ID,
            db_path=autoselfcal_prep_flow_env["db_path"],
        )
        args_str = mock_session.call_args.kwargs.get("args", "")
        # The split product path should appear in the session args
        assert "calibrated_targets.ms" in args_str


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------


class TestAutoselfcalPrepFlowErrorHandling:
    def test_session_failure_sets_error_status(self, autoselfcal_prep_flow_env):
        with patch(
            "mous_autoselfcal_prep.launch_autoselfcal_prep_job_task",
            side_effect=RuntimeError("Session refused to launch"),
        ):
            with pytest.raises(RuntimeError):
                autoselfcal_prep_flow(
                    mous_id=TEST_MOUS_ID,
                    db_path=autoselfcal_prep_flow_env["db_path"],
                )

        with get_db_connection(autoselfcal_prep_flow_env["db_path"]) as conn:
            row = get_pipeline_state_record(conn, TEST_MOUS_ID)
        assert row["selfcal_status"] == "error"