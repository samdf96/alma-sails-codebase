"""
test_autoselfcal.py
-------------------
Layer 2 integration tests for autoselfcal_mous_flow (mous_autoselfcal.py).

What is tested:
  - Status validation rejects wrong preconditions
  - Flow raises if auto_selfcal directory does not exist
  - Happy path: DB transitions to in_progress, selfcal_started_at timestamp set,
    Session.create called
  - Error path: Session failure causes DB to transition to error
"""

import sqlite3
from unittest.mock import patch

import pytest

from alma_ops.db import get_db_connection, get_pipeline_state_record
from conftest import TEST_MOUS_ID
from mous_autoselfcal import autoselfcal_mous_flow


# ---------------------------------------------------------------------------
# Status validation
# ---------------------------------------------------------------------------


class TestAutoselfcalFlowValidation:
    def test_rejects_non_prepped_selfcal_status(self, autoselfcal_flow_env):
        db = autoselfcal_flow_env["db_path"]
        conn = sqlite3.connect(db)
        conn.execute(
            "UPDATE pipeline_state SET selfcal_status='pending' WHERE mous_id=?",
            (TEST_MOUS_ID,),
        )
        conn.commit()
        conn.close()

        with pytest.raises(ValueError, match="expected 'prepped'"):
            autoselfcal_mous_flow(
                mous_id=TEST_MOUS_ID,
                db_path=db,
            )

    def test_rejects_missing_autoselfcal_directory(self, autoselfcal_flow_env):
        # Remove the auto_selfcal dir that the fixture created
        autoselfcal_dir = autoselfcal_flow_env["mous_dir"] / "auto_selfcal"
        autoselfcal_dir.rmdir()

        with pytest.raises(FileNotFoundError, match="auto_selfcal directory not found"):
            with patch("canfar.sessions.Session.create", return_value=["mock-job"]):
                autoselfcal_mous_flow(
                    mous_id=TEST_MOUS_ID,
                    db_path=autoselfcal_flow_env["db_path"],
                )


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


class TestAutoselfcalFlowHappyPath:
    @pytest.fixture(autouse=True)
    def mock_session(self):
        with patch(
            "canfar.sessions.Session.create", return_value=["mock-selfcal-job-001"]
        ) as m:
            yield m

    def test_sets_status_to_in_progress(self, autoselfcal_flow_env):
        autoselfcal_mous_flow(
            mous_id=TEST_MOUS_ID,
            db_path=autoselfcal_flow_env["db_path"],
        )
        with get_db_connection(autoselfcal_flow_env["db_path"]) as conn:
            row = get_pipeline_state_record(conn, TEST_MOUS_ID)
        assert row["selfcal_status"] == "in_progress"

    def test_selfcal_started_at_timestamp_is_set(self, autoselfcal_flow_env):
        autoselfcal_mous_flow(
            mous_id=TEST_MOUS_ID,
            db_path=autoselfcal_flow_env["db_path"],
        )
        with get_db_connection(autoselfcal_flow_env["db_path"]) as conn:
            row = get_pipeline_state_record(conn, TEST_MOUS_ID)
        assert row["selfcal_started_at"] is not None

    def test_session_create_is_called_once(self, autoselfcal_flow_env, mock_session):
        autoselfcal_mous_flow(
            mous_id=TEST_MOUS_ID,
            db_path=autoselfcal_flow_env["db_path"],
        )
        mock_session.assert_called_once()

    def test_mous_id_in_session_args(self, autoselfcal_flow_env, mock_session):
        autoselfcal_mous_flow(
            mous_id=TEST_MOUS_ID,
            db_path=autoselfcal_flow_env["db_path"],
        )
        args_str = mock_session.call_args.kwargs.get("args", "")
        assert TEST_MOUS_ID in args_str


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------


class TestAutoselfcalFlowErrorHandling:
    def test_session_failure_sets_error_status(self, autoselfcal_flow_env):
        with patch(
            "mous_autoselfcal.launch_autoselfcal_job_task",
            side_effect=RuntimeError("Session refused to launch"),
        ):
            with pytest.raises(RuntimeError):
                autoselfcal_mous_flow(
                    mous_id=TEST_MOUS_ID,
                    db_path=autoselfcal_flow_env["db_path"],
                )

        with get_db_connection(autoselfcal_flow_env["db_path"]) as conn:
            row = get_pipeline_state_record(conn, TEST_MOUS_ID)
        assert row["selfcal_status"] == "error"