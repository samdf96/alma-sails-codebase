"""
test_db.py
----------
Unit tests for selected functions in alma_ops/db.py.

Covers:
  - parse_json_safe        (pure function, no DB needed)
  - update_pipeline_state_record  (serialization + write)
  - get_mous_spw_mapping   (regex extraction + SPW remapping)
"""

import json
import sqlite3

import pytest

from alma_ops.db import (
    get_db_connection,
    get_mous_spw_mapping,
    get_pipeline_state_record,
    get_pipeline_state_record_column_value,
    parse_json_safe,
    update_pipeline_state_record,
)
from conftest import TEST_MOUS_ID


# ===========================================================================
# parse_json_safe
# ===========================================================================


class TestParseJsonSafe:
    def test_none_returns_none(self):
        assert parse_json_safe(None) is None

    def test_valid_json_list(self):
        result = parse_json_safe('["a", "b", "c"]')
        assert result == ["a", "b", "c"]

    def test_valid_json_dict(self):
        result = parse_json_safe('{"key": 1}')
        assert result == {"key": 1}

    def test_plain_string_returns_as_is(self):
        # A bare string that is not JSON should come back unchanged
        assert parse_json_safe("pending") == "pending"

    def test_invalid_json_returns_raw_value(self):
        # Malformed JSON should not raise — return the raw string
        bad = "{not: valid json}"
        assert parse_json_safe(bad) == bad

    def test_integer_input_returns_as_is(self):
        # Non-string values that are not JSON-parseable should pass through
        assert parse_json_safe(42) == 42

    def test_empty_string_returns_as_is(self):
        # json.loads("") raises JSONDecodeError → return raw value
        assert parse_json_safe("") == ""


# ===========================================================================
# update_pipeline_state_record
# ===========================================================================


class TestUpdatePipelineStateRecord:
    def test_update_string_field(self, test_db):
        with get_db_connection(test_db) as conn:
            update_pipeline_state_record(conn, TEST_MOUS_ID, download_status="complete")
            row = get_pipeline_state_record(conn, TEST_MOUS_ID)
        assert row["download_status"] == "complete"

    def test_update_multiple_fields(self, test_db):
        with get_db_connection(test_db) as conn:
            update_pipeline_state_record(
                conn,
                TEST_MOUS_ID,
                download_status="complete",
                pre_selfcal_split_status="in_progress",
            )
            row = get_pipeline_state_record(conn, TEST_MOUS_ID)
        assert row["download_status"] == "complete"
        assert row["pre_selfcal_split_status"] == "in_progress"

    def test_list_is_json_serialised_on_write(self, test_db):
        paths = [
            "/arc/projects/ALMA-SAILS/datasets/uid___A001_X1234_Xtest/splits/a_targets.ms",
            "/arc/projects/ALMA-SAILS/datasets/uid___A001_X1234_Xtest/splits/b_targets.ms",
        ]
        with get_db_connection(test_db) as conn:
            update_pipeline_state_record(conn, TEST_MOUS_ID, split_products_path=paths)

        # Verify what is actually stored in SQLite is a JSON string
        raw_conn = sqlite3.connect(test_db)
        raw = raw_conn.execute(
            "SELECT split_products_path FROM pipeline_state WHERE mous_id=?",
            (TEST_MOUS_ID,),
        ).fetchone()[0]
        raw_conn.close()
        assert isinstance(raw, str)
        assert json.loads(raw) == paths

    def test_list_roundtrips_through_column_value_getter(self, test_db):
        paths = [
            "/arc/projects/ALMA-SAILS/datasets/uid___A001_X1234_Xtest/splits/a_targets.ms",
        ]
        with get_db_connection(test_db) as conn:
            update_pipeline_state_record(conn, TEST_MOUS_ID, split_products_path=paths)
            result = get_pipeline_state_record_column_value(
                conn, TEST_MOUS_ID, "split_products_path"
            )
        assert result == paths

    def test_no_fields_is_noop(self, test_db):
        with get_db_connection(test_db) as conn:
            update_pipeline_state_record(conn, TEST_MOUS_ID)
            row = get_pipeline_state_record(conn, TEST_MOUS_ID)
        # Default value from schema should still be 'pending'
        assert row["download_status"] == "pending"


# ===========================================================================
# get_mous_spw_mapping
# ===========================================================================


class TestGetMousSpwMapping:
    def test_extracts_spw_numbers_from_obs_ids(self, test_db_with_targets):
        with get_db_connection(test_db_with_targets) as conn:
            spws = get_mous_spw_mapping(conn, TEST_MOUS_ID)
        assert spws == [17, 19, 21, 25]

    def test_result_is_sorted(self, test_db_with_targets):
        with get_db_connection(test_db_with_targets) as conn:
            spws = get_mous_spw_mapping(conn, TEST_MOUS_ID)
        assert spws == sorted(spws)

    def test_result_has_no_duplicates(self, test_db_with_targets):
        # Insert a duplicate SPW entry
        conn = sqlite3.connect(test_db_with_targets)
        conn.execute(
            "INSERT INTO targets (mous_id, obs_id) VALUES (?, ?)",
            (TEST_MOUS_ID, "uid://A002/Xabc/Xdup.spw.17"),
        )
        conn.commit()
        conn.close()

        with get_db_connection(test_db_with_targets) as conn:
            spws = get_mous_spw_mapping(conn, TEST_MOUS_ID)
        assert spws.count(17) == 1

    def test_obs_ids_without_spw_suffix_are_ignored(self, test_db):
        # Targets with no .spw.<N> pattern should not contribute any SPWs
        conn = sqlite3.connect(test_db)
        conn.execute(
            "INSERT INTO targets (mous_id, obs_id) VALUES (?, ?)",
            (TEST_MOUS_ID, "uid://A002/Xabc/X1"),  # no .spw. suffix
        )
        conn.commit()
        conn.close()

        with get_db_connection(test_db) as conn:
            spws = get_mous_spw_mapping(conn, TEST_MOUS_ID)
        assert spws == []

    def test_empty_targets_returns_empty_list(self, test_db):
        with get_db_connection(test_db) as conn:
            spws = get_mous_spw_mapping(conn, TEST_MOUS_ID)
        assert spws == []

    def test_spw_remap_is_applied(self, test_db_with_targets):
        # Remap SPW 17 → 99
        remap = json.dumps({"17": "99"})
        conn = sqlite3.connect(test_db_with_targets)
        conn.execute(
            "UPDATE pipeline_state SET raw_data_spectral_remap=? WHERE mous_id=?",
            (remap, TEST_MOUS_ID),
        )
        conn.commit()
        conn.close()

        with get_db_connection(test_db_with_targets) as conn:
            spws = get_mous_spw_mapping(conn, TEST_MOUS_ID)

        assert 17 not in spws
        assert 99 in spws

    def test_spw_remap_does_not_affect_unmapped_spws(self, test_db_with_targets):
        # Only SPW 17 is remapped; 19, 21, 25 should be unchanged
        remap = json.dumps({"17": "99"})
        conn = sqlite3.connect(test_db_with_targets)
        conn.execute(
            "UPDATE pipeline_state SET raw_data_spectral_remap=? WHERE mous_id=?",
            (remap, TEST_MOUS_ID),
        )
        conn.commit()
        conn.close()

        with get_db_connection(test_db_with_targets) as conn:
            spws = get_mous_spw_mapping(conn, TEST_MOUS_ID)

        assert 19 in spws
        assert 21 in spws
        assert 25 in spws