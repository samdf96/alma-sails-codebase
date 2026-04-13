"""
test_utils.py
-------------
Unit tests for MOUS ID conversion utilities in alma_ops/utils.py.
"""

import pytest

from alma_ops.utils import to_db_mous_id, to_dir_mous_id


# ---------------------------------------------------------------------------
# to_db_mous_id
# ---------------------------------------------------------------------------


class TestToDbMousId:
    def test_already_db_form_is_unchanged(self):
        mous_id = "uid://A001/X1234/Xabc"
        assert to_db_mous_id(mous_id) == mous_id

    def test_converts_dir_form_to_db_form(self):
        assert to_db_mous_id("uid___A001_X1234_Xabc") == "uid://A001/X1234/Xabc"

    def test_strips_leading_whitespace(self):
        assert to_db_mous_id("  uid://A001/X1234/Xabc") == "uid://A001/X1234/Xabc"

    def test_strips_trailing_whitespace(self):
        assert to_db_mous_id("uid___A001_X1234_Xabc  ") == "uid://A001/X1234/Xabc"

    def test_dir_form_wrong_part_count_raises(self):
        # only two underscore-separated parts instead of three
        with pytest.raises(ValueError, match="Invalid uid___"):
            to_db_mous_id("uid___A001_X1234")

    def test_unrecognised_prefix_raises(self):
        with pytest.raises(ValueError, match="Cannot interpret"):
            to_db_mous_id("A001/X1234/Xabc")

    def test_empty_string_raises(self):
        with pytest.raises(ValueError, match="Cannot interpret"):
            to_db_mous_id("")

    def test_real_world_mous_id(self):
        assert to_db_mous_id("uid___A001_X1982_X19b") == "uid://A001/X1982/X19b"


# ---------------------------------------------------------------------------
# to_dir_mous_id
# ---------------------------------------------------------------------------


class TestToDirMousId:
    def test_already_dir_form_is_unchanged(self):
        mous_id = "uid___A001_X1234_Xabc"
        assert to_dir_mous_id(mous_id) == mous_id

    def test_converts_db_form_to_dir_form(self):
        assert to_dir_mous_id("uid://A001/X1234/Xabc") == "uid___A001_X1234_Xabc"

    def test_strips_leading_whitespace(self):
        assert to_dir_mous_id("  uid___A001_X1234_Xabc") == "uid___A001_X1234_Xabc"

    def test_strips_trailing_whitespace(self):
        assert to_dir_mous_id("uid://A001/X1234/Xabc  ") == "uid___A001_X1234_Xabc"

    def test_db_form_wrong_part_count_raises(self):
        # only two slash-separated parts after uid://
        with pytest.raises(ValueError, match="Invalid uid://"):
            to_dir_mous_id("uid://A001/X1234")

    def test_unrecognised_prefix_raises(self):
        with pytest.raises(ValueError, match="Cannot interpret"):
            to_dir_mous_id("A001/X1234/Xabc")

    def test_empty_string_raises(self):
        with pytest.raises(ValueError, match="Cannot interpret"):
            to_dir_mous_id("")

    def test_real_world_mous_id(self):
        assert to_dir_mous_id("uid://A001/X1982/X19b") == "uid___A001_X1982_X19b"


# ---------------------------------------------------------------------------
# Round-trips
# ---------------------------------------------------------------------------


def test_db_to_dir_to_db_roundtrip():
    db_id = "uid://A001/X1234/Xabc"
    assert to_db_mous_id(to_dir_mous_id(db_id)) == db_id


def test_dir_to_db_to_dir_roundtrip():
    dir_id = "uid___A001_X1234_Xabc"
    assert to_dir_mous_id(to_db_mous_id(dir_id)) == dir_id