from tabulate import tabulate

CHECK = "🟢"
CROSS = "🔴"


def _is_ok_status(record) -> bool:
    """
    Interpret a single check result dict as 'ok' or not.

    Expected record shape: {"mous_id": ..., "status": ..., "note": ...}
    """
    if record is None:
        return False

    val = record.get("status", "")
    if isinstance(val, bool):
        return val

    s = str(val).strip().lower()
    # adjust these rules if your checkers use different labels
    if "ok" in s or "✅" in s:
        return True
    if "missing" in s or "no data" in s or "❌" in s:
        return False
    if "partial" in s or "⚠" in s:
        return False  # treat partial as NOT "ok" for presence
    return False


def summarize_results(raw, splits, listobs, db_path, show_table=False):
    """
    Summarize project-level and per-MOUS status across RAW, SPLITS, LISTOBS stages.

    Parameters
    ----------
    raw : list[dict]
        Output from check_for_raw_asdms()
    splits : list[dict]
        Output from check_for_split_products()
    listobs : list[dict]
        Output from check_for_listobs()
    db_path : str
        Path to the database (for summary header)
    show_table : bool
        If True, print the detailed per-MOUS status table.
    """

    # ------------------------------------------------------------
    # 1. Build index: mous_id -> record for each check
    # ------------------------------------------------------------
    raw_by_mous = {r["mous_id"]: r for r in raw}
    splits_by_mous = {r["mous_id"]: r for r in splits}
    listobs_by_mous = {r["mous_id"]: r for r in listobs}

    # union of all MOUS IDs we saw in any check
    mous_ids = sorted(set(raw_by_mous) | set(splits_by_mous) | set(listobs_by_mous))

    # ------------------------------------------------------------
    # 2. File-level presence (no priority override!)
    #    This is what we use for the global counts.
    # ------------------------------------------------------------
    files_present = {}
    for mid in mous_ids:
        files_present[mid] = {
            "raw": _is_ok_status(raw_by_mous.get(mid)),
            "splits": _is_ok_status(splits_by_mous.get(mid)),
            "listobs": _is_ok_status(listobs_by_mous.get(mid)),
        }

    # global stats (what actually exists on disk)
    total_mous = len(mous_ids)
    num_raw_ok = sum(1 for st in files_present.values() if st["raw"])
    num_splits_ok = sum(1 for st in files_present.values() if st["splits"])
    num_listobs_ok = sum(1 for st in files_present.values() if st["listobs"])

    # ------------------------------------------------------------
    # 3. Stage-level view for the table, with priority override:
    #    LISTOBS → SPLITS → RAW
    # ------------------------------------------------------------
    stage_status = {mid: st.copy() for mid, st in files_present.items()}

    for mid, st in stage_status.items():
        if st["listobs"]:
            st["splits"] = True
            st["raw"] = True
        elif st["splits"]:
            st["raw"] = True
        # if only raw is True, we leave it as-is

    # ------------------------------------------------------------
    # 4. Print global summary (based on actual files_present)
    # ------------------------------------------------------------
    print("\n==============================")
    print("  ALMA-SAILS PROJECT SUMMARY")
    print("==============================")
    print(f"Database: {db_path}")
    print(f"Total MOUS entries:          {total_mous}")
    print(f"Raw ASDMs present:           {num_raw_ok}/{total_mous}")
    print(f"Split products present:      {num_splits_ok}/{total_mous}")
    print(f"Listobs products present:    {num_listobs_ok}/{total_mous}")

    # ------------------------------------------------------------
    # 5. Optional per-MOUS table (using stage_status with override)
    # ------------------------------------------------------------
    if show_table:
        table = []
        for mid in mous_ids:
            st = stage_status[mid]
            table.append([
                mid,
                CHECK if st["raw"] else CROSS,
                CHECK if st["splits"] else CROSS,
                CHECK if st["listobs"] else CROSS,
            ])

        table = sorted(table, key=lambda row: row[0])
        
        print("\n=== PER-MOUS STATUS TABLE ===")
        print(
            tabulate(
                table,
                headers=["MOUS ID", "RAW ASDMs", "SPLITS", "LISTOBS"],
                tablefmt="grid",
            )
        )
