"""
init_database.py
----------------
Create a new pipeline database from schema/schema.sql, or check an existing one
against it.

    python3 alma-sails-codebase/scripts/init_database.py --db-path db/serpens_v2.db
    python3 alma-sails-codebase/scripts/init_database.py --check            # live DB

Create (the default) refuses to touch a file that already holds tables. A new
database is then populated with seed_database.py, which writes the reference
layer and the initial pending rows; every later table is filled by the pipeline.

--check builds the schema in memory and compares it with the database: tables,
columns (order, type, NOT NULL, default, primary key), foreign keys, unique
constraints and indexes. It prints every difference and exits 1 if there are
any. Run it after changing schema.sql and the live database together; a clean
check means a database built from schema.sql today matches the one in use.
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from alma_ops.config import DB_PATH, SCHEMA_PATH


def describe(conn: sqlite3.Connection) -> dict:
    """Structural description of every table and index in a database."""
    tables = {}
    for (name,) in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
    ):
        cols = [tuple(r[1:6]) for r in conn.execute(f"PRAGMA table_info('{name}')")]
        cols = [(n, (t or "").upper(), nn, d, pk) for n, t, nn, d, pk in cols]
        fks = sorted(tuple(r[2:5]) for r in conn.execute(f"PRAGMA foreign_key_list('{name}')"))
        uniques = sorted(
            tuple(c[2] for c in conn.execute(f"PRAGMA index_info('{idx[1]}')"))
            for idx in conn.execute(f"PRAGMA index_list('{name}')") if idx[2]
        )
        tables[name] = {"columns": cols, "foreign_keys": fks, "unique": uniques}
    indexes = {
        name: (tbl, tuple(c[2] for c in conn.execute(f"PRAGMA index_info('{name}')")))
        for name, tbl in conn.execute(
            "SELECT name, tbl_name FROM sqlite_master WHERE type='index' AND sql IS NOT NULL"
        )
    }
    return {"tables": tables, "indexes": indexes}


def differences(expected: dict, actual: dict) -> list[str]:
    """Human-readable differences between two describe() results."""
    out = []
    exp_t, act_t = expected["tables"], actual["tables"]
    for t in sorted(set(exp_t) - set(act_t)):
        out.append(f"missing table {t}")
    for t in sorted(set(act_t) - set(exp_t)):
        out.append(f"extra table {t} (not in schema.sql)")
    for t in sorted(set(exp_t) & set(act_t)):
        e, a = exp_t[t], act_t[t]
        e_cols = {c[0]: c for c in e["columns"]}
        a_cols = {c[0]: c for c in a["columns"]}
        for c in [c[0] for c in e["columns"] if c[0] not in a_cols]:
            out.append(f"{t}: missing column {c}")
        for c in [c[0] for c in a["columns"] if c[0] not in e_cols]:
            out.append(f"{t}: extra column {c}")
        for c in [c for c in e_cols if c in a_cols and e_cols[c] != a_cols[c]]:
            out.append(f"{t}.{c}: schema (type, notnull, default, pk) {e_cols[c][1:]} "
                       f"!= database {a_cols[c][1:]}")
        shared_e = [c[0] for c in e["columns"] if c[0] in a_cols]
        shared_a = [c[0] for c in a["columns"] if c[0] in e_cols]
        if shared_e != shared_a:
            out.append(f"{t}: column order differs")
        if e["foreign_keys"] != a["foreign_keys"]:
            out.append(f"{t}: foreign keys {e['foreign_keys']} != {a['foreign_keys']}")
        if e["unique"] != a["unique"]:
            out.append(f"{t}: unique constraints {e['unique']} != {a['unique']}")
    exp_i, act_i = expected["indexes"], actual["indexes"]
    for i in sorted(set(exp_i) - set(act_i)):
        out.append(f"missing index {i} on {exp_i[i][0]}{exp_i[i][1]}")
    for i in sorted(set(act_i) - set(exp_i)):
        out.append(f"extra index {i} on {act_i[i][0]}{act_i[i][1]}")
    for i in sorted(set(exp_i) & set(act_i)):
        if exp_i[i] != act_i[i]:
            out.append(f"index {i}: {exp_i[i]} != {act_i[i]}")
    return out


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0].strip(),
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--db-path", default=None,
                        help=f"Database to create or check (--check defaults to {DB_PATH})")
    parser.add_argument("--schema", default=str(SCHEMA_PATH), help="Schema file (default: %(default)s)")
    parser.add_argument("--check", action="store_true",
                        help="Compare an existing database with the schema instead of creating one")
    args = parser.parse_args()

    schema_sql = Path(args.schema).read_text()

    if args.check:
        db_path = Path(args.db_path or DB_PATH)
        if not db_path.exists():
            print(f"[ERROR] no database at {db_path}")
            sys.exit(1)
        ref = sqlite3.connect(":memory:")
        ref.executescript(schema_sql)
        # read-only: a check must never create or modify anything
        live = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
        diffs = differences(describe(ref), describe(live))
        if diffs:
            print(f"[DIFF] {db_path} does not match {args.schema}:")
            for d in diffs:
                print(f"  - {d}")
            sys.exit(1)
        print(f"[OK] {db_path} matches {args.schema} "
              f"({len(describe(ref)['tables'])} tables)")
        return

    if not args.db_path:
        parser.error("--db-path is required to create a database")
    db_path = Path(args.db_path)
    if db_path.exists():
        conn = sqlite3.connect(db_path)
        n = conn.execute("SELECT count(*) FROM sqlite_master "
                         "WHERE type='table' AND name NOT LIKE 'sqlite_%'").fetchone()[0]
        conn.close()
        if n:
            print(f"[ERROR] {db_path} already has {n} tables; not touching it. "
                  f"Use --check to compare it with the schema.")
            sys.exit(1)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.executescript(schema_sql)
    conn.commit()
    n = conn.execute(
        "SELECT count(*) FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
    ).fetchone()[0]
    conn.close()
    print(f"[OK] created {db_path} from {args.schema} ({n} tables)")
    print(f"     next: python3 alma-sails-codebase/scripts/seed_database.py <alminer.csv> "
          f"--db-path {db_path} --vlsr-kms <km/s>")


if __name__ == "__main__":
    main()
