"""
bootstrap.py
------------
Ensures the alma_ops module (in a sibling directory) is importable
from any script inside scripts/.
"""

import sys
from pathlib import Path

def setup_path():
    """
    Adds the project root (containing alma_ops/) to sys.path,
    so alma_ops.* can be imported from any script in scripts/.
    """
    scripts_dir = Path(__file__).resolve().parent
    project_root = scripts_dir.parent
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))