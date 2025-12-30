"""
bootstrap.py
--------------------
Allows scripts in the `flows/` directory to import from the project root.
"""

# ---------------------------------------------------------------------
# imports
# ---------------------------------------------------------------------

import sys
from pathlib import Path


def setup_path():
    """Adds the project root to sys.path for imports from scripts in the other directories."""
    scripts_dir = Path(__file__).resolve().parent
    project_root = scripts_dir.parent

    # Add project root so imports from other directories work
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))
