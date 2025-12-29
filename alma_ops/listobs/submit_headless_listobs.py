# alma_ops/listobs/submit_headless_listobs.py

from datetime import datetime
from pathlib import Path

from canfar.images import Images
from canfar.sessions import Session

from alma_ops.config import CASA_IMAGE
from alma_ops.logging import get_logger
from alma_ops.utils import to_dir_mous_id

log = get_logger(__name__)


def submit_headless_listobs(
    mous_id: str, db_path: str, datasets_dir: str, job_list: dict, dry_run: bool
):
    pass
