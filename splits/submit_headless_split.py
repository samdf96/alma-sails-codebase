# alma_ops/splits/submit_headless_split.py

from datetime import datetime
from pathlib import Path

from canfar.images import Images
from canfar.sessions import Session

from alma_ops.config import CASA_IMAGE
from alma_ops.logging import get_logger
from alma_ops.utils import to_dir_mous_id

log = get_logger(__name__)


def submit_headless_split(
    mous_id: str, db_path: str, datasets_dir: str, job_list: dict, dry_run: bool
):

    mous_dir = Path(datasets_dir) / to_dir_mous_id(mous_id)

    # Initialize session manager
    session = Session()

    job_ids = []
    for i, job in enumerate(job_list):
        vis_path = job["asdm_path"]
        outputvis_path = job["output_ms"]
        field = job["field"]
        spws = job["spw"]

        job_name = f"casa-{datetime.now().strftime('%Y%m%d')}-splits-{i:02d}"
        logfile_name = (
            f"casa-{datetime.now().strftime('%Y%m%d-%H%M%S')}-mous-split-{i:02d}.log"
        )
        logfile_path = mous_dir / logfile_name

        # need to build string for args
        spw_str = ",".join(str(s) for s in spws)

        args_str = " ".join(
            [
                "--logfile",
                str(logfile_path),
                "-c",
                "/arc/projects/ALMA-SAILS/alma_ops/splits/run_headless_split.py",
                f"--vis {vis_path}",
                f"--outputvis {outputvis_path}",
                f"--field {field}",
                f"--spws {spw_str}",
            ]
        )

        job_id = session.create(
            name=job_name,
            image=CASA_IMAGE,
            cmd="casa",
            args=args_str,
        )

        # Validate the result
        if not isinstance(job_id, list) or len(job_id) == 0:
            log.error(f"Job submission failed for index {i:02d}. No job created.")
            continue

        # add job_ids to master list
        job_ids.append(job_id)

        # ---- Logging for each submitted job ----
        log.info(f"[{i:02d}] Submitted job:")
        log.info(f"      Job ID:     {job_id}")
        log.info(f"      Job name:   {job_name}")
        log.info(f"      Log file:   {logfile_path}")
        log.info(f"      vis:        {vis_path}")
        log.info(f"      output:     {outputvis_path}")
        log.info(f"      field:      {field}")
        log.info(f"      spw:        {','.join(str(s) for s in spws)}")
        log.info("")
        log.info(f"      → To view job info:  canfar info {job_id[0]}")
        log.info("")

    # summary printout
    log.info(f"Total split jobs submitted: {len(job_ids)}")
    log.info(f"MOUS: {mous_id}")

    return job_ids
