"""
models.py
---------
Result dataclasses returned by pipeline work units.

Each class represents the output of one major pipeline step. Work units
populate these and return them; the orchestration layer passes them to
the state layer (record_transition) as the run payload.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class DownloadResult:
    mous_id: str
    success: bool
    download_path: Path | None
    size_bytes: int | None
    sha1_verified: bool
    duration_s: float
    error: str | None = None


@dataclass
class SplitResult:
    mous_id: str
    success: bool
    split_products: list[Path]
    source_names: list[str]
    duration_s: float
    spw_map: dict = field(default_factory=dict)  # {original_spw_int: new_index_int}
    error: str | None = None


@dataclass
class SelfcalResult:
    mous_id: str
    success: bool
    products_nonsub: list[Path]
    products_sub: list[Path]
    duration_s: float
    error: str | None = None
    integrity_checks_skipped: bool = False


@dataclass
class ImagingResult:
    imaging_unit_id: int
    target_name: str
    band: str
    config_type: str
    success: bool
    image_paths: list[Path]
    duration_s: float
    error: str | None = None