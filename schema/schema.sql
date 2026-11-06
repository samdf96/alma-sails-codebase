-- ============================================================
-- ALMA-SAILS pipeline schema (complete)
-- ============================================================
-- The single source of truth for the database layout. A new database is
-- built from this file by scripts/init_database.py, which also checks an
-- existing database against it (--check). A schema change means editing this
-- file AND applying the same change to the live database by hand (ALTER TABLE
-- / CREATE TABLE), then confirming with `init_database.py --check`.
--
-- Layers:
--   Reference : alminer_metadata, projects, mous, targets  (seed_database.py)
--   MOUS FSM  : pipeline_state, state_transitions, run_log
--   Continuum : imaging_unit, mous_imaging_unit, imaging_pipeline_state,
--               imaging_state_transitions, imaging_run_log
--   Lines     : line_triage_*, line_admit_*, line_admit_detection,
--               line_imaging_*, line_imaging_window_override
--   SCOUSE    : scouse_unit, scouse_pipeline_state, scouse_state_transitions,
--               scouse_cluster
--
-- Paths are stored in canonical /arc/... form (alma_ops.db.to_canonical).
-- Columns that were added to a table after it was first created sit at the
-- end of its column list, so a fresh database has the same column order as
-- the live one.
-- ============================================================

PRAGMA foreign_keys = ON;

-- ============================================================
-- REFERENCE LAYER
-- ============================================================

-- Raw ALMiner query output. One row per (MOUS, target) as returned
-- by ALMiner. Never written to by the pipeline — import only.
CREATE TABLE alminer_metadata (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    mous_id             TEXT NOT NULL,
    project_code        TEXT,
    proposal_id         TEXT,
    group_ous_uid       TEXT,
    obs_id              TEXT,
    asdm_uid            TEXT,
    obs_collection      TEXT,
    obs_publisher_did   TEXT,
    obs_title           TEXT,
    obs_type            TEXT,
    scan_intent         TEXT,
    obs_creator_name    TEXT,
    obs_release_date    TEXT,
    facility_name       TEXT,
    instrument_name     TEXT,
    antenna_arrays      TEXT,
    dataproduct_type    TEXT,
    calib_level         INTEGER,
    access_url          TEXT,
    access_format       TEXT,
    data_rights         TEXT,
    qa2_passed          TEXT,
    pwv                 REAL,
    scientific_category TEXT,
    science_keyword     TEXT,
    -- target-level fields
    alma_source_name    TEXT,
    ra_deg              REAL,
    dec_deg             REAL,
    gal_longitude       REAL,
    gal_latitude        REAL,
    ang_res_arcsec      REAL,
    las_arcsec          REAL,
    fov_arcsec          REAL,
    is_mosaic           TEXT,
    min_freq_GHz        REAL,
    max_freq_GHz        REAL,
    central_freq_GHz    REAL,
    bandwidth_GHz       REAL,
    freq_res_kHz        REAL,
    vel_res_kms         REAL,
    em_min              REAL,
    em_max              REAL,
    em_res_power        REAL,
    em_xel              INTEGER,
    cont_sens_bandwidth REAL,
    line_sens_10kms     REAL,
    line_sens_native    REAL,
    t_min               REAL,
    t_max               REAL,
    t_exptime           REAL,
    t_resolution        REAL,
    spatial_scale_max   REAL,
    s_fov_deg           REAL,
    s_region            TEXT,
    s_resolution        REAL,
    band_list           TEXT,
    frequency_support   TEXT,
    pol_states          TEXT,
    -- publication metadata (project-level)
    proposal_abstract   TEXT,
    proposal_authors    TEXT,
    pi_userid           TEXT,
    pi_name             TEXT,
    bib_reference       TEXT,
    pub_title           TEXT,
    first_author        TEXT,
    authors             TEXT,
    pub_abstract        TEXT,
    publication_year    INTEGER,
    collections         TEXT
);

CREATE TABLE projects (
    project_code        TEXT PRIMARY KEY,
    proposal_id         TEXT,
    pi_name             TEXT,
    pi_userid           TEXT,
    proposal_authors    TEXT,
    proposal_abstract   TEXT,
    scientific_category TEXT,
    bib_reference       TEXT,
    pub_title           TEXT,
    first_author        TEXT,
    authors             TEXT,
    pub_abstract        TEXT,
    publication_year    INTEGER
);

CREATE TABLE mous (
    mous_id             TEXT PRIMARY KEY,
    project_code        TEXT REFERENCES projects(project_code),
    group_ous_uid       TEXT,
    band                TEXT,
    array_config        TEXT,
    obs_type            TEXT,
    scan_intent         TEXT,
    qa2_passed          TEXT,
    obs_release_date    TEXT,
    obs_title           TEXT,
    obs_creator_name    TEXT,
    access_url          TEXT,
    ang_res_arcsec      REAL,
    las_arcsec          REAL,
    t_exptime           REAL,
    pwv                 REAL,
    scientific_category TEXT,
    science_keyword     TEXT
);

-- One row per target within a MOUS. A MOUS can contain multiple targets.
CREATE TABLE targets (
    target_id           INTEGER PRIMARY KEY AUTOINCREMENT,
    mous_id             TEXT NOT NULL REFERENCES mous(mous_id),
    alma_source_name    TEXT NOT NULL,
    band                TEXT,
    array_config        TEXT,
    ra_deg              REAL,
    dec_deg             REAL,
    gal_longitude       REAL,
    gal_latitude        REAL,
    ang_res_arcsec      REAL,
    las_arcsec          REAL,
    fov_arcsec          REAL,
    is_mosaic           TEXT,
    min_freq_GHz        REAL,
    max_freq_GHz        REAL,
    central_freq_GHz    REAL,
    bandwidth_GHz       REAL,
    freq_res_kHz        REAL,
    vel_res_kms         REAL,
    cont_sens_bandwidth REAL,
    line_sens_10kms     REAL,
    line_sens_native    REAL,
    t_exptime           REAL,
    frequency_support   TEXT,
    pol_states          TEXT,
    -- source systemic velocity (km/s, LSRK); set by seed_database.py --vlsr-kms.
    -- ADMIT and deep line imaging refuse to run without it.
    vlsr_kms            REAL
);

-- ============================================================
-- MOUS PIPELINE STATE
-- ============================================================

-- Valid states: pending | downloaded | split_complete |
--               selfcal_running | selfcal_complete | failed
CREATE TABLE pipeline_state (
    mous_id             TEXT PRIMARY KEY REFERENCES mous(mous_id),
    current_state       TEXT NOT NULL DEFAULT 'pending',
    updated_at          TEXT NOT NULL,
    job_id              TEXT,           -- cluster job ID when selfcal_running
    failure_count       INTEGER NOT NULL DEFAULT 0,
    last_transition_id  INTEGER REFERENCES state_transitions(id),
    -- SRDP download URL — set when download is triggered, retained for retries
    download_url        TEXT,
    -- product paths recorded as pipeline advances (stored as canonical /arc/... paths)
    mous_directory      TEXT,
    split_products      TEXT,           -- JSON array of paths
    selfcal_products    TEXT,           -- JSON array of paths (non-continuum-subtracted)
    selfcal_products_sub TEXT           -- JSON array of paths (continuum-subtracted)
);

-- FSM transition log for MOUSes.
-- from_state is NULL for the initial seeded entry.
CREATE TABLE state_transitions (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    mous_id     TEXT NOT NULL REFERENCES mous(mous_id),
    from_state  TEXT,
    to_state    TEXT NOT NULL,
    at          TEXT NOT NULL,          -- ISO 8601 timestamp
    run_id      TEXT,
    note        TEXT
);

-- Detailed step-level logging per pipeline run
CREATE TABLE run_log (
    id      INTEGER PRIMARY KEY AUTOINCREMENT,
    mous_id TEXT NOT NULL REFERENCES mous(mous_id),
    run_id  TEXT NOT NULL,
    step    TEXT NOT NULL,
    at      TEXT NOT NULL,
    payload TEXT,                       -- JSON blob
    level   TEXT NOT NULL DEFAULT 'INFO'
);

-- ============================================================
-- IMAGING
-- ============================================================

-- One row per (target, band, config_type) combination.
-- Seeded once all contributing MOUSes are known.
CREATE TABLE imaging_unit (
    imaging_unit_id INTEGER PRIMARY KEY AUTOINCREMENT,
    target_name     TEXT NOT NULL,
    band            TEXT NOT NULL,
    config_type     TEXT NOT NULL,      -- 12m | 7m | tp | combined
    UNIQUE (target_name, band, config_type)
);

-- Many-to-many: which MOUSes contribute data to which imaging unit
CREATE TABLE mous_imaging_unit (
    mous_id         TEXT NOT NULL REFERENCES mous(mous_id),
    imaging_unit_id INTEGER NOT NULL REFERENCES imaging_unit(imaging_unit_id),
    -- Let one shared-name mosaic MOUS back several imaging units, each pinned
    -- to a subset of its fields. NULL in both = the whole field named
    -- imaging_unit.target_name, which is every single-pointing unit.
    source_field_name TEXT,             -- CASA field name in this MOUS's MS
    field_ids         TEXT,             -- JSON list of CASA field ids, e.g. "[12,34]"
    PRIMARY KEY (mous_id, imaging_unit_id)
);

-- Valid states: pending | ready | running | complete | failed
-- pending : contributing MOUSes not yet selfcal_complete
-- ready   : all contributing MOUSes are selfcal_complete
CREATE TABLE imaging_pipeline_state (
    imaging_unit_id     INTEGER PRIMARY KEY REFERENCES imaging_unit(imaging_unit_id),
    target_name         TEXT NOT NULL,
    band                TEXT NOT NULL,
    config_type         TEXT NOT NULL,
    current_state       TEXT NOT NULL DEFAULT 'pending',
    updated_at          TEXT NOT NULL,
    job_id              TEXT,           -- cluster job ID when running
    failure_count       INTEGER NOT NULL DEFAULT 0,
    last_transition_id  INTEGER REFERENCES imaging_state_transitions(id),
    imaging_products    TEXT            -- JSON array of output image paths
);

-- FSM transition log for imaging units.
-- from_state is NULL for the initial seeded entry.
CREATE TABLE imaging_state_transitions (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    imaging_unit_id INTEGER NOT NULL REFERENCES imaging_unit(imaging_unit_id),
    target_name     TEXT NOT NULL,
    band            TEXT NOT NULL,
    config_type     TEXT NOT NULL,
    from_state      TEXT,
    to_state        TEXT NOT NULL,
    at              TEXT NOT NULL,
    run_id          TEXT,
    note            TEXT
);

-- Detailed step-level logging per imaging run
CREATE TABLE imaging_run_log (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    imaging_unit_id INTEGER NOT NULL REFERENCES imaging_unit(imaging_unit_id),
    target_name     TEXT NOT NULL,
    band            TEXT NOT NULL,
    config_type     TEXT NOT NULL,
    run_id          TEXT NOT NULL,
    step            TEXT NOT NULL,
    at              TEXT NOT NULL,
    payload         TEXT,
    level           TEXT NOT NULL DEFAULT 'INFO'
);

-- ============================================================
-- LINE TRIAGE  (dirty cube per (MOUS, target, spw), ADMIT input)
-- ============================================================

-- spw_idx = -1 marks a continuum-only sentinel: the MOUS/target has no
-- line spws, recorded so it is not picked up again.
-- Valid states: ready | running | complete | failed
CREATE TABLE line_triage_unit (
    line_triage_unit_id INTEGER PRIMARY KEY AUTOINCREMENT,
    mous_id             TEXT NOT NULL REFERENCES mous(mous_id),
    target_name         TEXT NOT NULL,
    spw_idx             INTEGER NOT NULL,
    UNIQUE (mous_id, target_name, spw_idx)
);

CREATE TABLE line_triage_pipeline_state (
    line_triage_unit_id INTEGER PRIMARY KEY REFERENCES line_triage_unit(line_triage_unit_id),
    mous_id             TEXT NOT NULL,
    target_name         TEXT NOT NULL,
    spw_idx             INTEGER NOT NULL,
    current_state       TEXT NOT NULL DEFAULT 'ready',
    updated_at          TEXT NOT NULL,
    job_id              TEXT,
    failure_count       INTEGER NOT NULL DEFAULT 0,
    last_transition_id  INTEGER REFERENCES line_triage_state_transitions(id),
    triage_products     TEXT            -- JSON: cube paths + the parameters to rebuild it
);

CREATE TABLE line_triage_state_transitions (
    id                   INTEGER PRIMARY KEY AUTOINCREMENT,
    line_triage_unit_id  INTEGER NOT NULL,
    mous_id              TEXT NOT NULL,
    target_name          TEXT NOT NULL,
    spw_idx              INTEGER NOT NULL,
    from_state           TEXT,
    to_state             TEXT NOT NULL,
    at                   TEXT NOT NULL,
    run_id               TEXT,
    note                 TEXT
);

CREATE TABLE line_triage_run_log (
    id                   INTEGER PRIMARY KEY AUTOINCREMENT,
    line_triage_unit_id  INTEGER NOT NULL,
    mous_id              TEXT NOT NULL,
    target_name          TEXT NOT NULL,
    spw_idx              INTEGER NOT NULL,
    run_id               TEXT NOT NULL,
    step                 TEXT NOT NULL,
    at                   TEXT NOT NULL,
    payload              TEXT,
    level                TEXT NOT NULL DEFAULT 'INFO'
);

-- ============================================================
-- ADMIT LINE IDENTIFICATION  (one run per completed triage unit)
-- ============================================================

-- Valid states: ready | running | complete | failed
CREATE TABLE line_admit_unit (
    line_admit_unit_id  INTEGER PRIMARY KEY AUTOINCREMENT,
    line_triage_unit_id INTEGER NOT NULL UNIQUE REFERENCES line_triage_unit(line_triage_unit_id)
);

CREATE TABLE line_admit_pipeline_state (
    line_admit_unit_id  INTEGER PRIMARY KEY REFERENCES line_admit_unit(line_admit_unit_id),
    line_triage_unit_id INTEGER NOT NULL,
    mous_id             TEXT NOT NULL,
    target_name         TEXT NOT NULL,
    spw_idx             INTEGER NOT NULL,
    current_state       TEXT NOT NULL DEFAULT 'ready',
    updated_at          TEXT NOT NULL,
    job_id              TEXT,
    failure_count       INTEGER NOT NULL DEFAULT 0,
    last_transition_id  INTEGER REFERENCES line_admit_state_transitions(id),
    admit_products      TEXT
);

CREATE TABLE line_admit_state_transitions (
    id                   INTEGER PRIMARY KEY AUTOINCREMENT,
    line_admit_unit_id   INTEGER NOT NULL,
    line_triage_unit_id  INTEGER NOT NULL,
    mous_id              TEXT NOT NULL,
    target_name          TEXT NOT NULL,
    spw_idx              INTEGER NOT NULL,
    from_state           TEXT,
    to_state             TEXT NOT NULL,
    at                   TEXT NOT NULL,
    run_id               TEXT,
    note                 TEXT
);

CREATE TABLE line_admit_run_log (
    id                   INTEGER PRIMARY KEY AUTOINCREMENT,
    line_admit_unit_id   INTEGER NOT NULL,
    line_triage_unit_id  INTEGER NOT NULL,
    mous_id              TEXT NOT NULL,
    target_name          TEXT NOT NULL,
    spw_idx              INTEGER NOT NULL,
    run_id               TEXT NOT NULL,
    step                 TEXT NOT NULL,
    at                   TEXT NOT NULL,
    payload              TEXT,
    level                TEXT NOT NULL DEFAULT 'INFO'
);

-- One row per line ADMIT identified. review_status is set by
-- review_line_detections.py: pending | confirmed | rejected.
CREATE TABLE line_admit_detection (
    line_admit_detection_id INTEGER PRIMARY KEY AUTOINCREMENT,
    line_admit_unit_id      INTEGER NOT NULL REFERENCES line_admit_unit(line_admit_unit_id),
    frequency_ghz           REAL NOT NULL,
    uid                     TEXT NOT NULL,
    formula                 TEXT,
    species_name            TEXT,
    transition              TEXT,
    velocity_kms            REAL,
    peakrms_sigma           REAL,
    peakintensity_jybeam    REAL,
    startchan               INTEGER,
    endchan                 INTEGER,
    blend_id                INTEGER,
    review_status           TEXT NOT NULL DEFAULT 'pending',
    reviewed_at             TEXT,
    reviewed_note           TEXT
);

-- ============================================================
-- DEEP LINE IMAGING  (one clean per confirmed detection)
-- ============================================================

-- Valid states: ready | running | complete | failed
CREATE TABLE line_imaging_unit (
    line_imaging_unit_id    INTEGER PRIMARY KEY AUTOINCREMENT,
    line_admit_detection_id INTEGER NOT NULL UNIQUE REFERENCES line_admit_detection(line_admit_detection_id)
);

CREATE TABLE line_imaging_pipeline_state (
    line_imaging_unit_id    INTEGER PRIMARY KEY REFERENCES line_imaging_unit(line_imaging_unit_id),
    line_admit_detection_id INTEGER NOT NULL,
    mous_id                 TEXT NOT NULL,
    target_name             TEXT NOT NULL,
    spw_idx                 INTEGER NOT NULL,
    species_name            TEXT,
    frequency_ghz           REAL NOT NULL,
    current_state           TEXT NOT NULL DEFAULT 'ready',
    updated_at              TEXT NOT NULL,
    job_id                  TEXT,
    failure_count           INTEGER NOT NULL DEFAULT 0,
    last_transition_id      INTEGER REFERENCES line_imaging_state_transitions(id),
    imaging_products        TEXT        -- JSON list, one entry per robust value
);

CREATE TABLE line_imaging_state_transitions (
    id                       INTEGER PRIMARY KEY AUTOINCREMENT,
    line_imaging_unit_id     INTEGER NOT NULL,
    line_admit_detection_id  INTEGER NOT NULL,
    mous_id                  TEXT NOT NULL,
    target_name              TEXT NOT NULL,
    spw_idx                  INTEGER NOT NULL,
    species_name             TEXT,
    frequency_ghz            REAL,
    from_state               TEXT,
    to_state                 TEXT NOT NULL,
    at                       TEXT NOT NULL,
    run_id                   TEXT,
    note                     TEXT
);

CREATE TABLE line_imaging_run_log (
    id                       INTEGER PRIMARY KEY AUTOINCREMENT,
    line_imaging_unit_id     INTEGER NOT NULL,
    line_admit_detection_id  INTEGER NOT NULL,
    mous_id                  TEXT NOT NULL,
    target_name              TEXT NOT NULL,
    spw_idx                  INTEGER NOT NULL,
    species_name             TEXT,
    frequency_ghz            REAL,
    run_id                   TEXT NOT NULL,
    step                     TEXT NOT NULL,
    at                       TEXT NOT NULL,
    payload                  TEXT,
    level                    TEXT NOT NULL DEFAULT 'INFO'
);

-- ============================================================
-- SCOUSE + ACORNS  (one unit per (source, line))
-- ============================================================

CREATE TABLE scouse_unit (
    scouse_unit_id  INTEGER PRIMARY KEY AUTOINCREMENT,
    source_name     TEXT NOT NULL,
    line_label      TEXT NOT NULL,
    cube_fits_path  TEXT NOT NULL,
    output_dir      TEXT NOT NULL,
    created_at      TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(source_name, line_label)
);

-- current_state is the last completed stage:
--   pending | prep_complete | stage1_complete | stage2_complete |
--   stage3_complete | stage4_complete | complete
-- The job_* columns describe the run of the NEXT stage (batch_scouse.py
-- session or a terminal run); job_status is NULL when nothing is in flight.
CREATE TABLE scouse_pipeline_state (
    scouse_unit_id       INTEGER PRIMARY KEY REFERENCES scouse_unit(scouse_unit_id),
    source_name          TEXT NOT NULL,
    line_label           TEXT NOT NULL,
    current_state        TEXT NOT NULL DEFAULT 'pending',
    updated_at           TEXT NOT NULL,
    parameters_json      TEXT,          -- prep, stage-1 GUI and resolved ACORNS parameters
    n_fitted_components  INTEGER,
    n_acorns_clusters    INTEGER,
    acorns_output_path   TEXT,          -- the narrow tree, else the broad one
    failure_count        INTEGER NOT NULL DEFAULT 0,
    job_stage            TEXT,          -- '1' | '2' | '3' | '4' | 'acorns'
    job_id               TEXT,          -- CANFAR session id, or 'local:<host>:<pid>'
    job_status           TEXT,          -- NULL | 'submitted' | 'running' | 'failed'
    job_submitted_at     TEXT,          -- UTC ISO
    job_note             TEXT           -- error text of a failed job
);

CREATE TABLE scouse_state_transitions (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    scouse_unit_id  INTEGER NOT NULL REFERENCES scouse_unit(scouse_unit_id),
    source_name     TEXT NOT NULL,
    line_label      TEXT NOT NULL,
    from_state      TEXT,
    to_state        TEXT NOT NULL,
    at              TEXT NOT NULL,
    note            TEXT
);

-- Optional human-set velocity range per detection, used by deep line imaging
-- in place of the window derived from ADMIT. Kept apart from
-- line_admit_detection so ADMIT's own output is never rewritten.
-- Set with review_line_detections.py --set-window.
CREATE TABLE line_imaging_window_override (
    line_admit_detection_id INTEGER PRIMARY KEY REFERENCES line_admit_detection(line_admit_detection_id),
    v_lo_kms                REAL NOT NULL,
    v_hi_kms                REAL NOT NULL,
    origin                  TEXT NOT NULL DEFAULT 'manual',
    note                    TEXT,
    created_at              TEXT NOT NULL,
    CHECK (v_lo_kms < v_hi_kms)
);

-- One row per ACORNS cluster (every node of the hierarchy, not only the top
-- level), written by manual_acorns_run.py. component_set is 'narrow' or
-- 'broad' (the narrow/broad sigma split each ACORNS run clusters separately).
-- Shape and velocity-gradient columns come from scouse_utils.cluster_geometry;
-- position_angle_deg and vgrad_pa_deg are degrees east of north.
CREATE TABLE scouse_cluster (
    scouse_cluster_id         INTEGER PRIMARY KEY AUTOINCREMENT,
    scouse_unit_id            INTEGER NOT NULL REFERENCES scouse_unit(scouse_unit_id),
    source_name               TEXT NOT NULL,
    line_label                TEXT NOT NULL,
    component_set             TEXT NOT NULL DEFAULT 'all',
    acorns_cluster_id         INTEGER NOT NULL,
    n_components              INTEGER NOT NULL,   -- includes descendants' components
    mean_x_pix                REAL,
    mean_y_pix                REAL,
    mean_vel_kms              REAL,
    vel_range_min_kms         REAL,
    vel_range_max_kms         REAL,
    vel_disp_kms              REAL,
    peak_amplitude_jybeam     REAL,
    mean_sigma_kms            REAL,     -- Gaussian sigma (scousepy 'width'); FWHM = 2.3548 x this
    spatial_extent_arcsec     REAL,
    aspect_ratio              REAL,
    position_angle_deg        REAL,
    label                     TEXT,
    notes                     TEXT,
    created_at                TEXT NOT NULL DEFAULT (datetime('now')),
    parent_acorns_cluster_id  INTEGER,  -- NULL for a top-level cluster
    is_top_level              INTEGER,
    major_length_arcsec       REAL,     -- 5th-95th percentile extent along the major axis
    minor_width_arcsec        REAL,     -- the same along the minor axis
    vgrad_kms_per_arcsec      REAL,     -- plane fit of velocity over position
    vgrad_pa_deg              REAL,     -- direction of increasing velocity
    vgrad_resid_kms           REAL,     -- rms velocity residual about the plane
    vgrad_r2                  REAL,     -- fraction of velocity variance the plane explains
    vgrad_misalign_deg        REAL,     -- angle between gradient and major axis (0-90)
    UNIQUE(scouse_unit_id, component_set, acorns_cluster_id)
);

CREATE INDEX idx_scouse_cluster_top_level
    ON scouse_cluster(scouse_unit_id, component_set, is_top_level);
