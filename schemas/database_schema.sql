-- ===============================
-- PROJECT METADATA
-- ===============================

CREATE TABLE projects (
    project_code TEXT PRIMARY KEY,
    proposal_id TEXT,
    proposal_abstract TEXT,
    proposal_authors TEXT,
    pi_userid TEXT,
    pi_name TEXT,
    scientific_category TEXT,
    bib_reference TEXT,
    pub_title TEXT,
    first_author TEXT,
    authors TEXT,
    pub_abstract TEXT,
    publication_year INTEGER
);

-- ===============================
-- MOUS METADATA
-- ===============================

CREATE TABLE mous (
    mous_id TEXT PRIMARY KEY,
    project_code TEXT REFERENCES projects(project_code),
    group_ous_uid TEXT,
    num_asdms TEXT,
    obs_collection TEXT,
    collections TEXT,
    obs_publisher_did TEXT,
    facility_name TEXT,
    instrument_name TEXT,
    antenna_arrays TEXT,
    dataproduct_type TEXT,
    calib_level INTEGER,
    access_url TEXT,
    access_format TEXT,
    data_rights TEXT,
    obs_title TEXT,
    scan_intent TEXT,
    obs_type TEXT,              -- renamed from 'type'
    obs_creator_name TEXT,
    obs_release_date TEXT,
    qa2_passed TEXT,
    pwv REAL,
    scientific_category TEXT,
    science_keyword TEXT
);

-- ===============================
-- TARGET METADATA
-- ===============================

CREATE TABLE targets (
    target_id INTEGER PRIMARY KEY AUTOINCREMENT,
    mous_id TEXT REFERENCES mous(mous_id),
    alma_source_name TEXT,
    obs_id TEXT,
    ra_deg REAL,
    dec_deg REAL,
    gal_longitude REAL,
    gal_latitude REAL,
    ang_res_arcsec REAL,
    las_arcsec REAL,
    fov_arcsec REAL,
    is_mosaic TEXT,
    min_freq_GHz REAL,
    max_freq_GHz REAL,
    central_freq_GHz REAL,
    bandwidth_GHz REAL,
    freq_res_kHz REAL,
    vel_res_kms REAL,
    em_min REAL,
    em_max REAL,
    em_res_power REAL,
    em_xel INTEGER,
    pwv REAL,
    asdm_uid TEXT,
    cont_sens_bandwidth REAL,
    line_sens_10kms REAL,
    line_sens_native REAL,
    t_min REAL,
    t_max REAL,
    t_exptime REAL,
    t_resolution REAL,
    spatial_scale_max REAL,
    s_fov_deg REAL,
    s_region TEXT,
    s_resolution REAL,
    band_list TEXT,
    frequency_support TEXT,
    pol_states TEXT,
    scientific_category TEXT,
    science_keyword TEXT,
    qa2_passed TEXT,
    obs_type TEXT,
    scan_intent TEXT
);

-- ===============================
-- PIPELINE STATE (DYNAMIC)
-- Tracks all stages of processing
-- ===============================

CREATE TABLE pipeline_state (
    mous_id TEXT PRIMARY KEY REFERENCES mous(mous_id),

    -- NRAO SRDP Download url
    download_url TEXT,

    -- High-level pipeline stages
    download_status TEXT DEFAULT 'pending',             -- pending | in_progress | downloaded | complete | error
    pre_selfcal_split_status TEXT DEFAULT 'pending',    -- pending | in_progress | split | complete | error
    pre_selfcal_listobs_status TEXT DEFAULT 'pending',  -- pending | in_progress | listed | complete | error
    selfcal_status TEXT DEFAULT 'pending',              -- pending | in_progress | prepped | selfcaled | complete | error
    imaging_status TEXT DEFAULT 'pending',
    cleanup_status TEXT DEFAULT 'pending',

    -- Step timestamps
    download_started_at TEXT,
    download_completed_at TEXT,
    selfcal_started_at TEXT,
    selfcal_completed_at TEXT,
    imaging_started_at TEXT,
    imaging_completed_at TEXT,
    cleanup_started_at TEXT,
    cleanup_completed_at TEXT,

    -- File paths useful even after cleanup cuts storage
    mous_directory TEXT DEFAULT '[]',               -- the top of the mous-specific directory
    calibrated_products TEXT DEFAULT '[]',          -- where the downloaded calibrated data landed
    split_products_path TEXT DEFAULT '[]',          -- list of split products
    selfcal_products_nonsub_path TEXT DEFAULT '[]', -- list of selfcalibrated products (non continuum subtracted)
    selfcal_products_sub_path TEXT DEFAULT '[]',    -- list of selfcalibrated products (continuum subtracted)
    final_imaging_products_path TEXT DEFAULT '[]',  -- location of final maps/cubes

    -- Cleanup notes or deletion records
    cleanup_notes TEXT,

    -- For debugging failed runs
    last_error_message TEXT,
    last_updated_at TEXT

    -- TODO: add information for raw datasets
    -- ADD: raw_data_spectral_remap TEXT
    -- ADD: preferred_datacolumn TEXT;
);