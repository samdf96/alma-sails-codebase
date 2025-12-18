# The Central Database

This file explains the current choices on the formatting of the central database, as it pertains to the metadata contained within, along with how the data pipeline will track itself.

An example of the schema used for the SQLite database is found here: [database_schema.sql](../../schemas/database_schema.sql).

## Overall Structure

The `database.db` file contains both raw metadata added from an ALMiner query, along with database tracking. The currently implemented database structure contains the following tables:

* projects : contains *project* specific information, inlcuidng the `project_code`, `pi_name`, and any associated publications tied to the project.
* mous : contains MOUS specific information including `mous_id`, `project_code`, and the corresponding data type.
* targets : contains *target* specific infomration including all the observation metadata (`ra`, `dec`, `source_name`, `t_exptime`, `[...]_freq_GHz`, etc.). This also includes the antenna array metadata (which can be parsed for information relating to which array was used). Finally, the targets table contains the important `obs_id` column, which states both the target name, along with the associated SPW - this is used to split out dataproducts in the splitting stage.

In addition to the tables created to hold the metadata of the ALMA targets, there is also an addition table to track all stages of data processing called `pipeline_state`. This has tracking information such as:

* `download_url`
* `download_status`
* `pre_selfcal_split_status`
* `pre_selfcal_listobs_status`
* `selfcal_status`
* `imaging_status`
* `cleanup_status`

These columns above are checked by the Prefect runner to get the current state of the pipeline for each `mous_id` in the `pipeline_state` table. The values that can be set are the following:

* `pending` : the default state
* `in_progress` : when jobs are launched, this entry prevents multiple identical flows (in addition to a Prefect-specific check by the worker)
* `complete` : marks the state as complete
* `error` : marks any potential failures by the flow

Additionally, there are also columns to hold the useful filepaths that are written by the pipeline throughout its major steps. These include:

* `mous_directory`: the top of the mous-specific directory
* `calibrated_products` : where the raw data landed from downloading
* `split_products_path` : locations of the split data products
* `selfcal_products_nonsub_path` : list of self calibrated products (non continuum subtracted)
* `selfcal_products_sub_path` : list of self calibrated products (continuum subtracted)
* `final_imaging_products_path` : list of final maps/cubes
