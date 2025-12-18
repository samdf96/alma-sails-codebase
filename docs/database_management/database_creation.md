## Creating the SQLite Database

First create a database in the `project-space/db` directory, using the schema provided in this codebase at the following location: [database_schema.sql](../../schemas/database_schema.sql).

```bash
cd project-space
mkdir db
cd db
sqlite3 database_name.db < ./path_to/database_schema.sql
```

The appropriate metadata can be in-filled by running the [db_populate_metadata.py](../../scripts/db_populate_metadata.py). To in-fill the expected number of asdm's that each MOUS contains, run the [db_populate_num_asdms.py](../../scripts/db_populate_num_asdms.py).