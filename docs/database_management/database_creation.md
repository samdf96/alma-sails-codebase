## Creating the SQLite Database

First create a database in the `project-space/db` directory, using the schema provided in this codebase at the following location: [database_schema.sql](../master/schemas/database_schema.sql).

```bash
cd project-space
mkdir db
cd db
sqlite3 database_name.db < ./path_to/database_schema.sql
```

Then running the script [db_populate.py](../master/scripts/db_populate.py) will fill the database with the appropriate metadata from the CSV file passed in.