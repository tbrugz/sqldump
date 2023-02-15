
sqlmigrate
==========

Tool to ease/automate database migration tasks.

sqlmigrate has three main tasks (and one "aggregated" task):

1. **status**: checks if database has needed control table and tests if there are pending migrations (*no changes are performed in the database*).

2. **setup**: inits the control table (`SQLMIGRATE_HISTORY` by default) and optionally populates the existing migrations (*only the controle table may be created/updated by this task*).

3. **migrate**: run migrations (*changes are performed in the database and the control table is updated accordingly*).

4. **setup_and_migrate**: runs both *setup* and *migrate* tasks.

Variables like the control table's name and migrations directory are usually defined in a properties file. An exemple file with the avaiable properties can be seen at [doc/sqlmigrate.template.properties](doc/sqlmigrate.template.properties).


migration types
---------------

There are two types of migrations: versioned and repeatable. Versioned migrations will be executed in (version-)ascending ordeer. Repeatable migrations will be executed when its contents have changed (checksum mismatch) in filename-ascending order. Repeatable migrations are always executed after versioned migrations.


common properties
-----------------

As shown in [doc/sqlmigrate.template.properties](doc/sqlmigrate.template.properties), all properties are preceded by `sqlmigrate.`.

* `migration-table` - name of the migration control table (`SQLMIGRATE_HISTORY` by default)
* `schema-name` - name of the migration's control table schema (if needed)
* `migrations-dir` - directory containing the versioned migrations
* `repeatable-migrations-dir` - directory containing the repeatable migrations
* `dry-run` - executes/simulates a task without performing any change in the database
* `scripts-charset` - charset of the scripts


versions & script syntax
--------------

A script version is a sequence of integers separated by dots (`.`) or hyphens (`-`). A versioned script file must have the syntax:

`<version>_<script-name>.<sql|properties>`

A repeatable migration file must have the following syntax:

`<script-name>.<sql|properties>`

A `.sql` migration file will be splitted and executed on the database. A `.properties` file will be processed as an sqlrun-importer (for importing `.csv`s & `.xls`s files)


tasks
-----


### `status` task

This task checks the 'migration-table' to see if it has the needed columns. If the migration-table is ok, the task will show the avaiable/pending migrations to execute.


### `setup` task

This task checks the 'migration-table' to see if it has the needed columns. If not, it will issue DDL comands (create table, add column, ...) to conform the migration table to the desired specification. Optionally, this task may populate (baseline) the control table with the avaiable migrations.

Baseline properties:

- `baseline` (`true` or `false` - default is `false`) - executes (or not) the baseline
- `baseline-version` - version to baseline
- `baseline-repeatables` (`all` or `none`, default is neither) - baseline `all` (inserts all), `none` (removes all) or does nothing (default)


### `migrate` task

This task represents the main purpose of this tool. First if checks if the migration table has the needed columns. If not, the process halts (`setup` task needed). After that, pending migrations are executed.


### `setup_and_migrate` task

Runs the **setup** task and the **migrate** task - in this order.


command line
------------

`sqlmigrate <action/task> [-propfile=<path-to-properties-file>] [-Dproperty-name=property-value]...`
