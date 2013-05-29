
SQLDump examples
================

This folder contains some usage examples of SQLDump

- `sqlqueries` - simple example showing how to generate datadumps in different file formats and 
  partition data from one SQL query in many files. 
  Example works with PostgreSQL and H2 (but may be used as template for any database).

- `firebird-to-h2` - grabs schema and data from the 'employee' example firebird database and 
  creates a new H2 database. Uses `SQLDialectTransformer` to transform firebird sql dialect into
  ansi sql.

- `rs2graphml/sqldump-oracle-deps.properties` - usage of ResultSet2GraphML to
  generate a graph of database object's dependencies on an Oracle Database
