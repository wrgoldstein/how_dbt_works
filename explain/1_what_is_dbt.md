DBT is a tool for orchestrating complex SQL transformations in a database.

It assumes:

  * The raw data exists in the data warehouse
  * All transformations can be made as a series of SQL statements from source tables into new tables
  * Each SQL statement creates exactly one new table (or view, or subquery)

Using Jinja2 templating, it allows a SQL script to reference another SQL script by use of a `{ ref(..) }` command.

DBT uses this python function call to know which other SQL statements this script relies on, and then replaces it with an actual SQL reference to that table. Let's say we have a file `derived.sql`:

  ```sql
  SELECT * FROM {{ ref(‘base_table’) }}
  ```
  becomes

  ```sql
  SELECT * FROM “base_table”
  ```

and DBT records a reference from `derived` to `base_table`, which will help it to execute the SQL statements in the appropriate order later.

Other important features DBT supports are:

  * Generating test assertions against data (e.g. assert that the results of a SQL query diffing two tables are empty)
  * Generate documentation for the generated tables and columns (through compassion YAML files)
  * Create sandbox environments by replacing the schema for each generated node with a sandbox schema (select * from “sandbox”.”base_table”)
  * Allowing the DRYing up of SQL code using macros
