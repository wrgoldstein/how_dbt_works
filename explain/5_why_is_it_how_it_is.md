## Slowness

To begin with, what makes dbt so slow to start up? Really two things:

1. Parsing strategy
1. Compilation strategy


### Parsing strategy

It takes about 4 seconds to load all the code and get to parsing. 

After that, dbt insists on parsing every file in the project on every startup (unless --partial-parse is passed, which leads to other kinds of problems). We have about 1700 sql models and 400 yaml files, which results in actual time spent on parsing each startup is as follows:

```
models.ModelParser         16.0766 secs
snapshots.SnapshotParser   0.0972 secs
analysis.AnalysisParser    0.1748 secs
data_test.DataTestParser   1.5169 secs
hooks.HookParser           0.0006 secs
seeds.SeedParser           0.0740 secs
docs.DocumentationParser   0.0902 secs
schemas.SchemaParser       16.2633 secs
```

### Compilation strategy

dbt allows for macros that reference the current state of the target database, for instance the `dbt_utils.star` macro which requests the list of columns from the target database and compiles to that list of columns. Eric recently merged a PR that removed many of these kinds of macros.


### Runtime strategy

dbt materializations often require information about the current state of the database. in a `before_run` hook on the ModelRunner, dbt queries for a list of all views, tables, and table dependencies and caches it in the adapter cache.

This can take as little as a few seconds if Redshift is responsive or minutes if not.

