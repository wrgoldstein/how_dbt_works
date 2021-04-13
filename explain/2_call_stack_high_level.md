So what happens when you call

```
dbt compile -m some.model
```
?

The rough steps are:

1. parse CLI args to create a Task of the right type
2. create a manifest, and compile it
3. create a graph of all nodes
4. filter to the selected nodes
5. execute the nodes in parallel


# More detail

### Tasks 

Tasks can be one of 
    * CompileTask(GraphRunnableTask)
    * RunTask(CompileTask)
    * TestTask(RunTask)

  GraphRunnableTask inherits from ManifestTask, which inherits from ConfiguredTask, which inherts from BaseTask.

Each Task type has its own Runner type:
    * CompileRunner
    * ModelRunner
    * TestRunner

More on Runners later.


### Manifest

we need an Adapter which depends on the database type we've configured (for us, it will be a RedshiftAdapter, implemented in dbt/plugins). The Adapter provides a compiler 

  The Manifest is the data structure that stores information about which nodes have been parsed and what the output from parsing was.

  There are 9 Parser types:

      MacroParser
      ModelParser
      SnapshotParser
      AnalysisParser
      DataTestParser
      HookParser
      SeedParser
      DocumentationParser
      SchemaParser

  Each has a `get_paths` method, implementing a custom FilesystemSearcher().

  To build a manifest, we instantiate a ManifestLoader, and then:

  ```
  For each parser type:
    For each path in `get_paths` (e.g. a .glob('**/*.sql')):
      call `ManifestLoader#parse_with_cache(parser, path)` (which eventually becomes `ConfiguredParser#parse_node` for most Parsers).
  ```

n.b. The MacroParser goes first and separate from the rest.

### Graph

To compile the manifest into a Graph, we need a Compiler. Compiler#compile(manifest) returns a Graph. The Manifest has already done all the work of determining nodes and edges, so this is very fast.

Finally, the Graph is turned into a GraphQueue, the subset of selected nodes (using a NodeSelector) is stored on the Task, and they are executed in order using the appropriate Runner.

### Selection

I haven't investigated how this works, but dbt uses the selection syntax from the CLI to filter the graph to just the selection.

### Execution

The relevant Runner implements `execute`, which for the run operation fetches the appropriate materialization macro, renders the template, and runs the SQL against the database.