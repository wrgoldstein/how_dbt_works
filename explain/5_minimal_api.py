"""
This is a minimum viable dbt model runner. You can only specify one model to run, and no arguments.
Note the model selection syntax just uses the model name itself, no path (e.g. `loan_files` not `core.loan_files.loan_files`).
Run as:
```
python -m scripts.noparse run staff
```
or to just print compiled SQL:
```
python -m scripts.noparse show staff
```
"""

import sys
import time

import fire
from dbt.main import parse_args
from dbt.task.base import ExecutionContext
from dbt.adapters.factory import get_adapter
from dbt.parser.manifest import (
    Manifest, 
    ManifestLoader, 
    ModelParser, 
    SchemaParser,
    SeedParser,
    process_sources, 
    process_refs
)
from dbt.contracts.files import SourceFile, FilePath
from dbt.parser.sources import patch_sources
from dbt.exceptions import DatabaseException


class timeit:
    def __init__(self, message):
        self.status = "OK"
        self.message = message

    def __enter__(self):
        self.t = time.time()
        print(self.message, end="", flush=True)
        return self
    
    def __exit__(self, *args):
        print(f"{self.status} ({time.time() - self.t:.2f} seconds)".rjust(80 - len(self.message) + len(self.status), '.'))


def compile(model):
    args = ["run"]
    parsed = parse_args(args)
    task = parsed.cls.from_args(args=parsed)

    config = root_config = task.config
    adapter = get_adapter(config)
    compiler = adapter.get_compiler()
    projects = root_config.load_dependencies()
    macro_hook = adapter.connections.set_query_header
    macro_manifest = ManifestLoader.load_macros(config, macro_hook)

    loader = ManifestLoader(root_config, projects, macro_hook)
    project = loader.all_projects.get('better_dbt')

    with timeit("Parsing macros and utilities"):
        other_projects = set(loader.all_projects.keys()) - set(['better_dbt'])
        for op in other_projects:
            op = loader.all_projects.get(op)
            model_parser = ModelParser(loader.results, op, loader.root_project, macro_manifest)
            for path in model_parser.search():
                loader.parse_with_cache(path, model_parser, [])

    model_parser = ModelParser(loader.results, project, loader.root_project, macro_manifest)
    model_paths = { f.relative_path.split("/")[-1][:-4]: f for f in model_parser.search()}

    path = model_paths.get(model)
    entry_block = loader._get_file(path, model_parser)
    model_parser.parse_file(entry_block)

    with timeit("Parsing seeds"):
        seed_parser = SeedParser(loader.results, project, loader.root_project, macro_manifest)
        for path in seed_parser.search():
            block = loader._get_file(path, seed_parser)
            model_paths[path.relative_path.split("/")[-1][:-4]] = path
            seed_parser.parse_file(block)

    with timeit("Parsing sources"):
        schema_parser = SchemaParser(loader.results, project, loader.root_project, macro_manifest)

        # unfortunately we have to parse all 407 yml files
        # to look for sources, which aren't structured by file path
        # the way models are.
        # full parsing every file takes about 35 seconds.. too slow
        # so we first search for sources

        src_files = []
        for path in schema_parser.search():
            if "sources:" in open(path.full_path).read():
                src_files.append(path)

        for path in src_files:
            block = loader._get_file(path, schema_parser)
            schema_parser.parse_file(block)

    parsed = []
    def recurse_through_refs(parsed=parsed):
        added = 0
        nodes = [ x for x in loader.results.nodes.values() ] # need a copy
        for node in nodes:
            if node in parsed:
                continue
            for ref in node.refs:
                _target_model_package = None
                if len(ref) == 1:
                    target_model_name = ref[0]
                elif len(ref) == 2:
                    _target_model_package, target_model_name = ref

                if target_model_name not in parsed:
                    path = model_paths.get(target_model_name)
                    block = loader._get_file(path, model_parser)
                    model_parser.parse_file(block)
                    
                    path = model_paths.get(target_model_name)
                    block = loader._get_file(path, model_parser)
                    compiled_path = model_parser.get_compiled_path(block)
                    fqn = model_parser.get_fqn(compiled_path, block.name)
                    build_config = model_parser.initial_config(fqn)
                    node = model_parser._create_parsetime_node(
                                block=block,
                                path=compiled_path,
                                config=build_config,
                                fqn=fqn,
                            )
                    model_parser.render_update(node, build_config)
                    added += 1

                parsed.append(target_model_name)

            parsed.append(node.search_name)
        if added > 0:
            recurse_through_refs(parsed=parsed)

    with timeit("Parsing models"):
        recurse_through_refs(parsed=parsed)

    with timeit("Preparing manifest"):
        loader.load_only_macros()
        sources = patch_sources(loader.results, loader.root_project)
        nodes = {k: v for k, v in loader.results.nodes.items()}
        manifest = Manifest(
            nodes=nodes,
            sources=sources,
            macros=loader.results.macros,
            docs=loader.results.docs,
            exposures=loader.results.exposures,
            metadata=loader.root_project.get_metadata(),
            disabled=[],
            files=loader.results.files,
            selectors=loader.root_project.manifest_selectors,
        )
        manifest.patch_nodes(loader.results.patches)
        manifest.patch_macros(loader.results.macro_patches)
        project_name = loader.root_project.project_name
        process_sources(manifest, project_name)
        process_refs(manifest, project_name)

        graph = compiler.compile(manifest)

        task.manifest = manifest
        task.graph = graph

        node = manifest.nodes.get(f'model.better_dbt.{model}')
        
        runner = task.get_runner(node)
        ctx = ExecutionContext(node)
    
    with timeit("Compiling node"):
        ctx.node = runner.compile(task.manifest)

    return task, runner, ctx.node, manifest


def show(model):
    task, runner, node, manifest = compile(model)
    print("Compiled SQL:")
    print(node.compiled_sql)


def run(model):
    task, runner, node, manifest = compile(model)
    exception_message = None
    with timeit("Running...") as t:
        try:
            result = runner.run(node, manifest)
            return result
        except DatabaseException as e:
            t.status = 'ERROR'
            exception_message = e.msg

    if exception_message is not None:
        print(exception_message)
        exit(1)


if __name__ == "__main__":
    fire.Fire()
