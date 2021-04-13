import pathlib
import collections
import jinja2
import graphlib


class Engine:
    current = None
    graph = collections.defaultdict(lambda: dict(deps=[], compiled=None, config={}))
    env = jinja2.Environment(
        loader=jinja2.FileSystemLoader(""),
        finalize=lambda x: x if x is not None else ''
    )

    def ref(node: str):
        Engine.graph[Engine.current]["deps"].append(node)
        return node

    def config(**config: dict):
        Engine.graph[Engine.current]["config"] = config

    def render(path: pathlib.Path):
        Engine.current = path.stem
        t = Engine.env.from_string(path.read_text())
        Engine.graph[path.stem]["compiled"] = t.render().strip()

    def add_macro(path: pathlib.Path):
        m = Engine.env.from_string(path.read_text())
        macros = [x for x in dir(m.module) if not x.startswith("_")]
        for macro in macros:
            Engine.env.globals[macro] = getattr(m.module, macro)

    env.globals.update(ref=ref, config=config)

    def get_sorted():
        graph = { k:v["deps"] for k,v in Engine.graph.items() }
        ts = graphlib.TopologicalSorter(graph)
        return list(ts.static_order())


for macro in pathlib.Path("macros").glob("**/*.sql"):
    Engine.add_macro(macro)


for model in pathlib.Path("models").glob("**/*.sql"):
    Engine.render(model)


mat = Engine.graph["foo"]["config"]["materialized"]
assert mat == "view"


# from psycopg2 import connect
# con = connect(...)

for node in Engine.get_sorted():
    sql = Engine.graph[node]["compiled"]
    mat = Engine.graph[node]["config"].get("materialized", "table")
    stmt = f"""
    drop {mat} if exists {node};
    create {mat} {node} as ({sql});
    """
    # with con.cursor() as cur:
    #     cur.execute(stmt)
    print(stmt)
