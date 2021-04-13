import pathlib
import collections
import jinja2
import graphlib


class Engine:
    current = None
    graph = collections.defaultdict(lambda: dict(deps=[], compiled=None))


def ref(node):
    Engine.graph[Engine.current]["deps"].append(node)
    return node


env = jinja2.Environment(
    loader=jinja2.FileSystemLoader("")
)
env.globals.update(ref=ref)


for model in pathlib.Path(".").glob("**/*.sql"):
    Engine.current = model.stem
    t = env.from_string(model.read_text())

    Engine.graph[model.stem]["compiled"] = t.render()

graph = { k:v["deps"] for k,v in Engine.graph.items() }

ts = graphlib.TopologicalSorter(graph)

node_order = list(ts.static_order())

assert node_order == ["bar", "foo"]

# from psycopg2 import connect

# con = connect(...)
# for node in ts.static_order():
#     sql = Engine.graph[node]["compiled"]
#     con.execute(f"""
#     create table {node} as ({sql})
#     """)
