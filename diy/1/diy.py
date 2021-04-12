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


for node in pathlib.Path(".").glob("**/*.sql"):
    Engine.current = node.stem
    t = env.from_string(node.read_text())

    Engine.graph[node.stem]["compiled"] = t.render()

graph = { k:v["deps"] for k,v in Engine.graph.items() }

ts = graphlib.TopologicalSorter(graph)

print(list(ts.static_order()))
