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


for macro in pathlib.Path("macros").glob("**/*.sql"):
    m = env.from_string(macro.read_text())
    macros = [x for x in dir(m.module) if not x.startswith("_")]
    for macro in macros: 
        env.globals[macro] = getattr(m.module, macro)


for model in pathlib.Path(".").glob("**/*.sql"):
    Engine.current = model.stem
    t = env.from_string(model.read_text())

    Engine.graph[model.stem]["compiled"] = t.render()

graph = { k:v["deps"] for k,v in Engine.graph.items() }

ts = graphlib.TopologicalSorter(graph)

node_order = list(ts.static_order())

expected = """
select 
case
  when trim(maybe_blank)  = '' then null
  else maybe_blank
end from public.baz""".strip()


compiled = Engine.graph["bar"]["compiled"]

assert expected == compiled

# from psycopg2 import connect

# con = connect(...)
# for node in ts.static_order():
#     sql = Engine.graph[node]["compiled"]
#     con.execute(f"""
#     create table {node} as ({sql})
#     """)
