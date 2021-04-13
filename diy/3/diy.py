import pathlib
import collections
import jinja2
import graphlib


class Engine:
    current = None
    graph = collections.defaultdict(lambda: dict(deps=[], compiled=None, config={}))
    env = jinja2.Environment(
        loader=jinja2.FileSystemLoader("")
    )

    def ref(node: str):
        Engine.graph[Engine.current]["deps"].append(node)
        return node

    def config(node: str):
        Engine.graph[Engine.current[""]]

    def render(path: pathlib.Path):
        Engine.current = path.stem
        t = Engine.env.from_string(path.read_text())
        Engine.graph[path.stem]["compiled"] = t.render()

    def add_macro(path: pathlib.Path):
        m = Engine.env.from_string(path.read_text())
        macros = [x for x in dir(m.module) if not x.startswith("_")]
        for macro in macros:
            Engine.env.globals[macro] = getattr(m.module, macro)

    env.globals.update(ref=ref)

    def get_sorted():
        graph = { k:v["deps"] for k,v in Engine.graph.items() }
        ts = graphlib.TopologicalSorter(graph)
        return list(ts.static_order())


for macro in pathlib.Path("macros").glob("**/*.sql"):
    Engine.add_macro(macro)

for model in pathlib.Path("models").glob("**/*.sql"):
    Engine.render(model)
    

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
# for node in Engine.get_sorted():
#     sql = Engine.graph[node]["compiled"]
#     con.execute(f"""
#     create table {node} as ({sql})
#     """)
