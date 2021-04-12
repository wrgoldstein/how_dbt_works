This is the most minimal example of a SQL transformation tool like DBT.

Running `python -m diy` from this directory will print the execution order of the scripts. In this case, `foo.sql` has a reference to `bar`, so the execution order is `bar`, `foo`.

This example doesn't have an actual materialization / runner. One option would be to finish the script with this:

```python
from psycopg2 import connect

con = connect(...)
for node in ts.static_order():
    sql = Engine.graph[node]["compiled"]
    con.execute(f"""
    create table {node} as ({sql})
    """)
```
