import duckdb
print(duckdb.__version__)

con = duckdb.connect()
print(con.execute("SELECT 42 AS test").fetchall())
