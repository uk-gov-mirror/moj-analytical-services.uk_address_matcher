import duckdb
import pytest


@pytest.fixture
def duck_con():
    con = duckdb.connect(database=":memory:")
    con.execute("INSTALL splink_udfs FROM community; LOAD splink_udfs;")

    yield con
    con.close()
