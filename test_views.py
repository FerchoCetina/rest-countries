import sqlite3

from views import get_data, get_connection_sqlite, insert_data, main_function


def test_main_function():
    assert main_function() == True


def test_get_connection_sqlite():
    assert type(get_connection_sqlite()) == sqlite3.Connection


def test_insert_data():
    data = get_data()
    conn = get_connection_sqlite()
    assert "Tiempo Total" in insert_data(data, conn)
    assert "Tiempo Promedio" in insert_data(data, conn)
    assert "Tiempo Minimo" in insert_data(data, conn)
    assert "Tiempo Maximo" in insert_data(data, conn)