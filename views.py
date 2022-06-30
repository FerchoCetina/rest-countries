import hashlib
import json
import time

import pandas as pd
import sqlite3
import requests

ENDPOINT_LEN = "https://restcountries.com/v3.1/all"


def get_connection_sqlite(db="tangelo"):
    """Esta funcion crea una conexion a la bd tangelo de sqlite3"""
    conn = sqlite3.connect(db)
    return conn


def create_table_sqlite(conn):
    """Esta funcion crea una tabla languages si no existe en la bd"""
    c = conn.cursor()
    c.execute('CREATE TABLE IF NOT EXISTS languages (region, city_name, language, time)')
    conn.commit()


def insert_data(data, conn):
    """Esta funcion recibe un data y un conn
        data -> type dict
        conn -> type sqlite3.Connection

        los convierte en un DataFrame de pandas
        inserta los datos en sqlite
        crea un archivo data.json de los datos
        return los tiempos de la ejecucion de las filas.
        :return:
        Tiempo Total:      {tiempo_total}
        Tiempo Promedio:   {promedio}
        Tiempo Minimo:   {min}
        Tiempo Maximo:   {max}

    """
    df = pd.DataFrame(data)
    df_describe = df.describe()['time']
    promedio = df_describe['mean']
    min = df_describe['min']
    max = df_describe['max']
    tiempo_total = df['time'].sum()
    time_records = F"""
    Tiempo Total:      {tiempo_total}
    Tiempo Promedio:   {promedio}
    Tiempo Minimo:   {min}
    Tiempo Maximo:   {max}
    """
    df.to_sql('languages', conn, if_exists='replace', index=False)

    json_data = df.to_json(orient="split")
    parsed = json.loads(json_data)
    with open('data.json', 'w', encoding='utf-8') as f:
        json.dump(parsed, f, ensure_ascii=False, indent=4)
    return time_records

def get_data():
    """
    Esta funcion consulta a la api para obtener region, ciudad, lenguaje y tiempo por fila

    :return:
    dict
    {
        'region': [regions],
        'city_name': [cities_names],
        'language': [languages],
        'time': [times]
    }

    """
    regions = []
    cities_names = []
    languages = []
    times = []

    try:
        response_data = requests.get(ENDPOINT_LEN, timeout=10)
        if response_data.status_code == 200:
            response_json = json.loads(response_data.content)
            for language in response_json:
                initial_time_row = time.time()
                regions.append(language['region'])
                cities_names.append(language['name']['common'])
                lan = language['languages'][list(language['languages'].keys())[0]] if "languages" in language else None
                lan_sha1 = ""
                if lan:
                    hash_object = hashlib.sha1(bytes(lan, encoding='utf8'))
                    lan_sha1 = hash_object.hexdigest()
                languages.append(lan_sha1)
                end_time_row = time.time()
                total_time_row = end_time_row - initial_time_row
                times.append(float("{:.5f}".format(total_time_row*1000)))

    except (requests.exceptions.Timeout, requests.exceptions.ReadTimeout):
        pass
    data = {
        'region': regions,
        'city_name': cities_names,
        'language': languages,
        'time': times
    }
    return data


def main_function():
    data = get_data()
    conn = get_connection_sqlite()
    create_table_sqlite(conn)
    result = insert_data(data, conn)
    print(result)
    return True




