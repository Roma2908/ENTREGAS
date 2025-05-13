import mysql.connector
import pandas as pd
import os
import json

# ----------- CARGA DE CONFIGURACIÓN DESDE JSON -----------
with open("config.json", "r") as f:
    config_json = json.load(f)

config = {
    "host": config_json["host"],
    "user": config_json["user"],
    "password": config_json["password"],
    "database": config_json["database"],
    "port": config_json["port"]
}

# ----------- DEFINIR RUTA DINÁMICA -----------
script_dir = os.path.dirname(os.path.abspath(__file__))  # ruta del script actual
output_dir = os.path.join(script_dir, config_json["output_folder"])
os.makedirs(output_dir, exist_ok=True)
output_file = os.path.join(output_dir, "resultado.parquet")

# ----------- MANEJO DE CONEXIÓN Y CONSULTA -----------
try:
    conn = mysql.connector.connect(**config)
    print("Conexión exitosa")

    with open("consulta.sql", "r") as file:
        query = file.read()

    df = pd.read_sql(query, conn)
    df.to_parquet(output_file)
    print(f"Datos guardados en '{output_file}'")

except mysql.connector.Error as err:
    print(f"Error al conectar: {err}")

finally:
    if 'conn' in locals() and conn.is_connected():
        conn.close()
        print("Conexión cerrada.")
