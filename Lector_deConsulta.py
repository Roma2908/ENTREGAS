import mysql.connector
import pandas as pd
import os

# ----------- CONFIGURACIÓN DE CONEXIÓN CON LA BASE DE DATOS -----------
config = {
    "host": "sql10.freesqldatabase.com",
    "user": "sql10777737",
    "password": "H6jPXNQPMW",
    "database": "sql10777737",
    "port": 3306
}

# ----------- RUTA DE SALIDA -----------
OUTPUT_DIR = "/content/Carpeta_ejemplo"
os.makedirs(OUTPUT_DIR, exist_ok=True)  # crea la carpeta si no existe
output_file = os.path.join(OUTPUT_DIR, "resultado.parquet")

# ....... MANEJO DE ERRORES
try:
    # Conectar a MySQL
    # **config convierte cada par clave: valor en un argumento de la forma clave=valor.
    conn = mysql.connector.connect(**config)
    print("Conexión exitosa")

    # Leer la consulta desde el archivo .sql
    with open("consulta.sql", "r") as file:
        query = file.read()

    # Ejecutar la consulta y cargar en DataFrame
    df = pd.read_sql(query, conn)

    # Guardamos en .parquet dentro de la carpeta deseada
    df.to_parquet(output_file)
    print(f"Datos guardados en '{output_file}'")

except mysql.connector.Error as err:
    print(f"Error al conectar: {err}")

finally:
    if 'conn' in locals() and conn.is_connected():
        conn.close()
        print("Conexión cerrada.")

