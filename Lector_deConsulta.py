from sqlalchemy import create_engine
import pandas as pd
import os
import json

class ConsultaDB:
    def __init__(self, config_path="config.json"):
        with open(config_path, "r") as f:
            config_data = json.load(f)

        self.user = config_data["user"]
        self.password = config_data["password"]
        self.host = config_data["host"]
        self.port = config_data["port"]
        self.database = config_data["database"]

        # Creamos el engine con SQLAlchemy
        self.engine = create_engine(
            f"mysql+mysqlconnector://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
        )

        self.default_output_folder = os.path.join(os.path.dirname(os.path.abspath(__file__)), config_data["output_folder"])
        os.makedirs(self.default_output_folder, exist_ok=True)

    def ejecutar_consulta(self, archivo_sql="consulta.sql"):
        try:
            with open(archivo_sql, "r") as file:
                query = file.read()

            df = pd.read_sql(query, self.engine)
            print("Consulta ejecutada correctamente")
            return df

        except Exception as err:
            print(f"Error al ejecutar la consulta: {err}")
            return None

    def exportar(self, df, nombre_archivo="resultado", formato="parquet", ruta=None):
        if df is None:
            print("DataFrame vacío, no se exporta.")
            return

        if ruta is None:
            ruta = self.default_output_folder

        os.makedirs(ruta, exist_ok=True)

        output_path = os.path.join(ruta, f"{nombre_archivo}.{formato}")

        try:
            if formato == "parquet":
                df.to_parquet(output_path)
            elif formato == "csv":
                df.to_csv(output_path, index=False)
            elif formato == "json":
                df.to_json(output_path, orient="records", lines=True)
            elif formato in ["excel", "xlsx"]:
                df.to_excel(output_path, index=False)
            else:
                raise ValueError(f"Formato '{formato}' no soportado.")

            print(f"Archivo guardado en: {output_path}")

        except Exception as e:
            print(f"Error al exportar: {e}")
