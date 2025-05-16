# CONSULTA AUTOMATIZADA DE DATOS

Este proyecto tiene como objetivo automatizar la consulta, carga y exportación de datos desde una base de datos MySQL o desde archivos planos (.csv, .parquet, .json, .xlsx) utilizando Python, en entornos como Databricks o locales.

## ¿Qué incluye?

Una clase reutilizable llamada `ConsultaDB` que permite:

- Conectarse a una base de datos MySQL utilizando SQLAlchemy.
- Ejecutar consultas SQL almacenadas en archivos `.sql`.
- Cargar archivos locales o de DBFS en formatos `.csv`, `.parquet`, `.json` o `.xlsx`.
- Exportar resultados a múltiples formatos: `.parquet`, `.csv`, `.json`, `.xlsx`.
- Personalizar la ruta de salida y el nombre de los archivos generados.
- Aplicar reducción automática de variables usando valores SHAP, manteniendo la explicabilidad del modelo.
- Seleccionar automáticamente las variables más relevantes sin importar el modelo usado.
