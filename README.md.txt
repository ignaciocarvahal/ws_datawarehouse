# Data Processing and Loading to Data Warehouse

Este proyecto contiene un script que procesa datos de una tabla de una base de datos y los carga en un data warehouse. 

## Requisitos

- Python 3.x
- Paquetes de Python:
  - `pandas`
  - `sqlalchemy`
  - `psycopg2`

## Instalación

1. Clona el repositorio:

   ```bash
   git clone https://github.com/ignaciocarvahal/ws_datawarehouse
   ```
2. Crea un entorno virtual y actívalo
   ```bash
   python -m venv env
   source env/bin/activate  # En Windows usa `env\Scripts\activate`
   ```
3. Instala las dependencias
   ```bash
   pip install -r requirements.txt
   ```

## Ejecución

El script load_server.py realiza la obtención de datos de la base de datos, la modificación y limpieza de los datos, y la carga en el data warehouse.

1. Asegúrate de tener configuradas las variables de entorno correctamente en el archivo .env.

2. Ejecuta el script:
   ```bash
   python load_server.py
   ```

## Funcionalidades

1. Extracción de Datos: Conecta a la base de datos de origen y extrae los datos de la tabla especificada.

2. Transformación de Datos: Procesa los datos extraídos según las necesidades del negocio.

3. Carga de Datos: Inserta los datos transformados en el data warehouse.
