# -*- coding: utf-8 -*-
"""
Created on Mon Jan 15 23:14:30 2024

Modified on Mon Jul 29 07 18:02:00 2024

@author: Ignacio Carvajal
"""

import time
import pandas as pd
import numpy as np
import random
import os
import psycopg2
from datetime import datetime, time
from connection import *
import psycopg2
import pandas as pd
import numpy as np
from utils_operaciones import hora_a_minutos, extract_data,  descomponer_fechas, formatear_a_fecha, unir_fecha_hora
from datetime import datetime, MINYEAR
import pytz
from datetime import datetime, timezone, timedelta

def execute_bulk_insert(cursor, query, data):
    """
    Inserta múltiples registros en una tabla usando un solo comando INSERT.

    Parameters
    ----------
    cursor : psycopg2 cursor object
        Cursor de la conexión a la base de datos.
    query : str
        La consulta SQL base para el comando INSERT.
    data : list of tuples
        Los datos a insertar en la tabla.

    Returns
    -------
    None
    """
    if not data:
        return
    # Determinar la cantidad de placeholders para el mogrify según la cantidad de columnas
    num_columns = len(data[0])
    placeholders = ', '.join(['%s'] * num_columns)
    args_str = ','.join(cursor.mogrify(f"({placeholders})", x).decode("utf-8") for x in data)
    cursor.execute(query + args_str)


def load_dimensions(df, initial_index):
    """
    Parameters
    ----------
    df : DataFrame pandas object
        DESCRIPTION.
    initial_index : int
        sera el indice inicial de la carga
    stop_index : int
        sera el ultimo indice en ser llenado

    Returns None
    -------
    index : int
        es la llave que une el datawarehouse.
    """
    # Datos de conexión
    database = "ws_datawarehouse"
    host = "34.229.102.45"
    port = "5432"
    user = "postgres"
    password = "ignacio"

    # Establece la conexión a la base de datos PostgreSQL
    conn = psycopg2.connect(
        host=host,
        port=port,
        database=database,
        user=user,
        password=password
    )

    # Abre un cursor para ejecutar consultas SQL
    cursor = conn.cursor()
    
    tracking_data = []
    proveedor_data = []
    hitos_data = []
    finanzas_data = []
    contenedor_data = []
    cliente_data = []
    caracteristicas_data = []

    # Itera a través de las filas del DataFrame 'df' e inserta los datos en ambas tablas
    for index, row in df.iterrows():
        print("Dimentions load index:", index)
        n_carpeta = row['n_carpeta']

        # DIM TRACKING 
        tracking_data.append((
            index, row['tracking_id'], n_carpeta, row['m3_recibidos'], row['bultos_recepcionados'], 
            row['m3_esperados'], row['peso_esperado'], row['bultos_esperados']
        ))
        
        # DIM PROVEEDOR
        proveedor_data.append((
            index, row['id_proveedor'], n_carpeta, row['nombre_proveedor'], row['fecha_creacion_proveedor']
        ))
        
        # DIM HITOS
        hitos_data.append((
            index, row['fecha_ultima_recepcion'], row['fecha_ultima_carga_documentos'], 
            row['fecha_cierre_consolidado_comercial'], row['fecha_aprobacion_documentos'], 
            row['fecha_consolidado_contenedor'], row['etd_nave_asignada'], row['eta'], 
            row['fecha_publicacion'], row['fecha_aforo'], row['fecha_publicacion_aforo'], 
            row['fecha_retiro_puerto'], row['fecha_retiro'], row['fecha_desconsolidacion_pudahuel'], 
            row['fecha_de_pago'], row['fecha_solicitud_despacho'], row['fecha_prog_despacho'], 
            row['fecha_entrega_retiro']
        ))

        # DIM FINANZAS
        finanzas_data.append((
            index, row['proforma_id'], n_carpeta, row['estado_finanzas'], row['fecha_de_pago']
        ))

        # DIM CONTENEDOR
        contenedor_data.append((
            index, n_carpeta, row['n_contenedor'], row['direccion_entrega'], row['comuna']
        ))
        
        # DIM CLIENTE
        cliente_data.append((
            index, n_carpeta, row['fecha_creacion_cliente'], row['razon_social_cliente'], 
            row['ejecutivo'], row['ejecutivo_cuenta'], row['id_cliente']
        ))

        #DIM CARACTERISTICAS
        caracteristicas_data.append((
            index, row['ejecutivo'], row['ejecutivo_cuenta'], row['comuna'],
            row['chofer'], row['estado_entrega'], row['tipo_de_entrega'], row['bodega_recepcion'], row['nombre_nave'], row['aforo'], row['estado_despacho']))

    # Consultas SQL
    tracking_query = """INSERT INTO sla.tracking(
        id_dim_tracking, tracking_id, n_carpeta, m3_recibidos, bultos_recepcionados, m3_esperados, peso_esperado, bultos_esperados
    ) VALUES """
    proveedor_query = """INSERT INTO sla.proveedor(
        id_dim_proveedor, id_proveedor, n_carpeta, nombre_proveedor, fecha_creacion_proveedor
    ) VALUES """
    hitos_query = """INSERT INTO sla.hitos(
        id_dim_hitos, fecha_ultima_recepcion, fecha_ultima_carga_documentos, fecha_cierre_consolidado_comercial, fecha_aprobacion_documentos, fecha_consolidado_contenedor, etd_nave_asignada, eta, fecha_publicacion, fecha_aforo, fecha_publicacion_aforo, fecha_retiro_puerto, fecha_retiro, fecha_desconsolidacion_pudahuel, fecha_de_pago, fecha_solicitud_despacho, fecha_prog_despacho, fecha_entrega_retiro
    ) VALUES """
    finanzas_query = """INSERT INTO sla.finanzas(
        id_dim_finanzas, proforma_id, n_carpeta, estado_finanzas, fecha_de_pago
    ) VALUES """
    contenedor_query = """INSERT INTO sla.contenedor(
        id_dim_contenedor, n_carpeta, n_contenedor, direccion_entrega, comuna
    ) VALUES """
    cliente_query = """INSERT INTO sla.cliente(
        id_dim_cliente, n_carpeta, fecha_creacion_cliente, razon_social_cliente, ejecutivo, ejecutivo_cuenta, id_cliente
    ) VALUES """
    caracteristicas_query = """INSERT INTO sla.caracteristicas_comerciales(
	    id_dim_caracteristicas, ejecutivo, ejecutivo_cuenta, comuna, chofer, estado_entrega, tipo_de_entrega, bodega_recepcion, nombre_nave, aforo, estado_despacho
    ) VALUES"""
    
    execute_bulk_insert(cursor, tracking_query, tracking_data)
    print("DIM TRACKING CARGADA")
    execute_bulk_insert(cursor, proveedor_query, proveedor_data)
    print("DIM PROVEEDOR CARGADA")
    execute_bulk_insert(cursor, hitos_query, hitos_data)
    print("DIM HITOS CARGADA")
    execute_bulk_insert(cursor, finanzas_query, finanzas_data)
    print("DIM FINANZAS CARGADA")
    execute_bulk_insert(cursor, contenedor_query, contenedor_data)
    print("DIM CONTENEDOR CARGADA")
    execute_bulk_insert(cursor, cliente_query, cliente_data)
    print("DIM CLIENTE CARGADA")
    execute_bulk_insert(cursor, caracteristicas_query, caracteristicas_data)
    print("DIM CARACTERISTICAS CARGADA")

    # Confirma los cambios en la base de datos
    conn.commit()
    # Cierra el cursor y la conexión
    cursor.close()
    conn.close()

    return 1

def load_fact_table(df, initial_index):
    """
    Carga datos en las tablas de hechos 'sla.fact_servicio' y 'sla.fact_carpeta'.

    Parameters
    ----------
    df : DataFrame pandas object
        DataFrame con los datos a cargar.
    initial_index : int
        Índice inicial de la carga.

    Returns
    -------
    int
        Retorna 1 si la carga fue exitosa.
    """
    # Datos de conexión
    database = "ws_datawarehouse"
    host = "34.229.102.45"
    port = "5432"
    user = "postgres"
    password = "ignacio"

    # Establece la conexión a la base de datos PostgreSQL
    conn = psycopg2.connect(
        host=host,
        port=port,
        database=database,
        user=user,
        password=password
    )

    # Abre un cursor para ejecutar consultas SQL
    cursor = conn.cursor()
    print("Conectado al servidor")

    fact_servicio_data = []
    fact_carpeta_data = []

    # Itera a través de las filas del DataFrame 'df' e inserta los datos en ambas tablas
    for index, row in df.iterrows():
        print("Fact table load index:", index)
        # FACT TABLE SERVICIO
        fact_servicio_data.append((
            index, row['fecha_de_creacion_del_consolidado'], index, index, row['n_carpeta'],
            row['id_consolidado_comercial'], index, index, index, index
        ))

        # FACT TABLE CARPETA
        fact_carpeta_data.append((
            index, row['sla_1'], row['sla_2'], row['sla_3'], index, row['sla_4'], row['sla_5'],
            row['sla_6'], row['sla_7'], row['sla_8'], row['sla_10'], row['sla_11'], row['n_carpeta']
        ))

    # Consultas SQL
    fact_servicio_query = """INSERT INTO sla.fact_servicio(
        id_servicio, fecha_creacion_consolidado_comercial, id_dim_contenedor, id_dim_finanzas, n_carpeta, 
        id_consolidado_comercial, id_dim_hitos, id_dim_proveedor, id_dim_tracking, id_dim_caracteristicas
    ) VALUES """
    fact_carpeta_query = """INSERT INTO sla.fact_carpeta(
        id_fact_carpeta, sla_1, sla_2, sla_3, id_dim_cliente, sla_4, sla_5, sla_6, sla_7, sla_8, sla_10, sla_11, n_carpeta
    ) VALUES """

    execute_bulk_insert(cursor, fact_servicio_query, fact_servicio_data)
    print("FACT TABLE SERVICIO CARGADA")
    execute_bulk_insert(cursor, fact_carpeta_query, fact_carpeta_data)
    print("FACT TABLE CARPETA CARGADA")

    # Confirma los cambios en la base de datos
    conn.commit()

    # Cierra el cursor y la conexión
    cursor.close()
    conn.close()

    return 1

import time

def loader(df, initial_index, batch_size, tipo_carga="incremental"):

    lenght = len(df)
    #print(lenght)
    index_dimension = initial_index
    index_fact = initial_index
    index = initial_index
    
    while index < lenght + initial_index:
        
        try:
            # Calcular el índice final del lote

            end_index = min(index_dimension + batch_size, lenght)
            # Extraer el lote del DataFrame
            df_batch = df.iloc[index_dimension:end_index]

            print(f"Procesando lote desde {index_dimension} hasta {end_index}")
            # Cargar el lote en las dimensiones
            load_dimensions(df_batch, index_dimension)

            # Actualizar el índice para el siguiente lote
            index_dimension += batch_size
            print(f"Dimensiones cargadas hasta el índice {index_dimension}")
        except:
            print("fallo dimension")

            # Pausa de 5 segundos
            time.sleep(5)
            pass
        try:
            # Calcular el índice final del lote
            end_index = min(index_fact + batch_size, lenght)

            # Extraer el lote del DataFrame
            df_batch = df.iloc[index_fact:end_index]
            print(f"Procesando lote desde {index_fact} hasta {end_index}")

            # Cargar el lote en la tabla de hechos
            load_fact_table(df_batch, index_fact)

            # Actualizar el índice para el siguiente lote
            index_fact += batch_size
            print(f"Fact table cargada hasta el índice {index_fact}")
        except:
            print("fallo hechos")
            time.sleep(5)
            pass
        index += batch_size
        


#pruebas
from utils_dates import *
from delete_data_server import *

print("Partiendo")

start_time = datetime.now()

print("Hora de inicio: ",start_time)
delete_records_all_tables()
df, initial_index, batch_size = db_modified(), 0, 1500
loader(df, initial_index, batch_size)

end_time = datetime.now()
print("Hora de termino: ",end_time)
delta_time =end_time-start_time
print("Tiempo total del proceso: ",delta_time)
