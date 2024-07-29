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
    host = "localhost"
    port = "5432"  # Puerto predeterminado de PostgreSQL
    database = "dw_ws"  # Reemplazar por el nombre real de la base de datos
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



    

    # Itera a través de las filas del DataFrame 'df' e inserta los datos en ambas tablas
    for index, row in df.iterrows():
        index = int(index + initial_index)
        print(index, initial_index)
        n_carpeta = row['n_carpeta']

        #DIM TRACKING 
        tracking_id = row['tracking_id']
        m3_recibidos = row['m3_recibidos']
        bultos_recepcionados = row['bultos_recepcionados']
        m3_esperados  = row['m3_esperados']
        peso_esperado = row['peso_esperado']
        bultos_esperados = row['bultos_esperados']

        # Consulta SQL para insertar un nuevo registro en la tabla 'sla.tracking'
        tracking_query = """INSERT INTO sla.tracking(
	        id_dim_tracking, tracking_id, n_carpeta, m3_recibidos, bultos_recepcionados, m3_esperados, peso_esperado, bultos_esperados)
	        VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
        """

        cursor.execute(tracking_query, (index, tracking_id, n_carpeta, m3_recibidos, bultos_recepcionados, m3_esperados, peso_esperado, bultos_esperados))
        
        #DIM PROVEEDOR
        id_proveedor = row['id_proveedor']
        nombre_proveedor = row['nombre_proveedor']
        fecha_creacion_proveedor = row['fecha_creacion_proveedor']

        # Consulta SQL para insertar un nuevo registro en la tabla 'sla.proveedor'
        proveedor_query = """INSERT INTO sla.proveedor(
            id_dim_proveedor, id_proveedor, n_carpeta, nombre_proveedor, fecha_creacion_proveedor)
            VALUES (%s, %s, %s, %s, %s);
        """

        cursor.execute(proveedor_query, (index, id_proveedor, n_carpeta, nombre_proveedor, fecha_creacion_proveedor))
        #DIM HITOS

        fecha_ultima_recepcion = row['fecha_ultima_recepcion']
        fecha_ultima_carga_documentos = row['fecha_ultima_carga_documentos']
        fecha_cierre_consolidado_comercial = row['fecha_cierre_consolidado_comercial']
        fecha_aprobacion_documentos = row['fecha_aprobacion_documentos']
        fecha_consolidado_contenedor = row['fecha_consolidado_contenedor']
        etd_nave_asignada = row['etd_nave_asignada']
        eta = row['eta']
        fecha_publicacion = row['fecha_publicacion']
        fecha_aforo = row['fecha_aforo']
        fecha_publicacion_aforo = row['fecha_publicacion_aforo']
        fecha_retiro_puerto = row['fecha_retiro_puerto']
        fecha_retiro = row['fecha_retiro']
        fecha_desconsolidacion_pudahuel = row['fecha_desconsolidacion_pudahuel']
        fecha_de_pago = row['fecha_de_pago']
        fecha_solicitud_despacho = row['fecha_solicitud_despacho']
        fecha_prog_despacho = row['fecha_prog_despacho']
        fecha_entrega_retiro = row['fecha_entrega_retiro']


        # Consulta SQL para insertar un nuevo registro en la tabla 'sla.hitos'
        hitos_query = """INSERT INTO sla.hitos(
            id_dim_hitos, fecha_ultima_recepcion, fecha_ultima_carga_documentos, fecha_cierre_consolidado_comercial, fecha_aprobacion_documentos, fecha_consolidado_contenedor, etd_nave_asignada, eta, fecha_publicacion, fecha_aforo, fecha_publicacion_aforo, fecha_retiro_puerto, fecha_retiro, fecha_desconsolidacion_pudahuel, fecha_de_pago, fecha_solicitud_despacho, fecha_prog_despacho, fecha_entrega_retiro)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """

        cursor.execute(hitos_query, (
            index, fecha_ultima_recepcion, fecha_ultima_carga_documentos, fecha_cierre_consolidado_comercial, 
            fecha_aprobacion_documentos, fecha_consolidado_contenedor, etd_nave_asignada, eta, fecha_publicacion, 
            fecha_aforo, fecha_publicacion_aforo, fecha_retiro_puerto, fecha_retiro, fecha_desconsolidacion_pudahuel, 
            fecha_de_pago, fecha_solicitud_despacho, fecha_prog_despacho, fecha_entrega_retiro
        ))
        #DIM FINANZAS
        proforma_id = row['proforma_id']
        estado_finanzas = row['estado_finanzas']
        fecha_de_pago = row['fecha_de_pago']

        # Consulta SQL para insertar un nuevo registro en la tabla 'sla.finanzas'
        finanzas_query = """INSERT INTO sla.finanzas(
            id_dim_finanzas, proforma_id, n_carpeta, estado_finanzas, fecha_de_pago)
            VALUES (%s, %s, %s, %s, %s);
        """

        cursor.execute(finanzas_query, (index, proforma_id, n_carpeta, estado_finanzas, fecha_de_pago))
        #DIM CONTENEDOR
        n_contenedor = row['n_contenedor']
        direccion_entrega = row['direccion_entrega']
        comuna = row['comuna']

        # Consulta SQL para insertar un nuevo registro en la tabla 'sla.contenedor'
        contenedor_query = """INSERT INTO sla.contenedor(
            id_dim_contenedor, n_carpeta, n_contenedor, direccion_entrega, comuna)
            VALUES (%s, %s, %s, %s, %s);
        """

        cursor.execute(contenedor_query, (index, n_carpeta, n_contenedor, direccion_entrega, comuna))
        
        #DIM CLIENTE
        fecha_creacion_cliente = row['fecha_creacion_cliente']
        razon_social_cliente = row['razon_social_cliente']
        ejecutivo = row['ejecutivo']
        ejecutivo_cuenta = row['ejecutivo_cuenta']
        id_cliente = row['id_cliente']

        # Consulta SQL para insertar un nuevo registro en la tabla 'sla.cliente'
        cliente_query = """INSERT INTO sla.cliente(
            id_dim_cliente, n_carpeta, fecha_creacion_cliente, razon_social_cliente, ejecutivo, ejecutivo_cuenta, id_cliente)
            VALUES (%s, %s, %s, %s, %s, %s, %s);
        """

        cursor.execute(cliente_query, (index, n_carpeta, fecha_creacion_cliente, razon_social_cliente, ejecutivo, ejecutivo_cuenta, id_cliente))

    # Confirma los cambios en la base de datos
    conn.commit()

    # Cierra el cursor y la conexión
    cursor.close()
    conn.close()

    return 1


def load_fact_table(df, initial_index):

    # Datos de conexión
    host = "localhost"
    port = "5432"  # Puerto predeterminado de PostgreSQL
    database = "dw_ws"  # Reemplazar por el nombre real de la base de datos
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


    
    # Itera a través de las filas del DataFrame 'df' e inserta los datos en ambas tablas
    for index, row in df.iterrows():
        print("index_entrada", index)
        print(index, initial_index)
        index = int(index + initial_index)
        #FACT TABLE SERVICIO
        fecha_creacion_consolidado_comercial = row['fecha_de_creacion_del_consolidado']
        id_dim_contenedor = index
        id_dim_finanzas = index
        n_carpeta = row['n_carpeta']
        id_consolidado_comercial = row['id_consolidado_comercial']
        id_dim_hitos = index
        id_dim_proveedor = index
        id_dim_tracking = index

        # Consulta SQL para insertar un nuevo registro en la tabla 'sla.fact_servicio'
        fact_servicio_query = """INSERT INTO sla.fact_servicio(
            id_servicio, fecha_creacion_consolidado_comercial, id_dim_contenedor, id_dim_finanzas, n_carpeta, id_consolidado_comercial, id_dim_hitos, id_dim_proveedor, id_dim_tracking)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
        """

        

        cursor.execute(fact_servicio_query, (
            index, fecha_creacion_consolidado_comercial, id_dim_contenedor, id_dim_finanzas, n_carpeta, id_consolidado_comercial, id_dim_hitos, id_dim_proveedor, id_dim_tracking
        ))
        

        sla_1 = row['sla_1']
        sla_2 = row['sla_2']
        sla_3 = row['sla_3']
        id_dim_cliente = index
        sla_4 = row['sla_4']
        sla_5 = row['sla_5']
        sla_6 = row['sla_6']
        sla_7 = row['sla_7']
        sla_8 = row['sla_8']
        sla_10 = row['sla_10']
        sla_11 = row['sla_11']
        n_carpeta = row['n_carpeta']

        # Consulta SQL para insertar un nuevo registro en la tabla 'sla.fact_carpeta'
        fact_carpeta_query = """INSERT INTO sla.fact_carpeta(
            id_fact_carpeta, sla_1, sla_2, sla_3, id_dim_cliente, sla_4, sla_5, sla_6, sla_7, sla_8, sla_10, sla_11, n_carpeta)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """
        cursor.execute(fact_carpeta_query, (
            index, sla_1, sla_2, sla_3, id_dim_cliente, sla_4, sla_5, sla_6, sla_7, sla_8, sla_10, sla_11, n_carpeta
        ))
    #print("index salida", index)
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
            index_dimension = index
            print("index_dimension", index_dimension,
                  index_dimension + batch_size)
            index_dimension = load_dimensions(
                df, index_dimension)
        except:
            print("fallo dimension")

            # Pausa de 5 segundos
            time.sleep(5)
            pass

        try:
            index_fact = index
            print("index_fact", index_fact)
            index_fact = load_fact_table(
                df, index_fact)
        except:
            print("fallo hechos")
            time.sleep(5)
            pass

        print("Proceso terminado")
        break
        initial_index2 = initial_index2 + batch_size
        index = min(index_dimension, index_fact)
        


#pruebas
from utils_dates import *
from delete_data_server import *

print("Partiendo")

start_time = datetime.now()

print("Hora de inicio: ",start_time)
delete_records_all_tables()
df, initial_index, batch_size = db_modified(), 0, 200
loader(df, initial_index, batch_size)

end_time = datetime.now()
print("Hora de termino: ",end_time)
delta_time =end_time-start_time
print("Tiempo total del proceso: ",delta_time)
