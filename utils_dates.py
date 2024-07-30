from datetime import datetime
import pandas as pd
import psycopg2

from constants import FECHAS_NO_CONTEMPLADAS, DICCIONARIO_NOMBRES_NUMEROS, DICCIONARIO_NUMEROS_NOMBRES, COLUMNAS_FECHA

def time_stamp(time):
    """
    Transforma una fecha a un formato específico.

    Parámetros:
    time (str): La fecha en formato de cadena.

    Retorna:
    str: La fecha transformada en formato 'YYYY-MM-DD HH:MM', o una cadena vacía si la entrada es nula o vacía.

    Descripción:
    La función intenta convertir la fecha en varios formatos posibles ('YYYY-MM-DD HH:MM:SS' y 'DD/MM/YYYY') y la retorna en el formato 'YYYY-MM-DD HH:MM'.
    Si la fecha no corresponde a ninguno de los formatos, retorna una cadena vacía.
    """
    if time is None or time == '' or pd.isna(time):
        return None

    try:
        dt = datetime.strptime(time, '%Y-%m-%d %H:%M:%S')
        return dt.strftime('%Y-%m-%d %H:%M')
    except ValueError:
        pass

    try:
        dt = datetime.strptime(time, '%d/%m/%Y')
        return dt.strftime('%Y-%m-%d %H:%M')
    except ValueError:
        pass

    return None

def to_time_stamp(df):
    """
    Aplica la transformación de formato de fecha a cada columna de fecha en el DataFrame.

    Parámetros:
    df (pandas.DataFrame): El DataFrame a procesar.

    Retorna:
    pandas.DataFrame: El DataFrame con las fechas transformadas.

    Descripción:
    La función itera sobre cada columna de fecha especificada en la lista `COLUMNAS_FECHA` y aplica la función `time_stamp` a cada valor de la columna.
    """
    for col in COLUMNAS_FECHA:
        if col in df.columns:
            df[col] = df[col].apply(time_stamp)
    return df

def connect_and_select():
    """
    Conecta a la base de datos y ejecuta una consulta para obtener datos.

    Retorna:
    pandas.DataFrame: Un DataFrame con los datos obtenidos de la base de datos.

    Descripción:
    La función establece una conexión con la base de datos especificada, ejecuta una consulta SELECT para obtener los datos y los almacena en un DataFrame.
    Cierra la conexión y retorna el DataFrame.
    """
    conn = None
    try:
        conn = psycopg2.connect(
            host='35.91.116.131',
            dbname='wscargo',
            user='user_solo_lectura_full',
            password='4l13nW4r3.C0ntr4s3n4.S0l0.L3ctur4'
        )

        cur = conn.cursor()

        query = """SELECT * FROM public.sla_00_completo limit 400;"""
        cur.execute(query)

        rows = cur.fetchall()

        col_names = [desc[0] for desc in cur.description]

        df = pd.DataFrame(rows, columns=col_names)

        cur.close()
        conn.close()

        return df

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

    finally:
        if conn is not None:
            conn.close()

def data_null_cleanser_col(df, col):
    """
    Elimina los valores NULL en una columna del DataFrame y asigna valores específicos según ciertas condiciones.

    Parámetros:
    df (pandas.DataFrame): El DataFrame a procesar.
    col (str): El nombre de la columna que se está procesando.

    Retorna:
    pandas.DataFrame: El DataFrame con los valores modificados.
    """
    peso = DICCIONARIO_NOMBRES_NUMEROS.get(col) + 2

    if peso > 10:
      peso = 10

    if peso is None:
        return df

    #Modificar lo de los pesos, que sean del siguiente lvl y si es el último que altiro sean 3000
    nombres = DICCIONARIO_NUMEROS_NOMBRES.get(peso, [])

    for id in df.index:
        value = df.at[id, col]
        if not pd.isna(value) and value != '':
            continue
        else:
            empty_value = 0
            for nombre in nombres:
                if empty_value == 0 and (not pd.isna(df.at[id, nombre])) and df.at[id, nombre] != '':
                    df.at[id, col] = '01/01/1900'
                    empty_value = 1

            if empty_value == 0:
                df.at[id, col] = '01/01/2200'
    return df


# Función para calcular SLA con condición
def calcular_sla(fecha1, fecha2):
    """

    Función auxiliar para el cálculo de los SLA.
    Calcula la diferencia en días entre dos fechas dadas.

    Parámetros:
    fecha1 (str): Primera fecha para el cálculo de SLA.
    fecha2 (str): Segunda fecha para el cálculo de SLA.

    Retorna:
    int: La diferencia en días entre las dos fechas. Si alguna de las fechas no es válida o está en la lista de fechas no contempladas, retorna -9999.
    """
    if fecha1 in FECHAS_NO_CONTEMPLADAS or fecha2 in FECHAS_NO_CONTEMPLADAS:
        return -9999
    else:
        try:
            fecha1 = pd.to_datetime(fecha1).date()
            fecha2 = pd.to_datetime(fecha2).date() 
            return ((fecha1) - fecha2).days
        except:
            return -9999


def sla_calculo(df):
    """
    Calcula los valores de SLA (Service Level Agreement) basados en diferentes fechas en el DataFrame.

    Parámetros:
    df (pandas.DataFrame): El DataFrame que contiene las fechas necesarias para el cálculo de los SLA.

    Retorna:
    pandas.DataFrame: El DataFrame con los valores de SLA calculados y añadidos como nuevas columnas.
    """
    df['sla_1'] = df.apply(lambda row: calcular_sla(row['fecha_ultima_recepcion'], row['fecha_ultima_recepcion']), axis=1)
    print("SLA_1 CALCULADO")
    df['sla_2'] = df.apply(lambda row: calcular_sla(row['fecha_cierre_consolidado_comercial'], row['fecha_ultima_recepcion']), axis=1)
    print("SLA_2 CALCULADO")
    df['sla_3'] = df.apply(lambda row: calcular_sla(row['fecha_consolidado_contenedor'], row['fecha_cierre_consolidado_comercial']), axis=1)
    print("SLA_3 CALCULADO")
    df['sla_4'] = df.apply(lambda row: calcular_sla(row['etd_nave_asignada'], row['fecha_consolidado_contenedor']), axis=1)
    print("SLA_4 CALCULADO")
    df['sla_5'] = df.apply(lambda row: calcular_sla(row['eta'], row['etd_nave_asignada']), axis=1)
    print("SLA_5 CALCULADO")
    df['sla_6'] = df.apply(lambda row: calcular_sla(row['fecha_aforo'], row['eta']), axis=1)
    print("SLA_6 CALCULADO")
    df['sla_7'] = df.apply(lambda row: calcular_sla(row['fecha_desconsolidacion_pudahuel'], row['fecha_aforo']), axis=1)
    print("SLA_7 CALCULADO")
    df['sla_8'] = df.apply(lambda row: calcular_sla(row['fecha_entrega_retiro'], row['fecha_desconsolidacion_pudahuel']), axis=1)
    print("SLA_8 CALCULADO")
    df['sla_10'] = df.apply(lambda row: calcular_sla(row['fecha_entrega_retiro'], row['fecha_cierre_consolidado_comercial']), axis=1)
    print("SLA_10 CALCULADO")
    df['sla_11'] = df.apply(lambda row: row['sla_10'] - (row['sla_6'] + row['sla_5']) if row['sla_10'] != -9999 and row['sla_6'] != -9999 and row['sla_5'] != -9999 else -9999, axis=1)
    print("SLA_11 CALCULADO")
    return df


def data_base_cleanser(df):
    """
    Limpia y transforma los datos de la base de datos, cambiando el formato de las fechas.

    Parámetros:
    df (pandas.DataFrame): El DataFrame obtenido de la base de datos.

    Retorna:
    pandas.DataFrame: El DataFrame con los valores de fechas en un formato correcto.

    Descripción:
    La función itera sobre cada columna de fechas especificada en `COLUMNAS_FECHA`, limpia los valores nulos en las columnas de fechas y transforma las fechas a un formato específico.
    """
    print("Limpiando los valores NULL")
    for col in COLUMNAS_FECHA:
        df = data_null_cleanser_col(df, col)
    print("Transformando el formato de las fechas")
    df = to_time_stamp(df)
    print("Calculando los SLA")
    df = sla_calculo(df)
    return df

def db_modified():
    """
    Obtiene datos de la base de datos, los limpia y transforma, y luego los imprime.

    Descripción:
    La función conecta a la base de datos, obtiene un DataFrame con los datos, los limpia y transforma, y luego imprime el DataFrame resultante.
    """
    print("Conectando a la base de datos")
    df = connect_and_select()
    print("Iniciando la limpieza")
    df_limpio = data_base_cleanser(df)
    return df_limpio

