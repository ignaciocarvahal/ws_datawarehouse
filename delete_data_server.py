

import psycopg2


def get_fk_servicio_values():
    # Datos de conexión
    host = "190.171.188.230"
    port = "5432"
    database = "topusDB"
    user = "user_solo_lectura"
    password = "4l13nW4r3.C0ntr4s3n4.S0l0.L3ctur4"

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

    try:
        # Consulta SQL para obtener los últimos 10000 valores de fk_servicio desde servicios_logs
        query = """
            SELECT fk_servicio
            FROM public.servicios_logs
            ORDER BY "updatedAt" DESC
            limit 20000
            ;
        """

        # Ejecuta la consulta SQL
        cursor.execute(query)

        # Obtiene los resultados de la consulta
        fk_servicio_values = [result[0] for result in cursor.fetchall()]

        return fk_servicio_values

    except Exception as e:
        # Maneja cualquier error que pueda ocurrir durante la ejecución de la consulta
        print(f"Error al obtener valores de fk_servicio: {e}")
        return None

    finally:
        # Cierra el cursor y la conexión
        cursor.close()
        conn.close()

def delete_records_all_tables():
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

    try:
        # Convierte la lista de fk_servicio_values a una cadena con formato ('valor1', 'valor2', 'valor3')
        #fk_servicio_values_str = ', '.join(map(str, fk_servicio_values))
        #fk_servicio_values_str2 = ', '.join(map(int, [int(value) for value in fk_servicio_values_str.split(",")]))

        # Construye la consulta SQL para eliminar registros en base a la lista de fk_servicio_values
        delete_query = f"""
            TRUNCATE TABLE sla.hitos CASCADE;
            TRUNCATE TABLE sla.cliente CASCADE;
            TRUNCATE TABLE sla.finanzas CASCADE;
            TRUNCATE TABLE sla.proveedor CASCADE;
            TRUNCATE TABLE sla.contenedor CASCADE;
            TRUNCATE TABLE sla.tracking CASCADE;  
            TRUNCATE TABLE sla.caracteristicas_comerciales CASCADE;        
        """
        #DELETE FROM public.facturas WHERE fk_servicio IN ({fk_servicio_values_str});
        # Ejecuta la consulta SQL
        cursor.execute(delete_query)

        # Confirma los cambios en la base de datos
        conn.commit()
        print(f"Registros eliminados satisfactoriamente para tablas hitos, cliente, finanzas, proveedor, contenedor y tracking")

    except Exception as e:
        # Maneja cualquier error que pueda ocurrir durante la ejecución de la consulta
        print(f"Error: {e}")
        conn.rollback()

    finally:
        # Cierra el cursor y la conexión
        cursor.close()
        conn.close()



"""

# Obtiene los valores de fk_servicio desde la consulta
fk_servicio_values_to_delete = get_fk_servicio_values()
print(fk_servicio_values_to_delete)
if fk_servicio_values_to_delete:
    # Llama a la función para eliminar registros
    delete_records_by_fk_servicio(fk_servicio_values_to_delete)
else:
    print("No se pudieron obtener los valores de fk_servicio.")
"""