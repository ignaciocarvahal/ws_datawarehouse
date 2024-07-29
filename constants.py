# Definir las fechas no contempladas como cadenas
FECHAS_NO_CONTEMPLADAS = ["2200-01-01 00:00", "1900-01-01 00:00", "2000-01-01 00:00"]

# Variables globales
DICCIONARIO_NOMBRES_NUMEROS = {
    "fecha_creacion_cliente": 1,
    "fecha_creacion_proveedor": 1,
    "fecha_ultima_recepcion": 2,
    "fecha_ultima_carga_documentos": 2,
    "fecha_de_creacion_del_consolidado": 3,
    "fecha_cierre_consolidado_comercial": 3,
    "fecha_aprobacion_documentos": 4,
    "fecha_consolidado_contenedor": 4,
    "etd_nave_asignada": 5,
    "eta": 5,
    "fecha_publicacion": 6,
    "fecha_aforo": 6,
    "fecha_publicacion_aforo": 7,
    "fecha_retiro_puerto": 7,
    "fecha_retiro": 8,
    "fecha_desconsolidacion_pudahuel": 8,
    "fecha_de_pago": 9,
    "fecha_solicitud_despacho": 9,
    "fecha_prog_despacho": 10,
    "fecha_entrega_retiro": 10
}

DICCIONARIO_NUMEROS_NOMBRES = {
    1: ["fecha_creacion_cliente", "fecha_creacion_proveedor"],
    2: ["fecha_ultima_recepcion", "fecha_ultima_carga_documentos"],
    3: ["fecha_de_creacion_del_consolidado", "fecha_cierre_consolidado_comercial"],
    4: ["fecha_aprobacion_documentos", "fecha_consolidado_contenedor"],
    5: ["etd_nave_asignada", "eta"],
    6: ["fecha_publicacion", "fecha_aforo"],
    7: ["fecha_publicacion_aforo", "fecha_retiro_puerto"],
    8: ["fecha_retiro", "fecha_desconsolidacion_pudahuel"],
    9: ["fecha_de_pago", "fecha_solicitud_despacho"],
    10: ["fecha_prog_despacho", "fecha_entrega_retiro"]
}


# Lista de columnas de fecha que deseas modificar
COLUMNAS_FECHA = [
    "fecha_creacion_cliente", "fecha_creacion_proveedor", "fecha_ultima_recepcion",
    "fecha_ultima_carga_documentos", "fecha_de_creacion_del_consolidado",
    "fecha_cierre_consolidado_comercial", "fecha_aprobacion_documentos",
    "fecha_consolidado_contenedor", "etd_nave_asignada", "eta",
    "fecha_publicacion", "fecha_aforo", "fecha_publicacion_aforo",
    "fecha_retiro_puerto", "fecha_retiro", "fecha_desconsolidacion_pudahuel",
    "fecha_de_pago", "fecha_solicitud_despacho", "fecha_prog_despacho",
    "fecha_entrega_retiro"
]
