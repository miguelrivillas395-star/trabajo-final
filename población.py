import random
import hashlib
from datetime import datetime, timedelta

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

# ==========================
# CONFIGURACIÓN GENERAL
# ==========================

DB_CONFIG = {
    "host": "localhost",
    "dbname": "monitoreo_produccion",
    "user": "postgres",
    "password": "postgres",
    "port": 5432,

}

# Parámetros del sensor que quieres simular
# NOTA: Usa IDs que existan en tu base de datos
ID_SENSOR = "A1S01"  # Sensor existente
ID_MICRO = "A1M01"   # Microcontrolador existente
ID_LINEA = "A1"      # Línea existente
ID_FABRICA = "A"     # Fábrica existente (character(1))

# Número de registros a generar
NUM_REGISTROS = 1000

# Fecha y hora inicial de la simulación
FECHA_INICIAL = datetime(2025, 1, 1, 8, 0, 0)  # 1 de enero 2025, 8:00 am

# ==========================
# FUNCIONES AUXILIARES
# ==========================

def clasificar_ppm(ppm: float) -> int:
    """
    Devuelve un id_clasificacion entero según el rango de ppm.
    Ajusta los códigos 1..6 a los que tengas en tu tabla 'clasificacion_ppm'.
    """
    if ppm <= 0.5:
        return 1  # Muy baja
    elif ppm <= 1:
        return 2  # Baja
    elif ppm <= 10:
        return 3  # Moderada
    elif ppm <= 50:
        return 4  # Alta
    elif ppm <= 500:
        return 5  # Muy alta
    else:
        return 6  # Extremadamente peligrosa


def generar_hash_integridad(row: dict) -> str:
    """
    Genera un hash MD5 simple a partir de algunos campos clave.
    Esto es sólo para simular un control de integridad.
    """
    base_str = f"{row['fecha']}{row['hora']}{row['id_sensor']}{row['ppm_benceno']}"
    return hashlib.md5(base_str.encode("utf-8")).hexdigest()


def generar_registros_simulados(
    num_registros: int,
    id_sensor: str,
    id_micro: str,
    id_linea: str,
    id_fabrica: str,
    fecha_inicial: datetime,
):
    """
    Genera una lista de diccionarios con los registros simulados para la tabla 'lecturas'
    y la hoja de cálculo 'lecturas-sensor'.
    """
    registros = []
    instante = fecha_inicial

    for i in range(num_registros):
        # timestamp de la lectura
        fecha = instante.date()
        hora = instante.time()

        # ppm de benceno aleatorio (ejemplo: 0 a 100 ppm, con sesgo hacia valores bajos)
        ppm = round(random.uniform(0, 80), 2)

        # clasificación según ppm
        id_clasif = clasificar_ppm(ppm)

        # ubicación simulada (ejemplo: cerca de una coordenada base)
        geo_lat = 6.2442 + random.uniform(-0.01, 0.01)   # ej. cerca de Medellín
        geo_lon = -75.5812 + random.uniform(-0.01, 0.01)
        geo_alt = 1500 + random.uniform(-10, 10)         # metros

        # variables ambientales
        temperatura = round(random.uniform(18, 35), 1)   # °C
        humedad = round(random.uniform(40, 90), 1)       # %

        estado_tx = "O"  # Cambiado a un solo carácter para coincidir con character(1)
        timestamp_envio = instante

        observaciones = ""
        origen_formato = "simulacion_py"

        row = {
            "fecha": fecha,
            "hora": hora,
            "id_sensor": id_sensor,
            "id_micro": id_micro,
            "id_linea": id_linea,
            "id_fabrica": id_fabrica,
            "ppm_benceno": ppm,
            "id_clasificacion": id_clasif,
            "geo_latitud": geo_lat,
            "geo_longitud": geo_lon,
            "geo_altitud": geo_alt,
            "temperatura": temperatura,
            "humedad": humedad,
            "estado_transmision": estado_tx,
            "timestamp_envio": timestamp_envio,
            "observaciones": observaciones,
            "hash_integridad": "",    # se llena luego
            "origen_formato": origen_formato,
        }

        # hash de integridad
        row["hash_integridad"] = generar_hash_integridad(row)

        registros.append(row)

        # avanzar 10 segundos para la siguiente lectura
        instante += timedelta(seconds=10)

    return registros


# ==========================
# 1) GENERAR LOS REGISTROS
# ==========================

registros = generar_registros_simulados(
    NUM_REGISTROS,
    ID_SENSOR,
    ID_MICRO,
    ID_LINEA,
    ID_FABRICA,
    FECHA_INICIAL,
)

print(f"Se generaron {len(registros)} registros simulados.")

# ==========================
# 2) GUARDAR EN EXCEL
# ==========================

df = pd.DataFrame(registros)

nombre_archivo_excel = f"lecturas-sensor_{ID_SENSOR}.xlsx"
df.to_excel(nombre_archivo_excel, index=False)

print(f"Archivo Excel generado: {nombre_archivo_excel}")

# ==========================
# 3) INSERTAR EN POSTGRESQL
# ==========================

def insertar_en_postgres(registros):
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    # Consultar qué IDs existen en las tablas relacionadas para validación
    try:
        cursor.execute("SELECT id_sensor FROM monitoreo_produccion.sensor LIMIT 10;")
        sensores = [s[0] for s in cursor.fetchall()]
        print(f"Sensores disponibles: {sensores}")
        
        cursor.execute("SELECT id_micro FROM monitoreo_produccion.microcontrolador LIMIT 10;")
        micros = [m[0] for m in cursor.fetchall()]
        print(f"Microcontroladores disponibles: {micros}")
        
        cursor.execute("SELECT id_linea FROM monitoreo_produccion.linea LIMIT 10;")
        lineas = [l[0] for l in cursor.fetchall()]
        print(f"Líneas disponibles: {lineas}")
        
        cursor.execute("SELECT id_fabrica FROM monitoreo_produccion.fabrica LIMIT 10;")
        fabricas = [f[0] for f in cursor.fetchall()]
        print(f"Fábricas disponibles: {fabricas}")
        print()
        
        # Validar que los IDs configurados existan
        if ID_SENSOR not in sensores:
            print(f"ADVERTENCIA: {ID_SENSOR} no existe en la tabla sensor. Usando el primero disponible: {sensores[0] if sensores else 'NINGUNO'}")
        if ID_MICRO not in micros:
            print(f"ADVERTENCIA: {ID_MICRO} no existe en la tabla microcontrolador. Usando el primero disponible: {micros[0] if micros else 'NINGUNO'}")
        if ID_LINEA not in lineas:
            print(f"ADVERTENCIA: {ID_LINEA} no existe en la tabla linea. Usando el primero disponible: {lineas[0] if lineas else 'NINGUNO'}")
        if ID_FABRICA not in fabricas:
            print(f"ADVERTENCIA: {ID_FABRICA} no existe en la tabla fabrica. Usando el primero disponible: {fabricas[0] if fabricas else 'NINGUNO'}")
        print()
    except Exception as e:
        print(f"Error al consultar tablas relacionadas: {e}")
        print("Continuando con la inserción...\n")

    # NOTA: id_lectura es serial, NO se incluye en el INSERT.
    insert_query = """
        INSERT INTO monitoreo_produccion.lecturas (
            fecha,
            hora,
            id_sensor,
            id_micro,
            id_linea,
            id_fabrica,
            ppm_benceno,
            id_clasificacion,
            geo_latitud,
            geo_longitud,
            geo_altitud,
            temperatura,
            humedad,
            estado_transmision,
            timestamp_envio,
            observaciones,
            hash_integridad,
            origen_formato
        ) VALUES %s
    """

    # Convertir lista de dicts a lista de tuplas en el orden correcto
    values = [
        (
            r["fecha"],
            r["hora"],
            r["id_sensor"],
            r["id_micro"],
            r["id_linea"],
            r["id_fabrica"],
            r["ppm_benceno"],
            r["id_clasificacion"],
            r["geo_latitud"],
            r["geo_longitud"],
            r["geo_altitud"],
            r["temperatura"],
            r["humedad"],
            r["estado_transmision"],
            r["timestamp_envio"],
            r["observaciones"],
            r["hash_integridad"],
            r["origen_formato"],
        )
        for r in registros
    ]

    execute_values(cursor, insert_query, values)
    conn.commit()
    cursor.close()
    conn.close()
    print(f"Se insertaron {len(registros)} registros en monitoreo_produccion.lecturas")


# Llamar a la función de inserción
insertar_en_postgres(registros)