# ETL PySpark - Proyecto 3
# Analisis de accidentes de transito en zonas urbanas (EE. UU., 2016-2023)
# Extrae, limpia, deriva variables, responde las preguntas, cruza con poblacion y exporta CSV.

import argparse
import csv
import os
import sys
import time

os.environ.setdefault("PYSPARK_SUBMIT_ARGS", "--driver-memory 6g pyspark-shell")

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

FIPS_A_ESTADO = {
    "01": "AL", "02": "AK", "04": "AZ", "05": "AR", "06": "CA", "08": "CO",
    "09": "CT", "10": "DE", "11": "DC", "12": "FL", "13": "GA", "15": "HI",
    "16": "ID", "17": "IL", "18": "IN", "19": "IA", "20": "KS", "21": "KY",
    "22": "LA", "23": "ME", "24": "MD", "25": "MA", "26": "MI", "27": "MN",
    "28": "MS", "29": "MO", "30": "MT", "31": "NE", "32": "NV", "33": "NH",
    "34": "NJ", "35": "NM", "36": "NY", "37": "NC", "38": "ND", "39": "OH",
    "40": "OK", "41": "OR", "42": "PA", "44": "RI", "45": "SC", "46": "SD",
    "47": "TN", "48": "TX", "49": "UT", "50": "VT", "51": "VA", "53": "WA",
    "54": "WV", "55": "WI", "56": "WY",
}

REGIONES = {"1": "Noreste", "2": "Medio Oeste", "3": "Sur", "4": "Oeste"}

MESES = [
    "Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio",
    "Julio", "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre",
]
DIAS = ["Lunes", "Martes", "Miercoles", "Jueves", "Viernes", "Sabado", "Domingo"]


def parse_args():
    parser = argparse.ArgumentParser(description="ETL PySpark - Accidentes de transito")
    parser.add_argument(
        "--data-path",
        default=r"C:\Users\Alex\AppData\Local\us-accidents\US_Accidents_March23.csv",
        help="Ruta del CSV US Accidents",
    )
    parser.add_argument(
        "--census-path",
        default=os.path.join(ROOT, "data", "NST-EST2023-ALLDATA.csv"),
        help="Estimaciones de poblacion estatal del Census Bureau",
    )
    parser.add_argument(
        "--output-dir",
        default=os.path.join(ROOT, "resultados"),
        help="Carpeta de los CSV de las 10 preguntas",
    )
    parser.add_argument(
        "--model-dir",
        default=os.path.join(ROOT, "modelo"),
        help="Carpeta del modelo para Power BI",
    )
    parser.add_argument(
        "--sample-fraction",
        type=float,
        default=0.012,
        help="Fraccion aleatoria que se guarda para los supuestos del ANOVA",
    )
    return parser.parse_args()


def crear_spark_session():
    """
    SparkSession local.
    El directorio temporal queda fuera de OneDrive para que el shuffle no se sincronice.
    """
    local_dir = r"C:\Users\Alex\AppData\Local\spark-tmp"
    os.makedirs(local_dir, exist_ok=True)
    spark = (
        SparkSession.builder
        .appName("ETL_Accidentes_Transito")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.ansi.enabled", "false")
        .config("spark.local.dir", local_dir)
        .config("spark.driver.maxResultSize", "2g")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    print(f"[OK] SparkSession creada. Version de Spark: {spark.version}")
    return spark


def preparar_demografia(census_path, model_dir):
    """
    La demografia no viene en el dataset de accidentes.
    Se toma la poblacion estatal al 1 de julio de 2023 (Census Bureau, Vintage 2023)
    y se deja lista para el join por sigla del estado.
    """
    print("\n=== Demografia estatal (Census Vintage 2023) ===")
    if not os.path.isfile(census_path):
        raise FileNotFoundError(f"No esta el archivo de poblacion: {census_path}")

    filas = []
    with open(census_path, newline="", encoding="utf-8") as archivo:
        for registro in csv.DictReader(archivo):
            if registro["SUMLEV"] != "040":
                continue
            sigla = FIPS_A_ESTADO.get(registro["STATE"].zfill(2))
            if not sigla:
                continue
            filas.append({
                "state": sigla,
                "estado": registro["NAME"],
                "region": REGIONES.get(registro["REGION"], "Sin region"),
                "poblacion_2023": int(registro["POPESTIMATE2023"]),
            })

    os.makedirs(model_dir, exist_ok=True)
    destino = os.path.join(model_dir, "dim_estado.csv")
    with open(destino, "w", newline="", encoding="utf-8") as archivo:
        writer = csv.DictWriter(archivo, fieldnames=["state", "estado", "region", "poblacion_2023"])
        writer.writeheader()
        writer.writerows(sorted(filas, key=lambda fila: fila["state"]))
    print(f"[EXPORTADO] {destino} ({len(filas)} estados)")
    return destino


def cargar_dataset(spark, data_path):
    """
    Fase de extraccion.
    El CSV se lee como texto y los tipos se aplican despues.
    Asi Spark no adivina el esquema en un archivo de mas de 7 millones de filas.
    """
    if not os.path.isfile(data_path):
        raise FileNotFoundError(
            "No esta el CSV de accidentes. Descargalo desde "
            "https://www.kaggle.com/datasets/sobhanmoosavi/us-accidents "
            f"y pasalo con --data-path. Ruta buscada: {data_path}"
        )
    tamanio = os.path.getsize(data_path)
    if tamanio < 2_700_000_000:
        raise RuntimeError(
            f"El CSV pesa {tamanio} bytes y parece incompleto. "
            "El archivo US_Accidents_March23.csv pesa cerca de 3 GB."
        )

    print("\n=== Paso 1: Extraccion ===")
    df_raw = (
        spark.read.option("header", True)
        .option("inferSchema", False)
        .option("encoding", "utf-8")
        .csv(data_path)
        .select(
            "ID", "Severity", "Start_Time", "End_Time", "Start_Lat", "Start_Lng",
            "Description", "Street", "City", "State", "Weather_Condition",
        )
    )
    print("Columnas usadas:")
    df_raw.printSchema()
    print(f"Registros leidos: {df_raw.count()}")
    return df_raw


def limpiar_datos(df_raw):
    """
    Fase de limpieza. Decisiones, no cortes arbitrarios:
      - Se eliminan duplicados por ID.
      - Sin estado, sin hora de inicio o sin coordenada no se puede ubicar el accidente.
      - Severity fuera de 1-4 se descarta: la escala del dataset solo tiene esos cuatro niveles.
      - La ciudad vacia se conserva como "Sin ciudad" para no perder el accidente en el resto de preguntas.
      - La duracion usa End_Time. Si falta, es negativa o pasa de 24 horas, no entra al promedio:
        en esta fuente End_Time es el cierre del evento de trafico y a veces queda abierto varios dias.
    """
    print("\n=== Paso 2: Limpieza ===")
    n_antes = df_raw.count()
    df = df_raw.dropDuplicates(["ID"])
    n_dedup = df.count()
    print(f"Registros antes de quitar ID repetidos: {n_antes} | despues: {n_dedup}")

    inicio = F.coalesce(
        F.to_timestamp("Start_Time", "yyyy-MM-dd HH:mm:ss"),
        F.to_timestamp("Start_Time", "yyyy-MM-dd HH:mm:ss.SSSSSS"),
        F.to_timestamp("Start_Time"),
    )
    fin = F.coalesce(
        F.to_timestamp("End_Time", "yyyy-MM-dd HH:mm:ss"),
        F.to_timestamp("End_Time", "yyyy-MM-dd HH:mm:ss.SSSSSS"),
        F.to_timestamp("End_Time"),
    )

    df = (
        df.withColumn("severity", F.col("Severity").cast("int"))
        .withColumn("start_ts", inicio)
        .withColumn("end_ts", fin)
        .withColumn("lat", F.col("Start_Lat").cast("double"))
        .withColumn("lng", F.col("Start_Lng").cast("double"))
        .withColumn("state", F.upper(F.trim(F.col("State"))))
        .withColumn(
            "city",
            F.when(F.trim(F.coalesce(F.col("City"), F.lit(""))) == "", F.lit("Sin ciudad"))
            .otherwise(F.initcap(F.trim(F.col("City")))),
        )
    )

    df = df.filter(
        F.col("state").rlike("^[A-Z]{2}$")
        & F.col("start_ts").isNotNull()
        & F.col("severity").between(1, 4)
        & F.col("lat").between(17, 72)
        & F.col("lng").between(-170, -65)
        & F.year("start_ts").between(2016, 2023)
    )

    df = df.withColumn(
        "duration_min",
        (F.unix_timestamp("end_ts") - F.unix_timestamp("start_ts")) / F.lit(60.0),
    ).withColumn(
        "duracion_valida",
        F.col("duration_min").isNotNull()
        & (F.col("duration_min") > 0)
        & (F.col("duration_min") <= 24 * 60),
    )

    print(f"Registros despues de filtros de calidad: {df.count()}")
    return df


def crear_variables_derivadas(df):
    """
    Columnas que las preguntas necesitan y que no vienen hechas en la fuente.
    tipo_via sale del nombre de la calle.
    tipo_vehiculo sale del texto de Description. La mayoria de registros no lo menciona:
    la API de trafico registra el evento, no el tipo de vehiculo. Esos quedan como No especificado.
    tipo_clima agrupa las decenas de textos meteorologicos en familias comparables.
    """
    print("\n=== Paso 3: Variables derivadas ===")

    clima = F.lower(F.trim(F.coalesce(F.col("Weather_Condition"), F.lit(""))))
    tipo_clima = (
        F.when(clima == "", "Sin dato")
        .when(clima.rlike("thunder|t-storm|tornado"), "Tormenta")
        .when(clima.rlike("snow|sleet|wintry|ice pellet|blizzard|freezing"), "Nieve o hielo")
        .when(clima.rlike("rain|drizzle|shower"), "Lluvia")
        .when(clima.rlike("fog|haze|mist|smoke|dust"), "Niebla o visibilidad baja")
        .when(clima.rlike("cloud|overcast"), "Nublado")
        .when(clima.rlike("clear|fair"), "Despejado")
        .otherwise("Otro")
    )

    via = F.upper(F.trim(F.coalesce(F.col("Street"), F.lit(""))))
    tipo_via = (
        F.when(via == "", "Sin nombre")
        .when(via.rlike(r"(^|[^A-Z])I- ?[0-9]|INTERSTATE"), "Interestatal")
        .when(via.rlike(r"(^|[^A-Z])US- ?[0-9]|U\.S\.|US HIGHWAY|US HWY"), "Carretera US")
        .when(via.rlike(r"STATE ROUTE|STATE HWY|STATE HIGHWAY|(^|[^A-Z])SR- ?[0-9]|HIGHWAY|HWY"), "Ruta estatal")
        .otherwise("Via local")
    )

    texto = F.lower(F.coalesce(F.col("Description"), F.lit("")))
    tipo_vehiculo = (
        F.when(texto.rlike(r"\bpedestrian\b"), "Peaton")
        .when(texto.rlike(r"\bbicycle\b|\bbicyclist\b|\bcyclist\b"), "Bicicleta")
        .when(texto.rlike(r"\bmotorcycle\b|\bmotorbike\b"), "Motocicleta")
        .when(texto.rlike(r"\bbus\b"), "Autobus")
        .when(texto.rlike(r"\btruck\b|\btractor\b|\bsemi\b|\blorry\b"), "Camion")
        .when(texto.rlike(r"\bambulance\b|\bfire truck\b"), "Vehiculo de emergencia")
        .otherwise("No especificado")
    )

    # dayofweek de Spark: 1 = domingo ... 7 = sabado. Se pasa a lunes = 1.
    orden_dia = F.when(F.dayofweek("start_ts") == 1, 7).otherwise(F.dayofweek("start_ts") - 1)
    nombre_dia = (
        F.when(orden_dia == 1, "Lunes")
        .when(orden_dia == 2, "Martes")
        .when(orden_dia == 3, "Miercoles")
        .when(orden_dia == 4, "Jueves")
        .when(orden_dia == 5, "Viernes")
        .when(orden_dia == 6, "Sabado")
        .otherwise("Domingo")
    )
    hora = F.hour("start_ts")
    franja = (
        F.when(hora.between(0, 5), "Madrugada")
        .when(hora.between(6, 11), "Manana")
        .when(hora.between(12, 17), "Tarde")
        .otherwise("Noche")
    )

    df = (
        df.withColumn("tipo_clima", tipo_clima)
        .withColumn("tipo_via", tipo_via)
        .withColumn("tipo_vehiculo", tipo_vehiculo)
        .withColumn("hora", hora)
        .withColumn("franja", franja)
        .withColumn("orden_dia", orden_dia)
        .withColumn("dia_semana", nombre_dia)
        .withColumn("anio", F.year("start_ts"))
        .withColumn("mes", F.month("start_ts"))
        .withColumn("fecha", F.to_date("start_ts"))
        .select(
            "severity", "start_ts", "fecha", "lat", "lng", "city", "state",
            "hora", "franja", "orden_dia", "dia_semana", "anio", "mes",
            "duration_min", "duracion_valida", "tipo_clima", "tipo_via", "tipo_vehiculo",
        )
    )
    df.cache()
    print(f"Filas listas para el analisis: {df.count()}")
    df.groupBy("tipo_via").count().orderBy(F.desc("count")).show(truncate=False)
    return df


def exportar_csv(df, nombre, carpeta):
    """
    El resultado ya es pequeno: se recoge en el driver y se escribe un CSV.
    """
    os.makedirs(carpeta, exist_ok=True)
    destino = os.path.join(carpeta, nombre)
    filas = [fila.asDict() for fila in df.collect()]
    with open(destino, "w", newline="", encoding="utf-8-sig") as archivo:
        writer = csv.DictWriter(archivo, fieldnames=list(df.columns))
        writer.writeheader()
        writer.writerows(filas)
    print(f"[EXPORTADO] {destino}")
    return destino


def con_porcentaje(df, columna_conteo, total):
    return df.withColumn(
        "porcentaje",
        F.round(F.col(columna_conteo) / F.lit(total) * 100, 2),
    )


def resolver_preguntas(df, total):
    print("\n=== Paso 4: Diez preguntas ===")
    resultados = {}

    # 1. Frecuencia por estado y ciudad.
    p1 = (
        df.groupBy("state", "city")
        .agg(F.count("*").alias("accidentes"))
        .orderBy(F.desc("accidentes"))
    )
    p1 = con_porcentaje(p1, "accidentes", total)
    print("[P1] Cinco ciudades con mas accidentes:")
    p1.show(5, truncate=False)
    resultados["p1"] = p1

    # 2. Hora del dia con mayor accidentalidad.
    p2 = (
        df.groupBy("hora", "franja")
        .agg(
            F.count("*").alias("accidentes"),
            F.round(F.avg("severity"), 3).alias("severidad_promedio"),
        )
        .orderBy("hora")
    )
    p2 = con_porcentaje(p2, "accidentes", total)
    hora_top = p2.orderBy(F.desc("accidentes")).first()
    print(f"[P2] Hora con mas accidentes: {hora_top['hora']}:00 ({hora_top['accidentes']})")
    resultados["p2"] = p2
    resultados["hora_top"] = hora_top

    # 3. Severidad. La escala va de 1 (menor impacto en el trafico) a 4.
    p3 = (
        df.groupBy("severity")
        .agg(F.count("*").alias("accidentes"))
        .orderBy("severity")
    )
    p3 = con_porcentaje(p3, "accidentes", total)
    print("[P3] Severidad:")
    p3.show(truncate=False)
    resultados["p3"] = p3

    # 4. Condicion climatica y severidad media, para ver la relacion entre ambas.
    p4 = (
        df.groupBy("tipo_clima")
        .agg(
            F.count("*").alias("accidentes"),
            F.round(F.avg("severity"), 3).alias("severidad_promedio"),
        )
        .orderBy(F.desc("accidentes"))
    )
    p4 = con_porcentaje(p4, "accidentes", total)
    print("[P4] Clima:")
    p4.show(truncate=False)
    resultados["p4"] = p4

    # 5. Tipo de via.
    p5 = (
        df.groupBy("tipo_via")
        .agg(
            F.count("*").alias("accidentes"),
            F.round(F.avg("severity"), 3).alias("severidad_promedio"),
        )
        .orderBy(F.desc("accidentes"))
    )
    p5 = con_porcentaje(p5, "accidentes", total)
    print("[P5] Tipo de via:")
    p5.show(truncate=False)
    resultados["p5"] = p5

    # 6. Dia de la semana.
    p6 = (
        df.groupBy("orden_dia", "dia_semana")
        .agg(
            F.count("*").alias("accidentes"),
            F.round(F.avg("severity"), 3).alias("severidad_promedio"),
        )
        .orderBy("orden_dia")
    )
    p6 = con_porcentaje(p6, "accidentes", total)
    print("[P6] Dia de la semana:")
    p6.show(truncate=False)
    resultados["p6"] = p6

    # 7. Duracion media del incidente, solo con duraciones validas.
    validas = df.filter(F.col("duracion_valida"))
    resumen_duracion = validas.agg(
        F.lit("Todos").alias("grupo"),
        F.round(F.avg("duration_min"), 2).alias("promedio_min"),
        F.round(F.percentile_approx("duration_min", 0.5), 2).alias("mediana_min"),
        F.round(F.min("duration_min"), 2).alias("minimo_min"),
        F.round(F.max("duration_min"), 2).alias("maximo_min"),
        F.count("*").alias("incidentes"),
    )
    por_severidad = (
        validas.groupBy("severity")
        .agg(
            F.round(F.avg("duration_min"), 2).alias("promedio_min"),
            F.round(F.percentile_approx("duration_min", 0.5), 2).alias("mediana_min"),
            F.round(F.min("duration_min"), 2).alias("minimo_min"),
            F.round(F.max("duration_min"), 2).alias("maximo_min"),
            F.count("*").alias("incidentes"),
        )
        .withColumn("grupo", F.concat(F.lit("Severidad "), F.col("severity").cast("string")))
        .drop("severity")
        .select("grupo", "promedio_min", "mediana_min", "minimo_min", "maximo_min", "incidentes")
    )
    p7 = resumen_duracion.unionByName(por_severidad).orderBy("grupo")
    print("[P7] Duracion (minutos):")
    p7.show(truncate=False)
    resultados["p7"] = p7

    # 8. Tendencia mensual y anual.
    p8 = (
        df.groupBy("anio", "mes")
        .agg(
            F.count("*").alias("accidentes"),
            F.round(F.avg("severity"), 3).alias("severidad_promedio"),
        )
        .orderBy("anio", "mes")
    )
    print("[P8] Primeros y ultimos meses:")
    p8.show(3, truncate=False)
    resultados["p8"] = p8

    # 9. Tipo de vehiculo mencionado en la descripcion.
    p9 = (
        df.groupBy("tipo_vehiculo")
        .agg(F.count("*").alias("accidentes"))
        .orderBy(F.desc("accidentes"))
    )
    p9 = con_porcentaje(p9, "accidentes", total)
    print("[P9] Tipo de vehiculo:")
    p9.show(truncate=False)
    resultados["p9"] = p9

    # 10. Puntos criticos. Cada celda mide 0.01 grados, cerca de 1 km.
    p10 = (
        df.withColumn("lat", F.round("lat", 2))
        .withColumn("lng", F.round("lng", 2))
        .groupBy("lat", "lng")
        .agg(
            F.count("*").alias("accidentes"),
            F.round(F.avg("severity"), 3).alias("severidad_promedio"),
            F.first("city", ignorenulls=True).alias("city"),
            F.first("state", ignorenulls=True).alias("state"),
        )
        .orderBy(F.desc("accidentes"))
        .limit(50)
    )
    print("[P10] Cinco puntos criticos:")
    p10.show(5, truncate=False)
    resultados["p10"] = p10

    extremos = df.agg(F.min("start_ts").alias("desde"), F.max("start_ts").alias("hasta")).first()
    resultados["desde"] = extremos["desde"]
    resultados["hasta"] = extremos["hasta"]
    resultados["total"] = total
    return resultados


def ejemplo_sql(spark, df):
    """La pregunta de la hora, calculada con una vista temporal SQL."""
    print("\n=== Vista SQL: hora con mas accidentes ===")
    df.createOrReplaceTempView("accidentes")
    spark.sql(
        """
        SELECT hora, COUNT(*) AS accidentes
        FROM accidentes
        GROUP BY hora
        ORDER BY accidentes DESC
        LIMIT 5
        """
    ).show(truncate=False)


def ejemplo_join(spark, df, ruta_demografia):
    """
    Join accidentes-demografia por la sigla del estado.
    La tasa por 100.000 habitantes permite comparar estados de distinto tamano.
    """
    print("\n=== Join con poblacion estatal ===")
    demo = (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .option("encoding", "utf-8")
        .csv(ruta_demografia)
    )
    por_estado = df.groupBy("state").agg(
        F.count("*").alias("accidentes"),
        F.round(F.avg("severity"), 3).alias("severidad_promedio"),
    )
    unido = (
        por_estado.join(demo, on="state", how="left")
        .withColumn(
            "tasa_por_100k",
            F.round(F.col("accidentes") / F.col("poblacion_2023") * F.lit(100000), 2),
        )
    )
    ventana = Window.orderBy(F.desc_nulls_last("tasa_por_100k"))
    unido = unido.withColumn("rank_tasa", F.rank().over(ventana)).orderBy("rank_tasa")
    sin_cruce = unido.filter(F.col("poblacion_2023").isNull()).count()
    print(f"Estados sin cruce demografico: {sin_cruce}")
    print("Estados con mayor tasa por 100.000 habitantes:")
    unido.select(
        "state", "estado", "region", "poblacion_2023", "accidentes", "tasa_por_100k", "rank_tasa"
    ).show(10, truncate=False)
    return unido


def ejemplo_window(df):
    """
    rank empata y salta numeros. row_number no repite.
    Aqui se quedan las 5 ciudades con mas accidentes dentro de cada estado.
    """
    print("\n=== Ventana: top 5 de ciudades dentro de cada estado ===")
    por_ciudad = df.groupBy("state", "city").agg(F.count("*").alias("accidentes"))
    ventana = Window.partitionBy("state").orderBy(F.desc("accidentes"))
    ranking = (
        por_ciudad
        .withColumn("rank", F.rank().over(ventana))
        .withColumn("row_number", F.row_number().over(ventana))
        .filter(F.col("rank") <= 5)
        .orderBy("state", "row_number")
    )
    ranking.show(15, truncate=False)
    return ranking


def exportar_anova(df, output_dir, sample_fraction):
    """
    El ANOVA de severidad por franja horaria usa todos los registros
    a traves de n, media y varianza. La muestra aleatoria solo sirve
    para Shapiro-Wilk y Levene, que no se calculan sobre millones de filas.
    """
    print("\n=== Insumos del ANOVA ===")
    resumen = (
        df.groupBy("franja")
        .agg(
            F.count("*").alias("n"),
            F.round(F.avg("severity"), 6).alias("media"),
            F.round(F.var_samp("severity"), 6).alias("varianza"),
        )
    )
    orden = (
        F.when(F.col("franja") == "Madrugada", 1)
        .when(F.col("franja") == "Manana", 2)
        .when(F.col("franja") == "Tarde", 3)
        .otherwise(4)
    )
    resumen = resumen.withColumn("orden", orden).orderBy("orden")
    exportar_csv(resumen, "anova_por_franja.csv", output_dir)

    muestra = (
        df.select(F.col("severity").alias("severidad"), "franja")
        .sample(False, sample_fraction, seed=42)
    )
    exportar_csv(muestra, "anova_muestra.csv", output_dir)


def exportar_modelo(df, model_dir):
    print("\n=== Modelo para Power BI ===")
    fact = (
        df.groupBy("fecha", "state")
        .agg(
            F.count("*").alias("accidentes"),
            F.sum("severity").alias("suma_severidad"),
            F.round(F.sum(F.when(F.col("duracion_valida"), F.col("duration_min"))), 2).alias("suma_duracion_min"),
            F.sum(F.when(F.col("duracion_valida"), F.lit(1)).otherwise(F.lit(0))).alias("n_con_duracion"),
        )
        .orderBy("fecha", "state")
    )
    exportar_csv(fact, "fact_diario.csv", model_dir)

    fact_hora = (
        df.groupBy("hora", "franja")
        .agg(
            F.count("*").alias("accidentes"),
            F.round(F.avg("severity"), 3).alias("severidad_promedio"),
        )
        .orderBy("hora")
    )
    exportar_csv(fact_hora, "fact_hora.csv", model_dir)

    fact_clima = (
        df.groupBy("tipo_clima")
        .agg(
            F.count("*").alias("accidentes"),
            F.round(F.avg("severity"), 3).alias("severidad_promedio"),
        )
        .orderBy(F.desc("accidentes"))
    )
    exportar_csv(fact_clima, "fact_clima.csv", model_dir)

    fechas = df.select("fecha", "anio", "mes", "dia_semana", "orden_dia").distinct().orderBy("fecha")
    # El nombre del dia ya esta en los hechos. dim_fecha se completa en Python
    # para que el calendario no tenga huecos y Power BI pueda usar inteligencia de tiempo.
    return fechas


def construir_dim_fecha(spark, df, model_dir):
    extremos = df.agg(F.min("fecha").alias("desde"), F.max("fecha").alias("hasta")).first()
    desde = extremos["desde"]
    hasta = extremos["hasta"]
    print(f"Calendario de {desde} a {hasta}")

    import datetime
    dia = desde
    filas = []
    while dia <= hasta:
        filas.append({
            "fecha": dia.isoformat(),
            "anio": dia.year,
            "mes": dia.month,
            "mes_nombre": MESES[dia.month - 1],
            "trimestre": (dia.month - 1) // 3 + 1,
            "dia_semana": dia.weekday() + 1,
            "dia_nombre": DIAS[dia.weekday()],
        })
        dia += datetime.timedelta(days=1)

    destino = os.path.join(model_dir, "dim_fecha.csv")
    with open(destino, "w", newline="", encoding="utf-8-sig") as archivo:
        writer = csv.DictWriter(
            archivo,
            fieldnames=["fecha", "anio", "mes", "mes_nombre", "trimestre", "dia_semana", "dia_nombre"],
        )
        writer.writeheader()
        writer.writerows(filas)
    print(f"[EXPORTADO] {destino} ({len(filas)} dias)")


def exportar_resultados(resultados, output_dir, unido, ranking):
    print("\n=== Exportacion ===")
    exportar_csv(resultados["p1"], "01_frecuencia_estado_ciudad.csv", output_dir)
    exportar_csv(resultados["p2"], "02_hora_dia.csv", output_dir)
    exportar_csv(resultados["p3"], "03_severidad.csv", output_dir)
    exportar_csv(resultados["p4"], "04_clima.csv", output_dir)
    exportar_csv(resultados["p5"], "05_tipo_via.csv", output_dir)
    exportar_csv(resultados["p6"], "06_dia_semana.csv", output_dir)
    exportar_csv(resultados["p7"], "07_duracion.csv", output_dir)
    exportar_csv(resultados["p8"], "08_tendencia_mensual.csv", output_dir)
    exportar_csv(resultados["p9"], "09_tipo_vehiculo.csv", output_dir)

    puntos = [fila.asDict() for fila in resultados["p10"].collect()]
    destino = os.path.join(output_dir, "10_puntos_criticos.csv")
    with open(destino, "w", newline="", encoding="utf-8-sig") as archivo:
        campos = ["rank", "lat", "lng", "city", "state", "accidentes", "severidad_promedio"]
        writer = csv.DictWriter(archivo, fieldnames=campos)
        writer.writeheader()
        for i, fila in enumerate(puntos, start=1):
            writer.writerow({
                "rank": i,
                "lat": fila["lat"],
                "lng": fila["lng"],
                "city": fila["city"],
                "state": fila["state"],
                "accidentes": fila["accidentes"],
                "severidad_promedio": fila["severidad_promedio"],
            })
    print(f"[EXPORTADO] {destino}")

    exportar_csv(
        unido.select(
            "state", "estado", "region", "poblacion_2023", "accidentes",
            "severidad_promedio", "tasa_por_100k", "rank_tasa",
        ),
        "11_demografia_estados.csv",
        output_dir,
    )
    exportar_csv(ranking, "12_top_ciudades_por_estado.csv", output_dir)


def escribir_resumen(resultados, output_dir, unido):
    hora = resultados["hora_top"]
    clima = resultados["p4"].first()
    via = (
        resultados["p5"]
        .filter(F.col("accidentes") >= 1000)
        .orderBy(F.desc("severidad_promedio"))
        .first()
    )
    dia = resultados["p6"].orderBy(F.desc("accidentes")).first()
    duracion = resultados["p7"].filter(F.col("grupo") == "Todos").first()
    ciudad = resultados["p1"].first()
    estado_acc = unido.orderBy(F.desc("accidentes")).first()
    estado_tasa = unido.orderBy("rank_tasa").first()
    vehiculo = (
        resultados["p9"]
        .filter(F.col("tipo_vehiculo") != "No especificado")
        .orderBy(F.desc("accidentes"))
        .first()
    )
    punto = resultados["p10"].first()
    mes = resultados["p8"].orderBy(F.desc("accidentes")).first()

    filas = [
        ("total_accidentes", resultados["total"]),
        ("desde", resultados["desde"]),
        ("hasta", resultados["hasta"]),
        ("hora_pico", hora["hora"]),
        ("hora_pico_accidentes", hora["accidentes"]),
        ("ciudad_top", f"{ciudad['city']}, {ciudad['state']}"),
        ("ciudad_top_accidentes", ciudad["accidentes"]),
        ("estado_top", estado_acc["estado"]),
        ("estado_top_accidentes", estado_acc["accidentes"]),
        ("estado_mayor_tasa", estado_tasa["estado"]),
        ("tasa_mayor", estado_tasa["tasa_por_100k"]),
        ("clima_mas_frecuente", clima["tipo_clima"]),
        ("via_mayor_severidad", via["tipo_via"]),
        ("via_mayor_severidad_media", via["severidad_promedio"]),
        ("dia_pico", dia["dia_semana"]),
        ("duracion_promedio_min", duracion["promedio_min"]),
        ("duracion_mediana_min", duracion["mediana_min"]),
        ("vehiculo_especificado_top", vehiculo["tipo_vehiculo"] if vehiculo else "Ninguno"),
        ("punto_critico", f"{punto['city']}, {punto['state']}"),
        ("punto_critico_accidentes", punto["accidentes"]),
        ("mes_pico", f"{mes['anio']}-{int(mes['mes']):02d}"),
        ("mes_pico_accidentes", mes["accidentes"]),
    ]

    destino = os.path.join(output_dir, "00_resumen.csv")
    with open(destino, "w", newline="", encoding="utf-8-sig") as archivo:
        writer = csv.writer(archivo)
        writer.writerow(["indicador", "valor"])
        writer.writerows(filas)
    print(f"[EXPORTADO] {destino}")

    print("\n=== Tabla resumen ===")
    for indicador, valor in filas:
        print(f"{indicador}: {valor}")


def main():
    args = parse_args()
    inicio = time.time()
    ruta_demo = preparar_demografia(args.census_path, args.model_dir)

    spark = crear_spark_session()
    df_raw = cargar_dataset(spark, args.data_path)
    df_limpio = limpiar_datos(df_raw)
    df = crear_variables_derivadas(df_limpio)
    total = df.count()

    resultados = resolver_preguntas(df, total)
    ejemplo_sql(spark, df)
    unido = ejemplo_join(spark, df, ruta_demo)
    ranking = ejemplo_window(df)

    exportar_resultados(resultados, args.output_dir, unido, ranking)
    exportar_anova(df, args.output_dir, args.sample_fraction)
    exportar_modelo(df, args.model_dir)
    construir_dim_fecha(spark, df, args.model_dir)
    escribir_resumen(resultados, args.output_dir, unido)

    df.unpersist()
    spark.stop()
    print(f"\n[OK] ETL terminado en {(time.time() - inicio) / 60:.1f} minutos.")


if __name__ == "__main__":
    main()
