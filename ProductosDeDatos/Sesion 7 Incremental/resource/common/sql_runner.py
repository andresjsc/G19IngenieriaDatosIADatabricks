from pathlib import Path

def ejecutar_script_sql(spark, nombre_archivo_sql: str, catalogo: str, esquema: str):
    """Resuelve la ruta en /sql/ de forma absoluta, inyecta {catalogo} y {esquema} y ejecuta."""
    carpeta_base = Path(__file__).resolve().parent.parent
    ruta_sql = carpeta_base / "sql" / Path(nombre_archivo_sql).name

    with open(ruta_sql, "r", encoding="utf-8") as f:
        script_sql = (
            f.read()
            .replace("{catalogo}", catalogo)
            .replace("{esquema}", esquema)
        )

    sentencias = [s.strip() for s in script_sql.split(";") if s.strip()]
    ultimo_df = None

    for sentencia in sentencias:
        lineas_codigo = [
            l for l in sentencia.splitlines()
            if not l.strip().startswith("--") and l.strip()
        ]
        if lineas_codigo:
            ultimo_df = spark.sql(sentencia)

    return ultimo_df