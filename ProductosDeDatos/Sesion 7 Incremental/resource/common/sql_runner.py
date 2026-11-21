from pathlib import Path

def ejecutar_script_sql(spark, ruta_sql: str, catalogo: str, esquema: str):
    """Resuelve la ruta en /sql/ de forma absoluta, inyecta {catalogo} y {esquema} y ejecuta."""
    carpeta_base = Path(__file__).resolve().parent.parent
    ruta_absoluta = carpeta_base / "sql" / Path(ruta_sql).name

    with open(ruta_absoluta, "r", encoding="utf-8") as f:
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