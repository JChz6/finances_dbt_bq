import os
import time
from collections import defaultdict, deque
from typing import List, Optional
from fastapi import FastAPI, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from google.cloud import bigquery

def find_credentials():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(current_dir)
    search_dirs = [
        os.path.join(current_dir, "creds"),
        os.path.join(project_root, "creds"),
        "creds"
    ]
    preferred_names = ["trabajador_bq.json", "html-dashboarder-creds.json"]
    for d in search_dirs:
        if os.path.exists(d) and os.path.isdir(d):
            for name in preferred_names:
                p = os.path.join(d, name)
                if os.path.exists(p):
                    return os.path.abspath(p)
            for f in os.listdir(d):
                if f.endswith(".json"):
                    return os.path.abspath(os.path.join(d, f))
    return None

creds_path = find_credentials()
if creds_path:
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = creds_path
app = FastAPI(title="Wealth Dashboard API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Rate limiting básico en memoria: es un fusible de "por si acaso" (bug de frontend en loop,
# fuerza bruta si en el futuro se expone con dominio público), no un control de tráfico fino.
# Ventana deslizante por IP; se reinicia si el proceso se reinicia (aceptable para este uso).
RATE_LIMIT_MAX_REQUESTS = 120
RATE_LIMIT_WINDOW_SECONDS = 60
_rate_limit_buckets: dict[str, deque] = defaultdict(deque)

@app.middleware("http")
async def limitar_tasa(request: Request, call_next):
    cliente_ip = request.client.host if request.client else "unknown"
    ahora = time.monotonic()
    bucket = _rate_limit_buckets[cliente_ip]
    while bucket and ahora - bucket[0] > RATE_LIMIT_WINDOW_SECONDS:
        bucket.popleft()
    if len(bucket) >= RATE_LIMIT_MAX_REQUESTS:
        return JSONResponse(
            status_code=429,
            content={"detail": "Demasiadas solicitudes. Intenta de nuevo en unos segundos."}
        )
    bucket.append(ahora)
    return await call_next(request)

client = bigquery.Client()

def _rango_fechas(fecha_inicio: Optional[str], fecha_fin: Optional[str], campo: str = "txn_time"):
    """Construye condiciones de rango de fecha parametrizadas de forma segura contra SQL injection."""
    condiciones = []
    parametros = []
    if fecha_inicio:
        condiciones.append(f"date({campo}) >= @fecha_inicio")
        parametros.append(bigquery.ScalarQueryParameter("fecha_inicio", "DATE", fecha_inicio))
    if fecha_fin:
        condiciones.append(f"date({campo}) <= @fecha_fin")
        parametros.append(bigquery.ScalarQueryParameter("fecha_fin", "DATE", fecha_fin))
    return condiciones, parametros

def _query(query: str, parametros: list):
    """Ejecuta un query parametrizado en BigQuery."""
    job_config = bigquery.QueryJobConfig(query_parameters=parametros)
    return client.query(query, job_config=job_config).result()

@app.get("/ultima-actualizacion")
def obtener_ultima_actualizacion():
    query = "SELECT MAX(fecha_carga) as ultima_actualizacion FROM `big-query-406221.finanzas_personales_mds.fact_transactions`"
    try:
        resultados = list(client.query(query).result())
        if resultados and resultados[0].ultima_actualizacion:
            return {"ultima_actualizacion": str(resultados[0].ultima_actualizacion)}
    except Exception as e:
        print("Error al obtener ultima actualizacion:", e)
    return {"ultima_actualizacion": "Desconocida"}

@app.get("/", response_class=HTMLResponse)
def read_index():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    index_path = os.path.join(current_dir, "index.html")
    with open(index_path, "r", encoding="utf-8") as f:
        html_content = f.read()
    return HTMLResponse(content=html_content)

@app.get("/flujo-caja")
def obtener_flujo_caja(
    fecha_inicio: Optional[str] = None,
    fecha_fin: Optional[str] = None,
    granularidad: str = "mensual" # "diario" o "mensual"
):
    filtros, parametros = _rango_fechas(fecha_inicio, fecha_fin)
    where_clause = ("WHERE " + " AND ".join(filtros)) if filtros else ""

    if granularidad == "diario":
        query = f"""
            WITH diario AS (
                SELECT 
                  CAST(date(txn_time) AS STRING) AS fecha, 
                  SUM(IF(ingreso_gasto = 'Ingreso', importe_moneda_principal, 0)) AS ingresos,
                  SUM(IF(ingreso_gasto = 'Gastos', importe_moneda_principal * -1, 0)) AS gastos,
                  SUM(
                    CASE
                        WHEN ingreso_gasto = 'Ingreso' THEN importe_moneda_principal
                        WHEN ingreso_gasto = 'Gastos' THEN importe_moneda_principal * -1
                        ELSE 0
                    END
                    ) AS balance
                FROM `big-query-406221.finanzas_personales_mds.fact_transactions` 
                {where_clause}
                GROUP BY 1
            )
            SELECT 
                fecha, 
                ingresos, 
                gastos, 
                balance,
                SUM(balance) OVER(ORDER BY fecha ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS acumulado
            FROM diario
            ORDER BY fecha
        """
    else: # mensual
        query = f"""
            WITH mensual AS (
                SELECT 
                  CAST(DATE_TRUNC(date(txn_time), MONTH) AS STRING) AS fecha, 
                  SUM(IF(ingreso_gasto = 'Ingreso', importe_moneda_principal, 0)) AS ingresos,
                  SUM(IF(ingreso_gasto = 'Gastos', importe_moneda_principal * -1, 0)) AS gastos,
                  SUM(
                    CASE
                        WHEN ingreso_gasto = 'Ingreso' THEN importe_moneda_principal
                        WHEN ingreso_gasto = 'Gastos' THEN importe_moneda_principal * -1
                        ELSE 0
                    END
                    ) AS balance
                FROM `big-query-406221.finanzas_personales_mds.fact_transactions` 
                {where_clause}
                GROUP BY 1
            )
            SELECT 
                fecha, 
                ingresos, 
                gastos, 
                balance,
                SUM(balance) OVER(ORDER BY fecha ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS acumulado
            FROM mensual
            ORDER BY fecha
        """

    resultados = _query(query, parametros)

    fechas, ingresos, gastos, balance, acumulado = [], [], [], [], []
    for fila in resultados:
        fechas.append(fila.fecha)
        ingresos.append(fila.ingresos or 0.0)
        gastos.append(fila.gastos or 0.0)
        balance.append(fila.balance or 0.0)
        acumulado.append(fila.acumulado or 0.0)
        
    return {
        "fechas": fechas,
        "ingresos": ingresos,
        "gastos": gastos,
        "balance": balance,
        "acumulado": acumulado
    }

@app.get("/gastos-categoria")
def obtener_categorias(fecha_inicio: str, fecha_fin: str):
    query = """
        SELECT
            categoria,
            SUM(importe_moneda_principal) as monto
        FROM `big-query-406221.finanzas_personales_mds.fact_transactions`
        WHERE ingreso_gasto = 'Gastos'
          AND date(txn_time) >= @fecha_inicio
          AND date(txn_time) <= @fecha_fin
          AND concepto NOT IN ('Cambio dólares', 'liquidación', 'Sin concepto')
          AND categoria IN
            ("Entretenimiento",
            "Auto",
            "Comida",
            "Mujeres",
            "Viajes",
            "Seguros",
            "Salud",
            "Anuncios",
            "Deudas",
            "Casa",
            "Gastos Variables",
            "Inversiones",
            "Regalos",
            "Transporte",
            "Lavandería",
            "Facturas",
            "Equipo de trabajo",
            "Autocuidado",
            "No comestibles"
            )
        GROUP BY categoria
        ORDER BY monto DESC
    """
    parametros = [
        bigquery.ScalarQueryParameter("fecha_inicio", "DATE", fecha_inicio),
        bigquery.ScalarQueryParameter("fecha_fin", "DATE", fecha_fin),
    ]
    resultados = _query(query, parametros)

    categorias, montos = [], []
    for fila in resultados:
        categorias.append(fila.categoria)
        montos.append(fila.monto or 0.0)
        
    return {"categorias": categorias, "montos": montos}

@app.get("/balance-trimestre")
def obtener_trimestre(fecha_inicio: Optional[str] = None, fecha_fin: Optional[str] = None):
    filtros, parametros = _rango_fechas(fecha_inicio, fecha_fin)
    where_clause = ("WHERE " + " AND ".join(filtros)) if filtros else ""

    query = f"""
        SELECT
            CAST(DATE_TRUNC(date(txn_time), QUARTER) AS STRING) AS trimestre,
            SUM(IF(LOWER(ingreso_gasto) LIKE '%ingr%', importe_moneda_principal, importe_moneda_principal * -1)) AS balance
        FROM `big-query-406221.finanzas_personales_mds.fact_transactions`
        {where_clause}
        GROUP BY 1
        ORDER BY trimestre
    """
    resultados = _query(query, parametros)

    trimestres, balances = [], []
    for fila in resultados:
        trimestres.append(fila.trimestre)
        balances.append(fila.balance or 0.0)
        
    return {"trimestres": trimestres, "balances": balances}

@app.get("/top-gastos")
def obtener_top_gastos(
    fecha_inicio: str,
    fecha_fin: str,
    categoria: Optional[str] = None,
    limite: int = 10
):
    filtros = [
        "ingreso_gasto = 'Gastos'",
        "date(txn_time) >= @fecha_inicio",
        "date(txn_time) <= @fecha_fin",
        "LOWER(concepto) NOT IN ('quinta categoría', 'aporte afp')"
    ]
    parametros = [
        bigquery.ScalarQueryParameter("fecha_inicio", "DATE", fecha_inicio),
        bigquery.ScalarQueryParameter("fecha_fin", "DATE", fecha_fin),
        bigquery.ScalarQueryParameter("limite", "INT64", limite),
    ]
    if categoria:
        filtros.append("categoria = @categoria")
        parametros.append(bigquery.ScalarQueryParameter("categoria", "STRING", categoria))

    where_clause = " AND ".join(filtros)

    query = """
        SELECT
            CAST(date(txn_time) AS STRING) AS fecha,
            categoria,
            concepto as descripcion,
            importe_moneda_principal as monto
        FROM `big-query-406221.finanzas_personales_mds.fact_transactions`
        WHERE """ + where_clause + """
        ORDER BY importe_moneda_principal DESC
        LIMIT @limite
    """
    resultados = _query(query, parametros)

    gastos = []
    for fila in resultados:
        gastos.append({
            "fecha": fila.fecha,
            "descripcion": fila.descripcion or "Sin concepto",
            "categoria": fila.categoria or "Sin categoría",
            "monto": fila.monto or 0.0
        })
        
    return {"gastos": gastos}

@app.get("/cumplimiento-presupuesto")
def obtener_cumplimiento_presupuesto(
    anios: Optional[List[int]] = Query(None),
    meses: Optional[List[int]] = Query(None),
    categorias: Optional[List[str]] = Query(None)
):
    filtros = []
    parametros = []
    if anios:
        filtros.append("EXTRACT(YEAR FROM date(fecha)) IN UNNEST(@anios)")
        parametros.append(bigquery.ArrayQueryParameter("anios", "INT64", anios))
    if meses:
        filtros.append("EXTRACT(MONTH FROM date(fecha)) IN UNNEST(@meses)")
        parametros.append(bigquery.ArrayQueryParameter("meses", "INT64", meses))
    if categorias:
        filtros.append("categoria IN UNNEST(@categorias)")
        parametros.append(bigquery.ArrayQueryParameter("categorias", "STRING", categorias))

    where_clause = " AND ".join(filtros)
    if where_clause:
        where_clause = "WHERE " + where_clause

    query = f"""
        SELECT 
            CAST(date(fecha) AS STRING) as fecha,
            categoria,
            CAST(gasto_acumulado_mes AS FLOAT64) as gasto_acumulado_mes,
            CAST(presupuesto AS FLOAT64) as presupuesto,
            utilizado,
            CAST(presupuesto_disponible AS FLOAT64) as presupuesto_disponible,
            cumplimiento
        FROM `big-query-406221.finanzas_personales_mds.agg_cumplimiento_presupuesto`
        {where_clause}
        ORDER BY fecha, categoria
    """
    resultados = _query(query, parametros)

    datos = []
    for fila in resultados:
        datos.append({
            "fecha": fila.fecha,
            "categoria": fila.categoria,
            "gasto_acumulado_mes": fila.gasto_acumulado_mes or 0.0,
            "presupuesto": fila.presupuesto or 0.0,
            "utilizado": fila.utilizado or "0%",
            "presupuesto_disponible": fila.presupuesto_disponible or 0.0,
            "cumplimiento": fila.cumplimiento or "DESCONOCIDO"
        })
        
    return datos

@app.get("/crecimiento-kpis")
def obtener_crecimiento_kpis(fecha_inicio: Optional[str] = None, fecha_fin: Optional[str] = None):
    fondo_emergencia = 30000.0
    
    query_estricta = """
        SELECT SUM(presupuesto) as total
        FROM `big-query-406221.finanzas_personales_mds.presupuesto_materialized`
        WHERE fecha = (SELECT MAX(fecha) FROM `big-query-406221.finanzas_personales_mds.presupuesto_materialized`)
          AND categoria IN ('Comida', 'Transporte', 'Facturas', 'Deudas indispensables', 'Salud', 'Gastos Variables', 'Seguros')
    """
    
    query_vida = """
        SELECT SUM(presupuesto) as total
        FROM `big-query-406221.finanzas_personales_mds.presupuesto_materialized`
        WHERE fecha = (SELECT MAX(fecha) FROM `big-query-406221.finanzas_personales_mds.presupuesto_materialized`)
    """
    
    total_estricta = 0.0
    total_vida = 0.0
    
    try:
        res_estricta = list(client.query(query_estricta).result())
        if res_estricta and res_estricta[0].total is not None:
            total_estricta = float(res_estricta[0].total)
    except Exception as e:
        print("Error en query_estricta en presupuesto_materialized (intentando fallback):", e)
        try:
            fallback_query = """
                SELECT SUM(CAST(presupuesto AS FLOAT64)) as total
                FROM `big-query-406221.finanzas_personales_mds.agg_cumplimiento_presupuesto`
                WHERE date(fecha) = (SELECT MAX(date(fecha)) FROM `big-query-406221.finanzas_personales_mds.agg_cumplimiento_presupuesto`)
                  AND categoria IN ('Comida', 'Transporte', 'Facturas', 'Deudas indispensables', 'Salud', 'Gastos Variables', 'Seguros')
            """
            res_fb = list(client.query(fallback_query).result())
            if res_fb and res_fb[0].total is not None:
                total_estricta = float(res_fb[0].total)
        except Exception as fb_err:
            print("Error en fallback_query_estricta:", fb_err)
        
    try:
        res_vida = list(client.query(query_vida).result())
        if res_vida and res_vida[0].total is not None:
            total_vida = float(res_vida[0].total)
    except Exception as e:
        print("Error en query_vida en presupuesto_materialized (intentando fallback):", e)
        try:
            fallback_query = """
                SELECT SUM(CAST(presupuesto AS FLOAT64)) as total
                FROM `big-query-406221.finanzas_personales_mds.agg_cumplimiento_presupuesto`
                WHERE date(fecha) = (SELECT MAX(date(fecha)) FROM `big-query-406221.finanzas_personales_mds.agg_cumplimiento_presupuesto`)
            """
            res_fb = list(client.query(fallback_query).result())
            if res_fb and res_fb[0].total is not None:
                total_vida = float(res_fb[0].total)
        except Exception as fb_err:
            print("Error en fallback_query_vida:", fb_err)

    # Mediana de ingreso_neto
    filtros, parametros = _rango_fechas(fecha_inicio, fecha_fin, campo="fecha")
    where_clause = ("WHERE " + " AND ".join(filtros)) if filtros else ""

    query_mediana = f"""
        SELECT DISTINCT PERCENTILE_CONT(ingreso_neto, 0.5) OVER() as mediana
        FROM `big-query-406221.finanzas_personales_mds.agg_ingresos`
        {where_clause}
    """
    mediana_ingreso_neto = 0.0
    try:
        res_mediana = list(_query(query_mediana, parametros))
        if res_mediana and res_mediana[0].mediana is not None:
            mediana_ingreso_neto = float(res_mediana[0].mediana)
    except Exception as e:
        print("Error en query_mediana:", e)

    # Tasa de ahorro de caja vs tasa de construcción de patrimonio.
    # Construcción de patrimonio = aportes a FIBRAS + transferencias netas a cuentas de alto
    # rendimiento + amortización voluntaria de capital (categoria 'Deudas' o
    # Inversiones/Inmuebles/Amortización, según cómo se haya registrado la transacción)
    # + el capital (no el interés ni seguros) de la cuota hipotecaria obligatoria del periodo.
    filtros_periodo, parametros_periodo = _rango_fechas(fecha_inicio, fecha_fin)
    where_periodo = ("WHERE " + " AND ".join(filtros_periodo)) if filtros_periodo else ""

    query_flujo_periodo = f"""
        SELECT
            SUM(IF(ingreso_gasto = 'Ingreso', importe_moneda_principal, 0)) AS ingresos,
            SUM(IF(ingreso_gasto = 'Gastos', importe_moneda_principal, 0)) AS gastos
        FROM `big-query-406221.finanzas_personales_mds.fact_transactions`
        {where_periodo}
    """
    query_construccion_patrimonio = f"""
        SELECT
            SUM(IF(categoria = 'Inversiones' AND subcategoria = 'FIBRAS', importe_moneda_principal, 0)) AS fibras,
            SUM(
                IF(categoria = 'Deudas'
                   OR (categoria = 'Inversiones' AND subcategoria = 'Inmuebles' AND concepto = 'Amortización'),
                   importe_moneda_principal, 0)
            ) AS amortizacion_voluntaria,
            SUM(
                CASE
                    WHEN cuenta IN ('Wow Compartamos', 'Pichincha', 'GNB') AND ingreso_gasto = 'Dinero ingresado' THEN importe_moneda_principal
                    WHEN cuenta IN ('Wow Compartamos', 'Pichincha', 'GNB') AND ingreso_gasto = 'Dinero gastado' THEN -importe_moneda_principal
                    ELSE 0
                END
            ) AS cuentas_alto_rendimiento
        FROM `big-query-406221.finanzas_personales_mds.fact_transactions`
        {where_periodo}
    """

    # Capital de la cuota hipotecaria OBLIGATORIA (no solo la amortización voluntaria):
    # pagar el capital de la cuota mensual también construye equity, sea la cuota obligatoria
    # o un abono extra voluntario — la diferencia real es capital (patrimonio) vs interés y
    # seguros (consumo real, el costo de haber pedido el préstamo).
    filtros_hip, parametros_hip = _rango_fechas(fecha_inicio, fecha_fin, campo="fecha_vencimiento")
    where_hip = "WHERE pagado" + ("".join(f" AND {f}" for f in filtros_hip))
    query_capital_hipoteca_periodo = f"""
        SELECT SUM(capital_cuota) AS capital
        FROM `big-query-406221.finanzas_personales_mds.hipotecas_materialized`
        {where_hip}
    """

    ingresos_periodo = 0.0
    gastos_totales_periodo = 0.0
    fibras_periodo = 0.0
    amortizacion_periodo = 0.0
    cuentas_alto_rendimiento_periodo = 0.0
    capital_hipoteca_periodo = 0.0
    try:
        res_flujo = list(_query(query_flujo_periodo, parametros_periodo))
        if res_flujo:
            ingresos_periodo = float(res_flujo[0].ingresos or 0.0)
            gastos_totales_periodo = float(res_flujo[0].gastos or 0.0)
    except Exception as e:
        print("Error en query_flujo_periodo:", e)

    try:
        res_cp = list(_query(query_construccion_patrimonio, parametros_periodo))
        if res_cp:
            fibras_periodo = float(res_cp[0].fibras or 0.0)
            amortizacion_periodo = float(res_cp[0].amortizacion_voluntaria or 0.0)
            cuentas_alto_rendimiento_periodo = float(res_cp[0].cuentas_alto_rendimiento or 0.0)
    except Exception as e:
        print("Error en query_construccion_patrimonio:", e)

    try:
        res_cap_hip = list(_query(query_capital_hipoteca_periodo, parametros_hip))
        if res_cap_hip and res_cap_hip[0].capital is not None:
            capital_hipoteca_periodo = float(res_cap_hip[0].capital)
    except Exception as e:
        print("Error en query_capital_hipoteca_periodo:", e)

    construccion_patrimonio_periodo = (
        fibras_periodo + amortizacion_periodo + cuentas_alto_rendimiento_periodo + capital_hipoteca_periodo
    )

    # Gasto de consumo real: 'gastos_totales_periodo' incluye FIBRAS, amortización voluntaria
    # y la cuota hipotecaria completa (capital + interés + seguros), porque en el ledger se
    # registran con ingreso_gasto = 'Gastos'. Se resta el capital (de ambas fuentes) para que
    # no quede contado dos veces — como "gasto" y como "construcción de patrimonio" — dejando
    # dentro del gasto de consumo solo interés y seguros de la hipoteca, que sí son costo real.
    # Las cuentas de alto rendimiento no se restan porque nunca estuvieron en el bucket
    # 'Gastos' (se registran como 'Dinero ingresado'/'Dinero gastado', transferencia interna).
    gasto_consumo_periodo = gastos_totales_periodo - fibras_periodo - amortizacion_periodo - capital_hipoteca_periodo

    tasa_gasto_consumo = (gasto_consumo_periodo / ingresos_periodo * 100.0) if ingresos_periodo > 0 else 0.0
    tasa_construccion_patrimonio = (construccion_patrimonio_periodo / ingresos_periodo * 100.0) if ingresos_periodo > 0 else 0.0
    # Ahorro de caja: residual — lo que no fue ni a gasto de consumo ni a construcción de
    # patrimonio. Por construcción, tasa_gasto_consumo + tasa_construccion_patrimonio +
    # tasa_ahorro_caja = 100% (puede dar negativo si se retiró más de lo que entró).
    tasa_ahorro_caja = (100.0 - tasa_gasto_consumo - tasa_construccion_patrimonio) if ingresos_periodo > 0 else 0.0

    # Ratio Deuda/Ingreso (DTI): solo la cuota indispensable (obligatoria), sin las
    # amortizaciones voluntarias (esas ya están en tasa_construccion_patrimonio).
    query_deuda_indispensable = """
        SELECT SUM(presupuesto) as total
        FROM `big-query-406221.finanzas_personales_mds.presupuesto_materialized`
        WHERE fecha = (SELECT MAX(fecha) FROM `big-query-406221.finanzas_personales_mds.presupuesto_materialized`)
          AND categoria = 'Deudas indispensables'
    """
    deuda_indispensable_mensual = 0.0
    try:
        res_deuda = list(client.query(query_deuda_indispensable).result())
        if res_deuda and res_deuda[0].total is not None:
            deuda_indispensable_mensual = float(res_deuda[0].total)
    except Exception as e:
        print("Error en query_deuda_indispensable (intentando fallback):", e)
        try:
            fallback_query = """
                SELECT SUM(CAST(presupuesto AS FLOAT64)) as total
                FROM `big-query-406221.finanzas_personales_mds.agg_cumplimiento_presupuesto`
                WHERE date(fecha) = (SELECT MAX(date(fecha)) FROM `big-query-406221.finanzas_personales_mds.agg_cumplimiento_presupuesto`)
                  AND categoria = 'Deudas indispensables'
            """
            res_fb = list(client.query(fallback_query).result())
            if res_fb and res_fb[0].total is not None:
                deuda_indispensable_mensual = float(res_fb[0].total)
        except Exception as fb_err:
            print("Error en fallback_query_deuda_indispensable:", fb_err)

    ratio_deuda_ingreso = (deuda_indispensable_mensual / mediana_ingreso_neto * 100.0) if mediana_ingreso_neto > 0 else 0.0

    return {
        "fondo_emergencia": fondo_emergencia,
        "supervivencia_estricta": total_estricta,
        "supervivencia_vida": total_vida,
        "mediana_ingreso_neto": mediana_ingreso_neto,
        "tasa_gasto_consumo": round(tasa_gasto_consumo, 1),
        "tasa_construccion_patrimonio": round(tasa_construccion_patrimonio, 1),
        "tasa_ahorro_caja": round(tasa_ahorro_caja, 1),
        "deuda_indispensable_mensual": deuda_indispensable_mensual,
        "ratio_deuda_ingreso": round(ratio_deuda_ingreso, 1)
    }

@app.get("/net-worth")
def obtener_net_worth():
    """
    Patrimonio neto = equity de hipoteca (capital amortizado) + aportes acumulados a FIBRAS +
    saldo neto en cuentas de alto rendimiento - capital pendiente de la hipoteca.
    No incluye valorización de activos (ni plusvalía de FIBRAS ni del inmueble): solo importes
    reales verificables (aportes/saldos), tal como se decidió con el usuario.
    """
    # Capital total del préstamo: TODA la tabla (cuotas pasadas y futuras), sin filtrar por
    # fecha. Si se filtrara por fecha_vencimiento <= hoy, se estaría ignorando el capital de
    # las cuotas futuras aún no vencidas, subestimando gravemente la deuda pendiente real.
    q_hipoteca_total = """
        SELECT SUM(capital_total) AS total
        FROM `big-query-406221.finanzas_personales_mds.hipotecas_materialized`
    """
    # Serie mensual de equity (solo para graficar la evolución histórica hasta hoy)
    q_hipoteca_mensual = """
        SELECT
            CAST(DATE_TRUNC(fecha_vencimiento, MONTH) AS STRING) AS mes,
            SUM(IF(pagado, capital_total, 0)) AS capital_pagado_mes
        FROM `big-query-406221.finanzas_personales_mds.hipotecas_materialized`
        WHERE fecha_vencimiento <= CURRENT_DATE()
        GROUP BY 1
        ORDER BY mes
    """
    q_fibras_mensual = """
        SELECT
            CAST(DATE_TRUNC(txn_time, MONTH) AS STRING) AS mes,
            SUM(importe_moneda_principal) AS monto
        FROM `big-query-406221.finanzas_personales_mds.fact_transactions`
        WHERE categoria = 'Inversiones' AND subcategoria = 'FIBRAS'
        GROUP BY 1
        ORDER BY mes
    """
    q_cuentas_mensual = """
        SELECT
            CAST(DATE_TRUNC(txn_time, MONTH) AS STRING) AS mes,
            SUM(
                CASE
                    WHEN ingreso_gasto IN ('Ingreso', 'Dinero ingresado') THEN importe_moneda_principal
                    WHEN ingreso_gasto IN ('Gastos', 'Dinero gastado') THEN -importe_moneda_principal
                    ELSE 0
                END
            ) AS monto_neto
        FROM `big-query-406221.finanzas_personales_mds.fact_transactions`
        WHERE cuenta IN ('Wow Compartamos', 'Pichincha', 'GNB')
        GROUP BY 1
        ORDER BY mes
    """
    # Efectivo en cuentas regulares: balance de caja acumulado de todo el historial (Ingreso -
    # Gastos, igual que el "acumulado" de /flujo-caja pero sin filtro de fecha) menos lo que ya
    # se movió a cuentas de alto rendimiento. Ese movimiento se registra como 'Dinero
    # ingresado'/'Dinero gastado' (no como 'Ingreso'/'Gastos'), así que no reduce el balance de
    # caja por sí solo — hay que restarlo explícitamente para no contar ese efectivo dos veces
    # (una como "cuenta regular" y otra como "cuenta de alto rendimiento").
    q_balance_mensual = """
        SELECT
            CAST(DATE_TRUNC(txn_time, MONTH) AS STRING) AS mes,
            SUM(IF(ingreso_gasto = 'Ingreso', importe_moneda_principal, 0))
                - SUM(IF(ingreso_gasto = 'Gastos', importe_moneda_principal, 0)) AS balance
        FROM `big-query-406221.finanzas_personales_mds.fact_transactions`
        GROUP BY 1
        ORDER BY mes
    """

    vacio = {"meses": [], "activos": {}, "pasivos": {}, "patrimonio_neto": [], "snapshot": {}}
    try:
        res_total = list(client.query(q_hipoteca_total).result())
        capital_total_hipoteca = float(res_total[0].total or 0.0) if res_total else 0.0
        hip_rows = list(client.query(q_hipoteca_mensual).result())
        fibras_rows = list(client.query(q_fibras_mensual).result())
        cuentas_rows = list(client.query(q_cuentas_mensual).result())
        balance_rows = list(client.query(q_balance_mensual).result())
    except Exception as e:
        print("Error en net-worth:", e)
        return vacio

    def _acumulado_por_mes(filas, campo_monto):
        acumulado = {}
        total = 0.0
        for fila in sorted(filas, key=lambda f: f.mes or ""):
            mes = (fila.mes or "")[:7]
            total += float(getattr(fila, campo_monto) or 0.0)
            acumulado[mes] = total
        return acumulado

    equity_cum = _acumulado_por_mes(hip_rows, "capital_pagado_mes")
    fibras_cum = _acumulado_por_mes(fibras_rows, "monto")
    cuentas_cum = _acumulado_por_mes(cuentas_rows, "monto_neto")
    balance_cum = _acumulado_por_mes(balance_rows, "balance")

    # La hipoteca no existía antes de su primera cuota — antes de ese mes, equity y deuda
    # pendiente deben ser 0, no la deuda total completa (que es lo que saldría si simplemente
    # no hubiera valor todavía en equity_cum para esos meses).
    mes_inicio_hipoteca = min(equity_cum) if equity_cum else None

    # Antes de mayo 2024 las transacciones no se registraban de forma consistente, así que
    # esos meses no se muestran — pero sí se siguen acumulando (no se descartan del cálculo),
    # para que el arranque de la serie visible ya traiga el acumulado correcto.
    MES_INICIO_SERIE = "2024-05"

    todos_los_meses = sorted(set(equity_cum) | set(fibras_cum) | set(cuentas_cum) | set(balance_cum))
    if not todos_los_meses:
        return vacio

    meses, equity_serie, fibras_serie, cuentas_serie, efectivo_serie = [], [], [], [], []
    pendiente_serie, patrimonio_serie = [], []
    ultimo_equity = ultimo_fibras = ultimo_cuentas = ultimo_balance = 0.0

    for mes in todos_los_meses:
        ultimo_fibras = fibras_cum.get(mes, ultimo_fibras)
        ultimo_cuentas = cuentas_cum.get(mes, ultimo_cuentas)
        ultimo_balance = balance_cum.get(mes, ultimo_balance)

        if mes_inicio_hipoteca and mes >= mes_inicio_hipoteca:
            ultimo_equity = equity_cum.get(mes, ultimo_equity)
            pendiente = capital_total_hipoteca - ultimo_equity
        else:
            ultimo_equity = 0.0
            pendiente = 0.0

        efectivo = ultimo_balance - ultimo_cuentas
        patrimonio = ultimo_equity + ultimo_fibras + ultimo_cuentas + efectivo - pendiente

        if mes < MES_INICIO_SERIE:
            continue

        meses.append(mes)
        equity_serie.append(round(ultimo_equity, 2))
        fibras_serie.append(round(ultimo_fibras, 2))
        cuentas_serie.append(round(ultimo_cuentas, 2))
        efectivo_serie.append(round(efectivo, 2))
        pendiente_serie.append(round(pendiente, 2))
        patrimonio_serie.append(round(patrimonio, 2))

    if not meses:
        return vacio

    return {
        "meses": meses,
        "activos": {
            "equity_hipoteca": equity_serie,
            "fibras": fibras_serie,
            "cuentas_alto_rendimiento": cuentas_serie,
            "efectivo": efectivo_serie
        },
        "pasivos": {
            "hipoteca_pendiente": pendiente_serie
        },
        "patrimonio_neto": patrimonio_serie,
        "snapshot": {
            "equity_hipoteca": equity_serie[-1],
            "fibras": fibras_serie[-1],
            "cuentas_alto_rendimiento": cuentas_serie[-1],
            "efectivo": efectivo_serie[-1],
            "hipoteca_pendiente": pendiente_serie[-1],
            "patrimonio_neto": patrimonio_serie[-1]
        }
    }

@app.get("/gasto-esencial-discrecional")
def obtener_gasto_esencial_discrecional(fecha_inicio: Optional[str] = None, fecha_fin: Optional[str] = None):
    """
    Tendencia mensual de gasto esencial vs discrecional. Excluye 'Inversiones' (FIBRAS,
    amortización de inmuebles) y 'Deudas' (amortización voluntaria) porque son construcción
    de patrimonio, no consumo — ver tasa_construccion_patrimonio en /crecimiento-kpis.
    """
    filtros, parametros = _rango_fechas(fecha_inicio, fecha_fin)
    filtros = ["ingreso_gasto = 'Gastos'", "categoria NOT IN ('Inversiones', 'Deudas')"] + filtros
    where_clause = "WHERE " + " AND ".join(filtros)

    categorias_esenciales_sql = "'Comida', 'Transporte', 'Facturas', 'Deudas indispensables', 'Salud', 'Gastos Variables', 'Seguros'"

    query = f"""
        SELECT
            CAST(DATE_TRUNC(date(txn_time), MONTH) AS STRING) AS mes,
            SUM(IF(categoria IN ({categorias_esenciales_sql}), importe_moneda_principal, 0)) AS esencial,
            SUM(IF(categoria NOT IN ({categorias_esenciales_sql}), importe_moneda_principal, 0)) AS discrecional
        FROM `big-query-406221.finanzas_personales_mds.fact_transactions`
        {where_clause}
        GROUP BY 1
        ORDER BY mes
    """
    try:
        resultados = _query(query, parametros)
        meses, esencial, discrecional, pct_esencial = [], [], [], []
        for fila in resultados:
            mes = fila.mes[:7] if fila.mes else ""
            e = float(fila.esencial or 0.0)
            d = float(fila.discrecional or 0.0)
            total = e + d
            meses.append(mes)
            esencial.append(round(e, 2))
            discrecional.append(round(d, 2))
            pct_esencial.append(round((e / total * 100), 1) if total > 0 else 0.0)
        return {
            "meses": meses,
            "esencial": esencial,
            "discrecional": discrecional,
            "pct_esencial": pct_esencial
        }
    except Exception as e:
        print("Error en gasto-esencial-discrecional:", e)
        return {"meses": [], "esencial": [], "discrecional": [], "pct_esencial": []}

@app.get("/ingreso-neto-mensual")
def obtener_ingreso_neto_mensual(fecha_inicio: Optional[str] = None, fecha_fin: Optional[str] = None):
    filtros, parametros = _rango_fechas(fecha_inicio, fecha_fin, campo="fecha")
    where_clause = ("WHERE " + " AND ".join(filtros)) if filtros else ""

    query = f"""
        SELECT CAST(DATE(fecha) AS STRING) AS fecha, ingreso_neto
        FROM `big-query-406221.finanzas_personales_mds.agg_ingresos`
        {where_clause}
        ORDER BY fecha
    """
    try:
        resultados = _query(query, parametros)
        fechas = []
        neto = []
        for fila in resultados:
            fechas.append(fila.fecha[:7] if fila.fecha else "")
            neto.append(float(fila.ingreso_neto or 0.0))
        return {
            "fechas": fechas,
            "neto": neto
        }
    except Exception as e:
        print("Error en ingreso_neto_mensual:", e)
        return {"fechas": [], "neto": []}

@app.get("/ingreso-pasivo")
def obtener_ingreso_pasivo():
    query = """
        SELECT
            CAST(DATE_TRUNC(date(txn_time), MONTH) AS STRING) AS fecha,
            SUM(importe_moneda_principal) AS monto
        FROM `big-query-406221.finanzas_personales_mds.fact_transactions`
        WHERE categoria = 'Pasivo'
        GROUP BY 1
        ORDER BY fecha
    """
    try:
        resultados = client.query(query).result()
        fechas = []
        mensual = []
        acumulado = []
        total_acumulado = 0.0
        
        for fila in resultados:
            val = float(fila.monto or 0.0)
            total_acumulado += val
            fechas.append(fila.fecha[:7] if fila.fecha else "")
            mensual.append(val)
            acumulado.append(total_acumulado)
            
        return {
            "fechas": fechas,
            "mensual": mensual,
            "acumulado": acumulado
        }
    except Exception as e:
        print("Error en ingreso_pasivo:", e)
        return {"fechas": [], "mensual": [], "acumulado": []}

@app.get("/libertad-financiera")
def obtener_libertad_financiera():
    query = """
        WITH pasivo AS (
            SELECT
                CAST(DATE_TRUNC(date(txn_time), MONTH) AS STRING) AS fecha,
                SUM(importe_moneda_principal) AS pasivo
            FROM `big-query-406221.finanzas_personales_mds.fact_transactions`
            WHERE categoria = 'Pasivo'
            GROUP BY 1
        ),
        indispensable AS (
            SELECT
                CAST(DATE_TRUNC(date(txn_time), MONTH) AS STRING) AS fecha,
                SUM(importe_moneda_principal) AS indispensable
            FROM `big-query-406221.finanzas_personales_mds.fact_transactions`
            WHERE categoria IN ('Comida', 'Transporte', 'Facturas', 'Deudas', 'Salud', 'Gastos Variables')
              AND ingreso_gasto = 'Gastos'
            GROUP BY 1
        )
        SELECT 
            COALESCE(p.fecha, i.fecha) AS fecha,
            COALESCE(p.pasivo, 0) AS pasivo,
            COALESCE(i.indispensable, 0) AS indispensable
        FROM pasivo p
        FULL OUTER JOIN indispensable i ON p.fecha = i.fecha
        ORDER BY fecha
    """
    try:
        resultados = client.query(query).result()
        fechas = []
        pasivo = []
        indispensable = []
        cobertura = []
        
        for fila in resultados:
            if not fila.fecha: continue
            f_pas = float(fila.pasivo or 0.0)
            f_ind = float(fila.indispensable or 0.0)
            fechas.append(fila.fecha[:7])
            pasivo.append(f_pas)
            indispensable.append(f_ind)
            cobertura.append(round((f_pas / f_ind * 100), 1) if f_ind > 0 else 0.0)
            
        import datetime
        current_month = datetime.date.today().strftime("%Y-%m")
        latest_cobertura = 0.0
        if fechas:
            if fechas[-1] == current_month and len(cobertura) > 1:
                # If current month has no passive income (or is incomplete), use previous month
                latest_cobertura = cobertura[-2]
            else:
                latest_cobertura = cobertura[-1]
            
        return {
            "fechas": fechas[-12:],
            "pasivo": pasivo[-12:],
            "indispensable": indispensable[-12:],
            "cobertura": cobertura[-12:],
            "latest_cobertura": latest_cobertura
        }
    except Exception as e:
        print("Error en libertad_financiera:", e)
        return {"fechas": [], "pasivo": [], "indispensable": [], "cobertura": [], "latest_cobertura": 0.0}

@app.get("/top-ingresos")
def obtener_top_ingresos(
    fecha_inicio: str,
    fecha_fin: str,
    categoria: Optional[str] = None,
    limite: int = 10
):
    filtros = [
        "ingreso_gasto = 'Ingreso'",
        "date(txn_time) >= @fecha_inicio",
        "date(txn_time) <= @fecha_fin",
        "LOWER(categoria) NOT IN ('reembolsos', 'descuentos', 'dinero extra')"
    ]
    parametros = [
        bigquery.ScalarQueryParameter("fecha_inicio", "DATE", fecha_inicio),
        bigquery.ScalarQueryParameter("fecha_fin", "DATE", fecha_fin),
        bigquery.ScalarQueryParameter("limite", "INT64", limite),
    ]
    if categoria:
        filtros.append("categoria = @categoria")
        parametros.append(bigquery.ScalarQueryParameter("categoria", "STRING", categoria))

    where_clause = " AND ".join(filtros)

    query = """
        SELECT
            CAST(date(txn_time) AS STRING) AS fecha,
            categoria,
            concepto as descripcion,
            importe_moneda_principal as monto
        FROM `big-query-406221.finanzas_personales_mds.fact_transactions`
        WHERE """ + where_clause + """
        ORDER BY importe_moneda_principal DESC
        LIMIT @limite
    """
    try:
        resultados = _query(query, parametros)
        ingresos = []
        for fila in resultados:
            ingresos.append({
                "fecha": fila.fecha,
                "descripcion": fila.descripcion or "Sin concepto",
                "categoria": fila.categoria or "Sin categoría",
                "monto": fila.monto or 0.0
            })
        return {"ingresos": ingresos}
    except Exception as e:
        print("Error en top_ingresos:", e)
        return {"ingresos": []}

@app.get("/ingresos-categoria")
def obtener_ingresos_categoria(fecha_inicio: str, fecha_fin: str):
    query = """
        SELECT
            categoria,
            SUM(importe_moneda_principal) as monto
        FROM `big-query-406221.finanzas_personales_mds.fact_transactions`
        WHERE ingreso_gasto = 'Ingreso'
          AND date(txn_time) >= @fecha_inicio
          AND date(txn_time) <= @fecha_fin
          AND categoria NOT IN ('Reembolsos')
        GROUP BY categoria
        ORDER BY monto DESC
    """
    parametros = [
        bigquery.ScalarQueryParameter("fecha_inicio", "DATE", fecha_inicio),
        bigquery.ScalarQueryParameter("fecha_fin", "DATE", fecha_fin),
    ]
    try:
        resultados = _query(query, parametros)
        categorias = []
        montos = []
        for fila in resultados:
            categorias.append(fila.categoria)
            montos.append(fila.monto or 0.0)
        return {"categorias": categorias, "montos": montos}
    except Exception as e:
        print("Error en ingresos_categoria:", e)
        return {"categorias": [], "montos": []}

@app.get("/hipoteca-kpis")
def obtener_hipoteca_kpis():
    q_pagado = """
        SELECT SUM(cuota_mensual) AS pagado_en_cuotas
        FROM `big-query-406221.finanzas_personales_mds.hipotecas_materialized`
        WHERE pagado
    """
    q_amort = """
        SELECT SUM(amortizacion_capital) AS amortizaciones_capital
        FROM `big-query-406221.finanzas_personales_mds.hipotecas_materialized`
        WHERE pagado
    """
    q_colaterales = """
        SELECT SUM(importe_moneda_principal) AS gastos_colaterales
        FROM `big-query-406221.finanzas_personales_mds.fact_transactions`
        WHERE valor = 'Depa Alameda Dolores'
          AND ingreso_gasto = 'Gastos'
          AND concepto NOT IN ('Amortización', 'Cuota inicial', 'Cuota hipoteca')
    """
    
    pagado_en_cuotas = 0.0
    amortizaciones_capital = 0.0
    gastos_colaterales = 0.0
    
    try:
        res = list(client.query(q_pagado).result())
        if res and res[0].pagado_en_cuotas is not None:
            pagado_en_cuotas = float(res[0].pagado_en_cuotas)
    except Exception as e:
        print("Error en q_pagado:", e)
        
    try:
        res = list(client.query(q_amort).result())
        if res and res[0].amortizaciones_capital is not None:
            amortizaciones_capital = float(res[0].amortizaciones_capital)
    except Exception as e:
        print("Error en q_amort:", e)
        
    try:
        res = list(client.query(q_colaterales).result())
        if res and res[0].gastos_colaterales is not None:
            gastos_colaterales = float(res[0].gastos_colaterales)
    except Exception as e:
        print("Error en q_colaterales:", e)
        
    costo_total = pagado_en_cuotas + amortizaciones_capital + gastos_colaterales
    
    return {
        "pagado_en_cuotas": pagado_en_cuotas,
        "amortizaciones_capital": amortizaciones_capital,
        "gastos_colaterales": gastos_colaterales,
        "costo_total": costo_total
    }

@app.get("/hipoteca-distribucion")
def obtener_hipoteca_distribucion():
    query = """
        SELECT 
            CAST(fecha_vencimiento AS STRING) as fecha,
            COALESCE(capital_cuota, 0) as capital,
            COALESCE(interes_cuota, 0) as interes,
            COALESCE(seguro_propiedad, 0) + COALESCE(seguro_desgravamen, 0) as seguros,
            COALESCE(ingreso_alquiler, 0) as alquiler
        FROM `big-query-406221.finanzas_personales_mds.hipotecas_materialized`
        ORDER BY fecha_vencimiento
    """
    try:
        resultados = client.query(query).result()
        fechas = []
        capital = []
        interes = []
        seguros = []
        alquiler = []
        
        for fila in resultados:
            fecha_str = fila.fecha[:7] if fila.fecha else "Desconocido"
            fechas.append(fecha_str)
            capital.append(float(fila.capital or 0.0))
            interes.append(float(fila.interes or 0.0))
            seguros.append(float(fila.seguros or 0.0))
            alquiler.append(float(fila.alquiler or 0.0))
            
        return {
            "fechas": fechas,
            "capital": capital,
            "interes": interes,
            "seguros": seguros,
            "alquiler": alquiler
        }
    except Exception as e:
        print("Error en hipoteca_distribucion:", e)
        return {"fechas": [], "capital": [], "interes": [], "seguros": [], "alquiler": []}

@app.get("/hipoteca-equity")
def obtener_hipoteca_equity():
    query = """
        SELECT 
            pagado,
            SUM(capital_total) as capital
        FROM `big-query-406221.finanzas_personales_mds.hipotecas_materialized`
        GROUP BY pagado
    """
    try:
        resultados = client.query(query).result()
        pagado = 0.0
        pendiente = 0.0
        for fila in resultados:
            val = float(fila.capital or 0.0)
            if fila.pagado:
                pagado = val
            else:
                pendiente = val
        return {
            "pagado": pagado,
            "pendiente": pendiente,
            "total": pagado + pendiente
        }
    except Exception as e:
        print("Error en hipoteca_equity:", e)
        return {"pagado": 0.0, "pendiente": 0.0, "total": 0.0}

@app.get("/hipoteca-amortizaciones")
def obtener_hipoteca_amortizaciones():
    query = """
        SELECT 
            CAST(fecha_vencimiento AS STRING) as fecha,
            amortizacion_capital as monto
        FROM `big-query-406221.finanzas_personales_mds.hipotecas_materialized` 
        WHERE pagado AND amortizacion_capital > 0
        ORDER BY fecha_vencimiento
    """
    try:
        resultados = client.query(query).result()
        fechas = []
        montos = []
        for fila in resultados:
            fecha_str = fila.fecha[:7] if fila.fecha else "Desconocido"
            fechas.append(fecha_str)
            montos.append(float(fila.monto or 0.0))
        return {
            "fechas": fechas,
            "montos": montos
        }
    except Exception as e:
        print("Error en hipoteca_amortizaciones:", e)
        return {"fechas": [], "montos": []}

@app.get("/costo-vida-kpis")
def obtener_costo_vida_kpis(fecha_inicio: Optional[str] = None, fecha_fin: Optional[str] = None):
    filtros, parametros = _rango_fechas(fecha_inicio, fecha_fin)
    where_clause = ("WHERE " + " AND ".join(filtros)) if filtros else ""

    query = f"""
        SELECT
            categoria,
            COALESCE(subcategoria, 'no_subcat') AS subcategoria,
            SUM(costo_en_horas) AS total_horas
        FROM `big-query-406221.finanzas_personales_mds.agg_costo_en_vida`
        {where_clause}
        GROUP BY 1, 2
    """
    try:
        resultados = _query(query, parametros)
        kpis = {
            "comida": 0.0,
            "facturas": 0.0,
            "regalos": 0.0,
            "entretenimiento": 0.0,
            "transporte": 0.0,
            "salud": 0.0,
            "inversiones": 0.0,
            "mujeres": 0.0,
            "equipo_trabajo": 0.0
        }
        
        for fila in resultados:
            cat = fila.categoria or ""
            subcat = fila.subcategoria or "no_subcat"
            horas = float(fila.total_horas or 0.0)
            
            cat_lower = cat.lower()
            subcat_lower = subcat.lower()
            
            if subcat_lower == "mujeres":
                kpis["mujeres"] += horas
            else:
                if cat_lower == "comida":
                    kpis["comida"] += horas
                elif cat_lower == "facturas":
                    kpis["facturas"] += horas
                elif cat_lower == "regalos":
                    kpis["regalos"] += horas
                elif cat_lower == "entretenimiento":
                    kpis["entretenimiento"] += horas
                elif cat_lower == "transporte":
                    kpis["transporte"] += horas
                elif cat_lower == "salud":
                    kpis["salud"] += horas
                elif cat_lower == "inversiones":
                    kpis["inversiones"] += horas
                elif cat_lower == "equipo de trabajo":
                    kpis["equipo_trabajo"] += horas
                    
        return kpis
    except Exception as e:
        print("Error en costo-vida-kpis:", e)
        return {
            "comida": 0.0,
            "facturas": 0.0,
            "regalos": 0.0,
            "entretenimiento": 0.0,
            "transporte": 0.0,
            "salud": 0.0,
            "inversiones": 0.0,
            "mujeres": 0.0,
            "equipo_trabajo": 0.0
        }

@app.get("/costo-vida-detalles")
def obtener_costo_vida_detalles(
    categoria: str,
    fecha_inicio: Optional[str] = None,
    fecha_fin: Optional[str] = None
):
    filtros = []
    parametros = []
    # Se gestiona el filtro de categoria o subcategoria Mujeres
    if categoria == "Mujeres":
        filtros.append("subcategoria = 'Mujeres'")
    else:
        filtros.append("categoria = @categoria")
        parametros.append(bigquery.ScalarQueryParameter("categoria", "STRING", categoria))
        filtros.append("COALESCE(subcategoria, 'no_subcat') NOT IN ('Mujeres')")

    fecha_filtros, fecha_parametros = _rango_fechas(fecha_inicio, fecha_fin)
    filtros.extend(fecha_filtros)
    parametros.extend(fecha_parametros)

    where_clause = " AND ".join(filtros)
    if where_clause:
        where_clause = "WHERE " + where_clause

    query = f"""
        SELECT 
            CAST(DATE(txn_time) AS STRING) AS fecha,
            categoria,
            subcategoria,
            concepto,
            importe_moneda_principal,
            costo_en_horas,
            horas_trabajadas AS horas_trabajadas_mes,
            pago_por_hora AS pago_por_hora_mes
        FROM `big-query-406221.finanzas_personales_mds.agg_costo_en_vida`
        {where_clause}
        ORDER BY costo_en_horas DESC
    """
    try:
        resultados = _query(query, parametros)
        detalles = []
        for fila in resultados:
            detalles.append({
                "fecha": fila.fecha,
                "categoria": fila.categoria or "",
                "subcategoria": fila.subcategoria or "",
                "concepto": fila.concepto or "",
                "importe_moneda_principal": float(fila.importe_moneda_principal or 0.0),
                "costo_en_horas": float(fila.costo_en_horas or 0.0),
                "horas_trabajadas_mes": float(fila.horas_trabajadas_mes or 0.0),
                "pago_por_hora_mes": float(fila.pago_por_hora_mes or 0.0)
            })
        return detalles
    except Exception as e:
        print("Error en costo-vida-detalles:", e)
        return []

@app.get("/costo-vida-grafico")
def obtener_costo_vida_grafico(fecha_inicio: Optional[str] = None, fecha_fin: Optional[str] = None):
    filtros, parametros = _rango_fechas(fecha_inicio, fecha_fin)
    where_clause = ("AND " + " AND ".join(filtros)) if filtros else ""

    query = f"""
        SELECT 
            CAST(DATE_TRUNC(date(txn_time), MONTH) AS STRING) AS mes,
            categoria,
            SUM(costo_en_horas) AS costo_en_horas_mes
        FROM `big-query-406221.finanzas_personales_mds.agg_costo_en_vida`
        WHERE categoria IN ('Comida', 'Viajes', 'Regalos', 'Entretenimiento', 'Facturas', 'Salud', 'Transporte', 'Autocuidado', 'Inversiones', 'Equipo de trabajo')
        AND COALESCE(subcategoria, 'no_subcat') NOT IN ('Mujeres')
          {where_clause}
        GROUP BY 1, 2
        ORDER BY mes
    """
    try:
        resultados = _query(query, parametros)
        datos_map = {}
        meses_set = set()
        categorias_set = {'Comida', 'Viajes', 'Regalos', 'Entretenimiento', 'Facturas', 'Salud', 'Transporte', 'Autocuidado', 'Inversiones', 'Equipo de trabajo'}
        
        for fila in resultados:
            mes_str = fila.mes[:7] if fila.mes else "Desconocido"
            meses_set.add(mes_str)
            datos_map[(mes_str, fila.categoria)] = float(fila.costo_en_horas_mes or 0.0)
            
        meses_ordenados = sorted(list(meses_set))
        
        datos_por_categoria = {}
        for cat in categorias_set:
            datos_por_categoria[cat] = []
            for m in meses_ordenados:
                datos_por_categoria[cat].append(datos_map.get((m, cat), 0.0))
                
        return {
            "meses": meses_ordenados,
            "datos": datos_por_categoria
        }
    except Exception as e:
        print("Error en costo-vida-grafico:", e)
        return {"meses": [], "datos": {}}

@app.get("/estabilizadores")
def obtener_estabilizadores(fecha_inicio: Optional[str] = None, fecha_fin: Optional[str] = None):
    filtros = [
        "concepto IN ('Pan', 'Pancito', 'Cerveza', 'Whisky', 'Café', 'Café Instantáneo', 'Popcorn')",
        "categoria IN ('Comida', 'Entretenimiento', 'Regalos')"
    ]
    fecha_filtros, parametros = _rango_fechas(fecha_inicio, fecha_fin)
    filtros.extend(fecha_filtros)

    where_clause = "WHERE " + " AND ".join(filtros)

    query = f"""
        SELECT
          CAST(DATE_TRUNC(date(txn_time), MONTH) AS STRING) AS mes,
          CASE
            WHEN concepto = 'Café Instantáneo' THEN 'Café (Comida)'
            WHEN concepto = 'Café' AND categoria = 'Comida' THEN 'Café (Comida)'
            WHEN concepto = 'Café' AND categoria = 'Entretenimiento' THEN 'Café (Entretenimiento)'
            ELSE concepto
          END AS concepto,
          SUM(importe_moneda_principal) AS gasto_estabilizador
        FROM `big-query-406221.finanzas_personales_mds.fact_transactions`
        {where_clause}
        GROUP BY 1, 2
        ORDER BY mes
    """
    try:
        resultados = _query(query, parametros)
        datos_map = {}
        meses_set = set()
        conceptos_set = {'Pan', 'Pancito', 'Cerveza', 'Whisky', 'Café (Comida)', 'Café (Entretenimiento)', 'Popcorn'}
        
        for fila in resultados:
            mes_str = fila.mes[:7] if fila.mes else "Desconocido"
            meses_set.add(mes_str)
            datos_map[(mes_str, fila.concepto)] = float(fila.gasto_estabilizador or 0.0)
            
        meses_ordenados = sorted(list(meses_set))
        
        datos_por_concepto = {}
        for c in conceptos_set:
            datos_por_concepto[c] = []
            for m in meses_ordenados:
                datos_por_concepto[c].append(datos_map.get((m, c), 0.0))
                
        return {
            "meses": meses_ordenados,
            "datos": datos_por_concepto
        }
    except Exception as e:
        print("Error en obtener_estabilizadores:", e)
        return {"meses": [], "datos": {}}

@app.get("/social-kpis")
def obtener_social_kpis(fecha_inicio: Optional[str] = None, fecha_fin: Optional[str] = None):
    filtros_tx, parametros_tx = _rango_fechas(fecha_inicio, fecha_fin, campo="txn_time")
    filtros_tx = ["subcategoria = 'Mujeres'"] + filtros_tx
    filtros_soc, parametros_soc = _rango_fechas(fecha_inicio, fecha_fin, campo="fecha_trunc")

    where_tx = "WHERE " + " AND ".join(filtros_tx)
    where_soc = ("WHERE " + " AND ".join(filtros_soc)) if filtros_soc else ""

    q_gastos = f"""
        SELECT
            SUM(IF(ingreso_gasto = 'Gastos', importe_moneda_principal, 0)) AS bruto,
            SUM(IF(categoria = 'Reembolsos', importe_moneda_principal, 0)) AS reembolsos
        FROM `big-query-406221.finanzas_personales_mds.fact_transactions`
        {where_tx}
    """
    q_social = f"""
        SELECT
            SUM(gasto_mujeres) AS gm,
            SUM(ingreso_total) AS it
        FROM `big-query-406221.finanzas_personales_mds.agg_social`
        {where_soc}
    """

    gasto_bruto = 0.0
    reembolsos = 0.0
    pct_social = 0.0

    try:
        res_g = list(_query(q_gastos, parametros_tx))
        if res_g and res_g[0].bruto is not None:
            gasto_bruto = float(res_g[0].bruto)
        if res_g and res_g[0].reembolsos is not None:
            reembolsos = float(res_g[0].reembolsos)
    except Exception as e:
        print("Error en social-kpis gastos:", e)

    try:
        res_s = list(_query(q_social, parametros_soc))
        if res_s and res_s[0].gm is not None and res_s[0].it is not None and res_s[0].it > 0:
            pct_social = (float(res_s[0].gm) / float(res_s[0].it)) * 100.0
    except Exception as e:
        print("Error en social-kpis social %:", e)
        
    return {
        "gasto_bruto": gasto_bruto,
        "reembolsos": reembolsos,
        "gasto_neto": gasto_bruto - reembolsos,
        "porc_social": pct_social
    }

@app.get("/social-tendencia")
def obtener_social_tendencia(fecha_inicio: Optional[str] = None, fecha_fin: Optional[str] = None):
    filtros, parametros = _rango_fechas(fecha_inicio, fecha_fin, campo="fecha_trunc")
    where_clause = ("WHERE " + " AND ".join(filtros)) if filtros else ""
    query = f"""
        SELECT
          CAST(DATE(fecha_trunc) AS STRING) AS mes,
          gasto_mujeres
        FROM `big-query-406221.finanzas_personales_mds.agg_social`
        {where_clause}
        ORDER BY fecha_trunc
    """
    try:
        resultados = _query(query, parametros)
        meses = []
        gastos = []
        for fila in resultados:
            meses.append(fila.mes[:7] if fila.mes else "")
            gastos.append(float(fila.gasto_mujeres or 0.0))
        return {"meses": meses, "gastos": gastos}
    except Exception as e:
        print("Error en social-tendencia:", e)
        return {"meses": [], "gastos": []}

@app.get("/social-top-gastos")
def obtener_social_top_gastos(fecha_inicio: Optional[str] = None, fecha_fin: Optional[str] = None):
    filtros = [
        "categoria NOT IN ('Préstamos', 'Reembolsos')",
        "ingreso_gasto = 'Gastos'",
        "subcategoria = 'Mujeres'"
    ]
    fecha_filtros, parametros = _rango_fechas(fecha_inicio, fecha_fin)
    filtros.extend(fecha_filtros)

    where_clause = "WHERE " + " AND ".join(filtros)
    query = f"""
        SELECT
          CAST(DATE(txn_time) AS STRING) AS fecha,
          concepto,
          importe_moneda_principal,
          valor
        FROM `big-query-406221.finanzas_personales_mds.fact_transactions`
        {where_clause}
        ORDER BY importe_moneda_principal DESC
        LIMIT 100
    """
    try:
        resultados = _query(query, parametros)
        gastos = []
        for fila in resultados:
            gastos.append({
                "fecha": fila.fecha,
                "concepto": fila.concepto or "Sin concepto",
                "importe": float(fila.importe_moneda_principal or 0.0),
                "valor": fila.valor or ""
            })
        return gastos
    except Exception as e:
        print("Error en social-top-gastos:", e)
        return []

@app.get("/social-categoria")
def obtener_social_categoria(fecha_inicio: Optional[str] = None, fecha_fin: Optional[str] = None):
    filtros = [
        "subcategoria = 'Mujeres'",
        "categoria != 'Reembolsos'"
    ]
    fecha_filtros, parametros = _rango_fechas(fecha_inicio, fecha_fin)
    filtros.extend(fecha_filtros)

    where_clause = "WHERE " + " AND ".join(filtros)
    query = f"""
        SELECT
            categoria,
            SUM(IF(ingreso_gasto = 'Gastos', importe_moneda_principal, -importe_moneda_principal)) AS gasto_por_categoria
        FROM `big-query-406221.finanzas_personales_mds.fact_transactions`
        {where_clause}
        GROUP BY categoria
        ORDER BY gasto_por_categoria DESC
    """
    try:
        resultados = _query(query, parametros)
        categorias = []
        montos = []
        for fila in resultados:
            categorias.append(fila.categoria)
            montos.append(float(fila.gasto_por_categoria or 0.0))
        return {"categorias": categorias, "montos": montos}
    except Exception as e:
        print("Error en social-categoria:", e)
        return {"categorias": [], "montos": []}

@app.get("/social-compartido")
def obtener_social_compartido(fecha_inicio: Optional[str] = None, fecha_fin: Optional[str] = None):
    filtros = ["clave = 'C/'"]
    fecha_filtros, parametros = _rango_fechas(fecha_inicio, fecha_fin)
    filtros.extend(fecha_filtros)

    where_clause = " AND " + " AND ".join(filtros)

    query = f"""
        WITH precalculo AS (
            SELECT
                DATETIME_TRUNC(txn_time, MONTH) AS mes,
                valor,
                categoria,
                SUM(
                    IF(ingreso_gasto = 'Gastos', importe_moneda_principal, -importe_moneda_principal)
                ) OVER(PARTITION BY valor, DATETIME_TRUNC(txn_time, MONTH)) AS gasto_por_persona
            FROM `big-query-406221.finanzas_personales_mds.fact_transactions`
            WHERE 1=1 {where_clause}
        )
        SELECT DISTINCT
            CAST(mes AS STRING) as mes,
            valor,
            gasto_por_persona
        FROM precalculo
        WHERE categoria != 'Reembolsos'
        ORDER BY mes
    """
    try:
        resultados = _query(query, parametros)

        datos_map = {}
        meses_set = set()
        valores_set = set()
        
        for fila in resultados:
            mes_str = fila.mes[:7] if fila.mes else "Desconocido"
            meses_set.add(mes_str)
            val_str = fila.valor or "Sin asignar"
            valores_set.add(val_str)
            datos_map[(mes_str, val_str)] = float(fila.gasto_por_persona or 0.0)
            
        meses_ordenados = sorted(list(meses_set))
        valores_ordenados = sorted(list(valores_set))
        
        datos_por_valor = {}
        for v in valores_ordenados:
            datos_por_valor[v] = []
            for m in meses_ordenados:
                datos_por_valor[v].append(datos_map.get((m, v), 0.0))
                
        return {
            "meses": meses_ordenados,
            "valores": valores_ordenados,
            "datos": datos_por_valor
        }
    except Exception as e:
        print("Error en social-compartido:", e)
        return {"meses": [], "valores": [], "datos": {}}