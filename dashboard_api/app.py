import os
from typing import List, Optional
from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse
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
    return "creds/html-dashboarder-creds.json"

creds_path = find_credentials()
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = creds_path
app = FastAPI(title="Wealth Dashboard API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

client = bigquery.Client()

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
    filtros = []
    if fecha_inicio:
        filtros.append(f"date(txn_time) >= '{fecha_inicio}'")
    if fecha_fin:
        filtros.append(f"date(txn_time) <= '{fecha_fin}'")
    
    where_clause = " AND ".join(filtros)
    if where_clause:
        where_clause = "WHERE " + where_clause

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

    resultados = client.query(query).result()
    
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
    query = f""" 
        SELECT 
            categoria, 
            SUM(importe_moneda_principal) as monto
        FROM `big-query-406221.finanzas_personales_mds.fact_transactions`
        WHERE ingreso_gasto = 'Gastos'
          AND date(txn_time) >= '{fecha_inicio}' 
          AND date(txn_time) <= '{fecha_fin}'
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
    resultados = client.query(query).result()
    
    categorias, montos = [], []
    for fila in resultados:
        categorias.append(fila.categoria)
        montos.append(fila.monto or 0.0)
        
    return {"categorias": categorias, "montos": montos}

@app.get("/balance-trimestre")
def obtener_trimestre(fecha_inicio: Optional[str] = None, fecha_fin: Optional[str] = None):
    filtros = []
    if fecha_inicio:
        filtros.append(f"date(txn_time) >= '{fecha_inicio}'")
    if fecha_fin:
        filtros.append(f"date(txn_time) <= '{fecha_fin}'")
    where_clause = " AND ".join(filtros)
    if where_clause:
        where_clause = "WHERE " + where_clause

    query = f"""
        SELECT 
            CAST(DATE_TRUNC(date(txn_time), QUARTER) AS STRING) AS trimestre,
            SUM(IF(LOWER(ingreso_gasto) LIKE '%ingr%', importe_moneda_principal, importe_moneda_principal * -1)) AS balance
        FROM `big-query-406221.finanzas_personales_mds.fact_transactions`
        {where_clause}
        GROUP BY 1
        ORDER BY trimestre
    """
    resultados = client.query(query).result()
    
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
        f"date(txn_time) >= '{fecha_inicio}'",
        f"date(txn_time) <= '{fecha_fin}'",
        "LOWER(concepto) NOT IN ('quinta categoría', 'aporte afp')"
    ]
    if categoria:
        filtros.append(f"categoria = '{categoria}'")
        
    where_clause = " AND ".join(filtros)
    
    query = f"""
        SELECT 
            CAST(date(txn_time) AS STRING) AS fecha, 
            categoria,
            concepto as descripcion,
            importe_moneda_principal as monto
        FROM `big-query-406221.finanzas_personales_mds.fact_transactions` 
        WHERE {where_clause}
        ORDER BY importe_moneda_principal DESC
        LIMIT {limite}
    """
    resultados = client.query(query).result()
    
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
    anio: Optional[int] = None,
    meses: Optional[List[int]] = Query(None),
    categorias: Optional[List[str]] = Query(None)
):
    filtros = []
    if anio:
        filtros.append(f"EXTRACT(YEAR FROM date(fecha)) = {anio}")
    if meses:
        meses_formatted = ", ".join([str(m) for m in meses])
        filtros.append(f"EXTRACT(MONTH FROM date(fecha)) IN ({meses_formatted})")
    if categorias:
        cats_formatted = ", ".join([f"'{c}'" for c in categorias])
        filtros.append(f"categoria IN ({cats_formatted})")
        
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
    resultados = client.query(query).result()
    
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
          AND categoria IN ('Comida', 'Transporte', 'Facturas', 'Deudas indispensables', 'Salud', 'Gastos Variables')
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
                  AND categoria IN ('Comida', 'Transporte', 'Facturas', 'Deudas indispensables', 'Salud', 'Gastos Variables')
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
    filtros = []
    if fecha_inicio:
        filtros.append(f"date(fecha) >= '{fecha_inicio}'")
    if fecha_fin:
        filtros.append(f"date(fecha) <= '{fecha_fin}'")
    where_clause = " AND ".join(filtros)
    if where_clause:
        where_clause = "WHERE " + where_clause
    else:
        where_clause = ""
        
    query_mediana = f"""
        SELECT DISTINCT PERCENTILE_CONT(ingreso_neto, 0.5) OVER() as mediana
        FROM `big-query-406221.finanzas_personales_mds.agg_ingresos`
        {where_clause}
    """
    mediana_ingreso_neto = 0.0
    try:
        res_mediana = list(client.query(query_mediana).result())
        if res_mediana and res_mediana[0].mediana is not None:
            mediana_ingreso_neto = float(res_mediana[0].mediana)
    except Exception as e:
        print("Error en query_mediana:", e)
        
    return {
        "fondo_emergencia": fondo_emergencia,
        "supervivencia_estricta": total_estricta,
        "supervivencia_vida": total_vida,
        "mediana_ingreso_neto": mediana_ingreso_neto
    }

@app.get("/ingreso-neto-mensual")
def obtener_ingreso_neto_mensual(fecha_inicio: Optional[str] = None, fecha_fin: Optional[str] = None):
    filtros = []
    if fecha_inicio:
        filtros.append(f"date(fecha) >= '{fecha_inicio}'")
    if fecha_fin:
        filtros.append(f"date(fecha) <= '{fecha_fin}'")
    
    where_clause = " AND ".join(filtros)
    if where_clause:
        where_clause = "WHERE " + where_clause
    else:
        where_clause = ""
        
    query = f"""
        SELECT CAST(DATE(fecha) AS STRING) AS fecha, ingreso_neto
        FROM `big-query-406221.finanzas_personales_mds.agg_ingresos`
        {where_clause}
        ORDER BY fecha
    """
    try:
        resultados = client.query(query).result()
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

@app.get("/top-ingresos")
def obtener_top_ingresos(
    fecha_inicio: str,
    fecha_fin: str,
    categoria: Optional[str] = None,
    limite: int = 10
):
    filtros = [
        "ingreso_gasto = 'Ingreso'",
        f"date(txn_time) >= '{fecha_inicio}'",
        f"date(txn_time) <= '{fecha_fin}'",
        "LOWER(categoria) NOT IN ('reembolsos', 'descuentos', 'dinero extra')"
    ]
    if categoria:
        filtros.append(f"categoria = '{categoria}'")
        
    where_clause = " AND ".join(filtros)
    
    query = f"""
        SELECT 
            CAST(date(txn_time) AS STRING) AS fecha, 
            categoria,
            concepto as descripcion,
            importe_moneda_principal as monto
        FROM `big-query-406221.finanzas_personales_mds.fact_transactions` 
        WHERE {where_clause}
        ORDER BY importe_moneda_principal DESC
        LIMIT {limite}
    """
    try:
        resultados = client.query(query).result()
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
    query = f"""
        SELECT 
            categoria, 
            SUM(importe_moneda_principal) as monto
        FROM `big-query-406221.finanzas_personales_mds.fact_transactions`
        WHERE ingreso_gasto = 'Ingreso'
          AND date(txn_time) >= '{fecha_inicio}' 
          AND date(txn_time) <= '{fecha_fin}'
          AND categoria NOT IN ('Reembolsos')
        GROUP BY categoria
        ORDER BY monto DESC
    """
    try:
        resultados = client.query(query).result()
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
    filtros = []
    if fecha_inicio:
        filtros.append(f"date(txn_time) >= '{fecha_inicio}'")
    if fecha_fin:
        filtros.append(f"date(txn_time) <= '{fecha_fin}'")
    
    where_clause = " AND ".join(filtros)
    if where_clause:
        where_clause = "WHERE " + where_clause
    else:
        where_clause = ""
        
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
        resultados = client.query(query).result()
        kpis = {
            "comida": 0.0,
            "facturas": 0.0,
            "regalos": 0.0,
            "entretenimiento": 0.0,
            "transporte": 0.0,
            "salud": 0.0,
            "inversiones": 0.0,
            "mujeres": 0.0
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
            "mujeres": 0.0
        }

@app.get("/costo-vida-detalles")
def obtener_costo_vida_detalles(
    categoria: str,
    fecha_inicio: Optional[str] = None,
    fecha_fin: Optional[str] = None
):
    filtros = []
    # Se gestiona el filtro de categoria o subcategoria Mujeres
    if categoria == "Mujeres":
        filtros.append("subcategoria = 'Mujeres'")
    else:
        filtros.append(f"categoria = '{categoria}'")
        filtros.append("COALESCE(subcategoria, 'no_subcat') NOT IN ('Mujeres')")
        
    if fecha_inicio:
        filtros.append(f"date(txn_time) >= '{fecha_inicio}'")
    if fecha_fin:
        filtros.append(f"date(txn_time) <= '{fecha_fin}'")
        
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
        resultados = client.query(query).result()
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
    filtros = []
    if fecha_inicio:
        filtros.append(f"date(txn_time) >= '{fecha_inicio}'")
    if fecha_fin:
        filtros.append(f"date(txn_time) <= '{fecha_fin}'")
    
    where_clause = " AND ".join(filtros)
    if where_clause:
        where_clause = "AND " + where_clause
    else:
        where_clause = ""
        
    query = f"""
        SELECT 
            CAST(DATE_TRUNC(date(txn_time), MONTH) AS STRING) AS mes,
            categoria,
            SUM(costo_en_horas) AS costo_en_horas_mes
        FROM `big-query-406221.finanzas_personales_mds.agg_costo_en_vida`
        WHERE categoria IN ('Comida', 'Viajes', 'Regalos', 'Entretenimiento', 'Facturas', 'Salud', 'Transporte', 'Autocuidado', 'Inversiones')
        AND COALESCE(subcategoria, 'no_subcat') NOT IN ('Mujeres')
          {where_clause}
        GROUP BY 1, 2
        ORDER BY mes
    """
    try:
        resultados = client.query(query).result()
        datos_map = {}
        meses_set = set()
        categorias_set = {'Comida', 'Viajes', 'Regalos', 'Entretenimiento', 'Facturas', 'Salud', 'Transporte', 'Autocuidado', 'Inversiones'}
        
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
    if fecha_inicio:
        filtros.append(f"date(txn_time) >= '{fecha_inicio}'")
    if fecha_fin:
        filtros.append(f"date(txn_time) <= '{fecha_fin}'")
        
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
        resultados = client.query(query).result()
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
    filtros_tx = ["subcategoria = 'Mujeres'"]
    filtros_soc = []
    if fecha_inicio:
        filtros_tx.append(f"date(txn_time) >= '{fecha_inicio}'")
        filtros_soc.append(f"date(fecha_trunc) >= '{fecha_inicio}'")
    if fecha_fin:
        filtros_tx.append(f"date(txn_time) <= '{fecha_fin}'")
        filtros_soc.append(f"date(fecha_trunc) <= '{fecha_fin}'")
        
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
        res_g = list(client.query(q_gastos).result())
        if res_g and res_g[0].bruto is not None:
            gasto_bruto = float(res_g[0].bruto)
        if res_g and res_g[0].reembolsos is not None:
            reembolsos = float(res_g[0].reembolsos)
    except Exception as e:
        print("Error en social-kpis gastos:", e)
        
    try:
        res_s = list(client.query(q_social).result())
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
    filtros = []
    if fecha_inicio:
        filtros.append(f"date(fecha_trunc) >= '{fecha_inicio}'")
    if fecha_fin:
        filtros.append(f"date(fecha_trunc) <= '{fecha_fin}'")
        
    where_clause = "WHERE " + " AND ".join(filtros) if filtros else ""
    query = f"""
        SELECT
          CAST(DATE(fecha_trunc) AS STRING) AS mes,
          gasto_mujeres
        FROM `big-query-406221.finanzas_personales_mds.agg_social`
        {where_clause}
        ORDER BY fecha_trunc
    """
    try:
        resultados = client.query(query).result()
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
    if fecha_inicio:
        filtros.append(f"date(txn_time) >= '{fecha_inicio}'")
    if fecha_fin:
        filtros.append(f"date(txn_time) <= '{fecha_fin}'")
        
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
        resultados = client.query(query).result()
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
    if fecha_inicio:
        filtros.append(f"date(txn_time) >= '{fecha_inicio}'")
    if fecha_fin:
        filtros.append(f"date(txn_time) <= '{fecha_fin}'")
        
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
        resultados = client.query(query).result()
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
    if fecha_inicio:
        filtros.append(f"date(txn_time) >= '{fecha_inicio}'")
    if fecha_fin:
        filtros.append(f"date(txn_time) <= '{fecha_fin}'")
        
    where_clause = " AND " + " AND ".join(filtros) if filtros else ""
    
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
        resultados = client.query(query).result()
        
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