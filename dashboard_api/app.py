import os
from typing import List, Optional
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from google.cloud import bigquery

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "creds/html-dashboarder-creds.json"
app = FastAPI(title="Wealth Dashboard API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

client = bigquery.Client()

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
def obtener_crecimiento_kpis():
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
        
    return {
        "fondo_emergencia": fondo_emergencia,
        "supervivencia_estricta": total_estricta,
        "supervivencia_vida": total_vida,
        "mediana_ingreso_neto": 0.0
    }

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