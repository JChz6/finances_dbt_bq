{#
  Lista canónica de categorías "esenciales de supervivencia", compartida por
  agg_gasto_esencial (real y presupuesto) para que /crecimiento-kpis,
  /gasto-esencial-discrecional y /libertad-financiera dejen de tener 3 definiciones
  distintas (ver dashboard_api/docs/metricas.md, sección 0.3).

  Excluye deliberadamente 'Deudas'/'Deudas indispensables': esos nombres nunca capturan
  la cuota de hipoteca en fact_transactions (categorizada como 'Inversiones', ver 0.1), y
  el costo esencial real de la vivienda propia se agrega aparte desde
  hipotecas_materialized.costo_vida_interes_seguros, no por nombre de categoría.
  Comparar en mayúsculas para no depender de la capitalización exacta de cada fuente.
#}
{% macro categorias_esenciales_supervivencia() %}
['COMIDA', 'TRANSPORTE', 'FACTURAS', 'SALUD', 'GASTOS VARIABLES', 'SEGUROS']
{% endmacro %}
