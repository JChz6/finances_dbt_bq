{#
  Meta (monto objetivo) del fondo de emergencia. Constante única compartida entre
  agg_fondo_emergencia.sql y la vista que lo expone a dashboard_api (evita el mismo desfase que
  ya pasó con cuentas_alto_rendimiento: dos copias hardcodeadas, una en dbt y otra en app.py, que
  se desincronizan con el tiempo).

  Único lugar a tocar si el monto objetivo del fondo cambia.
#}
{%- macro fondo_emergencia_meta() -%}
30000
{%- endmacro -%}
