{#
  Lista canónica de cuentas de ahorro de alto rendimiento (Caja Municipal / CD y similares),
  compartida entre todos los modelos dbt que necesitan identificar transferencias hacia/desde
  esas cuentas (tracking_inversiones, y cualquier modelo futuro).

  También es la fuente de la vista `cuentas_alto_rendimiento` (ver
  models/gold/cuentas_alto_rendimiento.sql), que expone esta misma lista como tabla real en
  BigQuery para que dashboard_api la consulte por SELECT en vez de hardcodear el literal en
  Python — evita el desfase que ya pasó una vez: se agregó 'Global66 - USD' en app.py en varios
  lugares, pero tracking_inversiones.sql se quedó con la lista vieja porque vivía hardcodeada
  aparte, en otro archivo, en otro lenguaje.

  Único lugar a tocar cuando se abra o cierre una cuenta de alto rendimiento.
#}
{% macro cuentas_alto_rendimiento() %}
['Wow Compartamos', 'Pichincha', 'GNB', 'Global66 - USD']
{% endmacro %}
