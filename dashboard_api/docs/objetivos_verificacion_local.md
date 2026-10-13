# Verificación local: Objetivos y decisiones

Fecha: 2026-10-08 (America/Lima). Estado: implementación e integración local con
BigQuery real verificadas; exportación de capturas a archivo local pendiente;
envío al servidor **no realizado**.

## Cambios y seguimiento

La última pestaña «Fase 2» se reemplazó por «Objetivos y decisiones», manteniendo el
identificador `fase2` para navegación. La API nueva `/objetivos-decisiones` lee fondo,
presupuesto, hipoteca, renta candidata y posiciones; devuelve `null`/motivo ante falta
de datos. `objectives.py` concentra hitos y condiciones. `objectives_config.yaml` es una
plantilla vacía. Los datos privados y las decisiones explícitas se completan en
`objectives_data.yaml` (ignorado por Git). Se documentaron fórmulas en `metricas.md`.

Seguimiento respecto a `financial_advisor/tasks/todo.md`:

- T1 reserva/hipoteca: lógica, UI y contraste con muestra real realizados.
- T2 renta/historia: lógica y UI realizadas; neto conciliado sigue pendiente como dato
  financiero, por lo que la pantalla indica provisional y no habilita decisiones.
- T3 decisiones: condiciones, estados y registro persistente manual implementados; no
  hay IR conciliado, oferta ni tolerancia aprobada en la configuración inicial.
- T4 pruebas: unitarias, navegador vacío y real, y conciliación de muestra realizados;
  capturas vistas en sesión, pero archivo de captura local pendiente. No se marca completo.
- T5 entrega remota: bloqueada por el criterio local del encargo.

## Comandos y resultados

- `./venv/bin/python -m unittest dashboard_api.test_objectives dashboard_api.test_api_objectives -v`
  desde la raíz del monorepo: **9 pruebas OK**. Cubren seis meses completos, cero frente
  a desconocido, caída tras hito alcanzado, reserva y cuota una vez, seguros, umbrales,
  IR cero verificado, asignación separada, oferta ausente, cobertura y estrés.
  Incluye además saldo S/0.01 bajo la meta: aunque la cobertura visible redondee a
  6.00 meses, el checkpoint compara soles exactos y permanece en `faltan condiciones`.
- `./venv/bin/python -m py_compile dashboard_api/app.py dashboard_api/objectives.py`:
  **OK**.
- `node --check /private/tmp/dashboard_inline.js` (script extraído de `index.html`):
  **OK**.
- `git diff --check`: **OK**.
- `./venv/bin/uvicorn dashboard_api.app:app --host 127.0.0.1 --port 8000` inicialmente
  inició, pero la identidad ADC previa dio `403 Access Denied` sobre las tablas. Tras
  `gcloud auth application-default login` iniciado con autorización y
  `GOOGLE_CLOUD_PROJECT=big-query-406221`, el SELECT local funcionó. La variable solo
  se aplicó al proceso de prueba, sin cambiar el servicio ni la configuración remota.
- `GOOGLE_CLOUD_PROJECT=big-query-406221 ./venv/bin/uvicorn dashboard_api.app:app
  --host 127.0.0.1 --port 8000`: **API local real OK**. `/objetivos-decisiones`
  respondió HTTP 200, 4,940 bytes en 8.9 s, y los cuatro checkpoints quedaron en
  `faltan datos` por IR, oferta, tolerancia e ingreso futuro aún pendientes.
- Instancia local con `_query` simulado vacío en `http://127.0.0.1:8001/`:
  la ruta respondió HTTP 200 y se probó en Chrome escritorio y viewport 390×844.
  Última pestaña, recarga, menú móvil, tooltip de reserva y navegación hacia
  Crecimiento e Inversiones, Hipoteca y Patrimonio visibles. Consola sin errores de
  JavaScript en la nueva pestaña; advertencia previa de `cdn.tailwindcss.com`.
  En recargas y selecciones observadas hubo una petición `/objetivos-decisiones` por
  selección, sin bucle. La pantalla mostró `Pendiente` para datos vacíos.
- Chrome con datos reales en `http://127.0.0.1:8000/`: escritorio y 390×844,
  última pestaña, recarga, tooltip, menú móvil y navegación a Crecimiento (fondo
  S/21,441 redondeado en su tarjeta existente), Hipoteca (pagado en cuotas S/26,147)
  y Patrimonio (equity S/71,036). Sin errores de consola observados.

## Revisión visual de la pestaña (2026-10-08)

Se sustituyeron los párrafos por una barra de reserva (97.2% con saldo, meta y
faltante), escalera de reducción de cuota, barras para los seis meses, pistas de
los seis hitos de renta y condiciones individuales con estado, actual, umbral,
diferencia y fecha. Se mantuvieron Outfit, las superficies oscuras y el verde del
dashboard. Las barras de renta candidata usan rayado y etiqueta «provisional»;
no se presentan como renta neta verificada ni como hitos alcanzados.

En Chrome local con BigQuery real se revisó el render, el tooltip de fórmula y la
consola sin errores JavaScript. En 320, 768, 1024 y 1440 px no hubo desbordamiento
horizontal de la pestaña. El estado de carga usa esqueletos; con `_query` vacío se
comprobaron «Pendiente», barras sin dato y motivos, sin tratarlos como cero. Con el
servidor simulado detenido se comprobó el aviso de error de red y los paneles sin
datos. Tras el ajuste visual, `node --check`, `git diff --check` y las 9 pruebas
unitarias/API continuaron pasando; `/objetivos-decisiones` local real respondió 200.
Las capturas se inspeccionaron en el navegador, pero no se exportaron a archivos;
T4 y el envío remoto siguen pendientes por ese requisito del encargo.

## Ajuste visual de hitos (2026-10-10)

La ruta de hipoteca muestra seis umbrales de capital e interés: S/1,200, S/1,000,
S/800, S/600, S/400 y S/200. Los umbrales cumplidos se calculan con la próxima
cuota al corte; el progreso entre nodos es solo una guía visual y no crea fechas
históricas. La deuda cero conserva su condición independiente. Renta pasiva presenta
un roadmap vertical con el punto actual y seis hitos, en vez de seis barras por meta.
Los importes candidatos siguen rotulados como provisionales y no acreditan hitos.
Seguridad y las secciones posteriores se dejaron intactas.

Con un fixture local en `127.0.0.1:8002` se revisó el render en escritorio y en
viewport móvil de 320×800. La ruta se distribuye horizontalmente en escritorio y
verticalmente en móvil, sin desbordamiento observado; el roadmap y la gráfica de
seis meses se leyeron completos en móvil. Las 9 pruebas unitarias/API, `node --check`
y `git diff --check` pasaron. En esta nueva sesión la identidad ADC recibe
`403 bigquery.jobs.create`; por ello la comprobación visual con cifras fue sintética,
no una nueva validación contra BigQuery real. No hubo envío al servidor.

## Conciliación de lectura al corte

Consulta independiente de filas fuente (script temporal `/private/tmp/verify_obj_sources.py`):

- `agg_fondo_emergencia`: última fila 12/09, saldo S/21,440.60; la meta S/30,000
  del pipeline no se usó como saldo.
- `presupuesto_materialized` 01/10: total S/3,676.87, deuda presupuestada S/756.87,
  presupuesto sin esa deuda S/2,920. API: S/2,920 + cuota prevista S/756.87 =
  S/3,676.87; meta seis meses S/22,061.22; faltante S/620.62; 5.83 meses.
- `hipotecas_materialized`: 16/09 `pagado=true`, `saldo_final` S/72,841.92;
  16/10 `pagado=false`, capital S/245.74 + interés S/468.10 + seguros S/43.03
  = cuota S/756.87. La UI distingue fecha prevista de saldo pagado.
- El antiguo concepto “Extra Trii” ya está reclasificado en la tabla actual como
  `Pasivo` / `FIBPRIME dividendos` el 29/08, S/98.52. La condición OR del SQL lo
  suma una sola vez como candidato bruto; no se editó la transacción. Promedio
  candidato abril–septiembre S/193.72, rango S/144.34–S/290.95. Sigue
  **provisional**, sin inferir neto ni recurrencia.

Las capturas de escritorio y móvil con cifras reales se observaron y adjuntaron en
la sesión de navegador del agente. La herramienta de navegador no ofreció guardarlas
como archivos locales; no se generó un JPG/PNG local. El criterio de evidencia de
archivo del encargo sigue pendiente.
La apertura de Preview mediante la herramienta de UI agotó el tiempo de espera. Un
intento posterior de abrir una página `data:` auxiliar para exportar la captura fue
rechazado por la revisión automática del navegador: solo admite protocolos `http:` y
`https:` y prohíbe usar una vía alternativa para ese resultado. No se continuó por
esa ruta.

## Datos pendientes y condición de envío

Para habilitar decisiones concretas se requieren, por separado, IR estimado,
retenciones válidas, reserva fiscal efectivamente separada, caja acreditada y
compromisos; netos mensuales conciliados; oferta y costos verificables; importe,
horizonte y tolerancia global; ingreso laboral futuro. La ausencia de estos datos no
impide construir la pantalla, pero mantiene checkpoints sin habilitar. Antes de enviar
al servidor falta conservar capturas como archivos locales y cerrar ese requisito de T4.

No se ha copiado código al servidor ni reiniciado systemd. El plan de reversión, una vez
autorizado un despliegue posterior, es restaurar la versión anterior de `app.py`,
`index.html`, `docs/metricas.md` y `.gitignore`, retirar `objectives.py` y la plantilla
YAML de esa versión, reiniciar el servicio `dashboard-api` tras confirmar su nombre
instalado y comprobar `/` y los endpoints anteriores. El archivo privado
`objectives_data.yaml` se preserva aparte.
