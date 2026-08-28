# Métricas y KPIs de `dashboard_api`

Referencia de todas las métricas financieras que expone `dashboard_api/app.py`, pensada para
que un agente (p. ej. `financial_advisor/`) pueda razonar sobre los números sin releer el SQL
cada vez. No cubre endpoints que solo devuelven listados crudos de transacciones sin agregación
de negocio (esos ya se explican solos).

**Cómo leer "fuente de datos" en cada métrica**: `finances_bq` distingue entre lo que se
**planeó gastar** (`presupuesto` / `presupuesto_materialized`, cargado a mano por el usuario)
y lo que **realmente se gastó** (`fact_transactions` y sus derivados `agg_costo_en_vida`,
`agg_ingresos`, `agg_social`, `hipotecas_materialized`). Confundir ambas fuentes invalida
cualquier lectura de "cumplimiento" — se marca explícitamente en cada métrica.

Todo lo marcado como "confirmado en BigQuery" en este documento se verificó ejecutando queries
de solo lectura contra `big-query-406221.finanzas_personales_mds` el **2026-08-22**, con la
service account `finances-dbt-bq@...` (no la que usa `dashboard_api` en producción, pero con
acceso de lectura a las mismas tablas gold/silver).

---

## 0. Problemas transversales (afectan a varios endpoints — leer antes que las secciones)

### 0.1 Bug confirmado: la cuota de hipoteca no se categoriza como "Deudas"

En `fact_transactions`, todas las transacciones de la cuota de "Depa Alameda Dolores"
(`valor = 'Depa Alameda Dolores'`) están bajo `categoria = 'Inversiones'`, con o sin
`subcategoria = 'Inmuebles'` (concepto `Cuota hipoteca`, `Amortización`, `Cuota inicial`,
`Intereses y seguros amortización`, etc.). **Nunca** aparecen con `categoria = 'Deudas'`.

Confirmado en BigQuery (2026-08-22): `categoria = 'Deudas'` en `fact_transactions` solo tiene
23 transacciones históricas correspondientes a préstamos personales ("Floresta", "Henry"),
cero relación con la hipoteca. Y `categoria = 'Deudas indispensables'` **no existe como valor
real en `fact_transactions`** — es un nombre que solo vive en `presupuesto_materialized`
(10 filas, desde 2025-11-01).

**Efecto concreto**: cualquier métrica que calcule "gasto real en Deudas/Deudas indispensables"
filtrando `fact_transactions` por esas categorías va a devolver 0 o subestimar, aunque la
hipoteca sea el pasivo más grande del usuario. En particular:

- `/cumplimiento-presupuesto` para `categoria = 'Deudas indispensables'` siempre muestra
  `gasto_acumulado_mes = 0` y `cumplimiento = 'CUMPLE'` — no porque no se pague la cuota, sino
  porque ningún registro real usa ese nombre de categoría. **El "cumplimiento" de esa fila del
  presupuesto está roto por diseño de datos, no informa nada real.**
- `/crecimiento-kpis` → `deuda_indispensable_mensual` y `ratio_deuda_ingreso` **no están
  afectados por este bug específico** porque leen el presupuesto (lo planeado), no el gasto
  real — pero eso significa que ese ratio DTI es "cuota planeada / ingreso", no "cuota
  efectivamente pagada / ingreso". Ver caveat propio en la sección de hipoteca/crecimiento.
- `/gasto-esencial-discrecional` y `/libertad-financiera` incluyen `'Deudas'` o
  `'Deudas indispensables'` en sus listas de categorías "esenciales" (ver 0.3) — como esos
  nombres no capturan la hipoteca, el gasto hipotecario que sí es real consumo (interés +
  seguros) queda fuera de "esencial" en esos dos endpoints porque además excluyen
  `'Inversiones'` por completo (ver 0.3).
- La hipoteca sí se mide correctamente en los endpoints dedicados `/hipoteca-*` y en
  `/net-worth`, que leen `hipotecas_materialized` (tabla separada, con capital/interés/seguros
  desglosados por cuota) en vez de depender de la categorización en `fact_transactions`.

### 0.2 Mismatch 'Seguro' (presupuesto) vs 'Seguros' (código) — **sigue sin corregirse**

Confirmado en BigQuery (2026-08-22): `presupuesto_materialized.categoria` usa `'Seguro'`
(singular; 10 filas, desde 2025-11-01 hasta el mes más reciente cargado). El código de
`app.py` filtra `'Seguros'` (plural) en:

- `/crecimiento-kpis` → `query_estricta` (línea ~370) y `query_vida` no filtra por categoría
  individual pero sí `query_estricta`, además del fallback sobre `agg_cumplimiento_presupuesto`.
- `/crecimiento-kpis` y `/gasto-esencial-discrecional` → `categorias_esenciales_sql`.

Como `'Seguros'` (plural) nunca existe en `presupuesto_materialized`, el presupuesto de
seguros del mes **queda excluido de `supervivencia_estricta`** (el "gasto mínimo de
supervivencia" en `/crecimiento-kpis`) — ese KPI está subestimado en el monto mensual del
seguro de la hipoteca (seguro de propiedad + desgravamen, vía `presupuesto_personal`/`Seguro`
en el presupuesto). No es un problema simétrico: en `fact_transactions` (gasto real) la
categoría **sí** se llama `'Seguros'` (plural, confirmado — 1 fila histórica, monto menor), así
que el filtro de `/gasto-esencial-discrecional` sobre transacciones reales es literalmente
correcto contra esa tabla; el bug vive específicamente en el cruce con `presupuesto_materialized`.

### 0.3 Tres definiciones distintas de "gasto esencial/indispensable" — no producen el mismo número

Diff sistemático de las listas de categorías "esenciales" hardcodeadas en `app.py`:

| Endpoint | Tabla que filtra | Lista de categorías "esenciales" |
|---|---|---|
| `/crecimiento-kpis` (`supervivencia_estricta`) | `presupuesto_materialized` | `Comida, Transporte, Facturas, Deudas indispensables, Salud, Gastos Variables, Seguros` |
| `/gasto-esencial-discrecional` | `fact_transactions` | mismas 7 categorías que arriba |
| `/libertad-financiera` (`indispensable`) | `fact_transactions` | `Comida, Transporte, Facturas, Deudas, Salud, Gastos Variables` (6 categorías — sin `Seguros`, y usa `Deudas` genérico en vez de `Deudas indispensables`) |

Ningún par de estas tres listas es idéntico. Un asesor que compare "gasto esencial" entre
`/crecimiento-kpis`, `/gasto-esencial-discrecional` y `/libertad-financiera` para el mismo mes
va a ver tres cifras distintas por diseño, no por error de cálculo — son definiciones
divergentes que nunca se unificaron. Además, como ya vimos en 0.1, `Deudas` y
`Deudas indispensables` no capturan la hipoteca en ninguna de las tres, así que "esencial" en
los tres casos subestima el verdadero costo de vida indispensable en el monto de intereses +
seguros de la hipoteca.

### 0.4 Inconsistencia confirmada: cuentas de alto rendimiento en `/crecimiento-kpis` vs `/net-worth`

Ambos endpoints suman movimientos en las cuentas `Wow Compartamos`, `Pichincha`, `GNB`, pero con
lógicas distintas:

- `/net-worth` (`q_cuentas_mensual`): `ingreso_gasto IN ('Ingreso', 'Dinero ingresado')` suma,
  `IN ('Gastos', 'Dinero gastado')` resta.
- `/crecimiento-kpis` (`query_construccion_patrimonio`): solo contempla
  `'Dinero ingresado'`/`'Dinero gastado'`; las filas con `ingreso_gasto = 'Ingreso'` o
  `'Gastos'` en esas cuentas caen al `ELSE 0` y no se cuentan.

Confirmado en BigQuery (2026-08-22): existen filas reales con `ingreso_gasto = 'Ingreso'` en
esas cuentas (GNB: 7, Pichincha: 26, Wow Compartamos: 5 — mayormente interés/rendimiento
acreditado) y 2 filas `Pichincha`/`Gastos`. Esas filas sí las ve `/net-worth` (quedan dentro de
`cuentas_alto_rendimiento`/equity) pero **no** las ve `/crecimiento-kpis` — ese interés
acreditado cuenta como ingreso total (`ingresos_periodo`) pero no como
`construccion_patrimonio_periodo`, así que termina empujando `tasa_ahorro_caja` hacia arriba en
vez de `tasa_construccion_patrimonio`, subestimando ligeramente esta última frente a lo que
`/net-worth` implica que realmente pasó con el patrimonio.

---

## 1. Flujo de caja

Fuente de datos: **real** (`fact_transactions`) en todos los endpoints de esta sección.

### `/flujo-caja`
Serie temporal (diaria o mensual) de ingresos, gastos y balance de caja.

- `ingresos`: suma de `importe_moneda_principal` donde `ingreso_gasto = 'Ingreso'`.
- `gastos`: suma de `importe_moneda_principal` donde `ingreso_gasto = 'Gastos'`, invertida a
  positivo (`* -1` en el SQL, ya que en la tabla los gastos se guardan en negativo).
- `balance`: ingresos − gastos del periodo (día o mes).
- `acumulado`: suma corriente (`SUM() OVER`) de `balance` ordenado por fecha — es la base del
  "efectivo acumulado" que después reaparece en `/net-worth` como `efectivo`.

Sin exclusiones de categoría: incluye TODO lo que pase por `fact_transactions`, incluida la
hipoteca completa (capital+interés+seguros) como gasto y cualquier transferencia a cuentas de
alto rendimiento marcada como `'Ingreso'`/`'Gastos'` (no `'Dinero ingresado'`/`'Dinero
gastado'`). Es la vista "cruda" de caja, no un KPI de consumo — para eso ver
`/gasto-esencial-discrecional` y `/crecimiento-kpis`.

### `/balance-trimestre`
Igual que `/flujo-caja` pero agregado por trimestre, usando `LOWER(ingreso_gasto) LIKE
'%ingr%'` en vez de comparar contra `'Ingreso'` exacto (matchea también `'Dinero ingresado'` —
inconsistente con el resto del código, que sí distingue ambos casos en otros endpoints).

### `/gastos-categoria` y `/ingresos-categoria`
Suma de `importe_moneda_principal` por `categoria` en un rango de fechas.

- `/gastos-categoria` excluye `concepto IN ('Cambio dólares', 'liquidación', 'Sin concepto')` y
  restringe a una whitelist fija de 19 categorías (incluye `Deudas` e `Inversiones` como
  categorías separadas — ver 0.1: la hipoteca cae dentro de `Inversiones` aquí, no de
  `Deudas`).
- `/ingresos-categoria` excluye `categoria = 'Reembolsos'` (para no inflar ingresos con
  devoluciones de gasto).

### `/top-gastos` y `/top-ingresos`
Listado de las N transacciones más grandes en el rango, con exclusiones puntuales:
`/top-gastos` excluye `concepto IN ('quinta categoría', 'aporte afp')` (retenciones, no gasto
de consumo); `/top-ingresos` excluye `categoria IN ('reembolsos', 'descuentos', 'dinero
extra')`. Filtro opcional por `categoria` exacta.

---

## 2. Presupuesto

### `/cumplimiento-presupuesto`
Fuente de datos: **híbrida** — lee directo de la tabla gold
`agg_cumplimiento_presupuesto`, que ya cruza presupuesto (planeado) contra gasto real
(`fact_transactions`, `ingreso_gasto = 'Gastos'`) por mes y categoría (`FULL OUTER JOIN` en
`UPPER(categoria)` + mes truncado, así que el cruce **sí** es insensible a mayúsculas/minúsculas
en el nombre de categoría — el mismatch 'Gastos Variables'/'Gastos variables' no rompe este
join en particular, aunque sí puede romper otros filtros hardcodeados de `app.py`, ver 0.2).

Campos:
- `gasto_acumulado_mes`: suma corriente del gasto real del mes para esa categoría
  (`SUM() OVER` particionado por mes+categoría).
- `presupuesto`: monto planeado para esa categoría/mes (de `presupuesto_materialized`).
- `utilizado`: `gasto_acumulado_mes / presupuesto * 100`, como string con `%`.
- `presupuesto_disponible`: `presupuesto - gasto_acumulado_mes`.
- `cumplimiento`: `'CUMPLE'` si `presupuesto >= gasto_acumulado_mes`, `'EXCESO'` si no, `NULL`
  si no hay presupuesto para esa categoría/mes (categoría existe en transacciones pero nunca se
  presupuestó, o viceversa).

**Caveat crítico (ver 0.1)**: para `categoria = 'Deudas indispensables'`, `gasto_acumulado_mes`
será siempre 0 porque ningún registro de `fact_transactions` usa ese nombre — el
`'CUMPLE'` que se ve para esa fila no refleja que la hipoteca se esté pagando puntualmente,
refleja que el nombre de categoría no tiene contraparte en las transacciones reales.

Último ajuste conocido: commit `c1d8ba8` ("Corrige presupuestos de agosto no cargados") y
`786f7eb` ("Cambio en CTE para cumplimiento de presupuesto") — cambios recientes en el modelo
`agg_cumplimiento_presupuesto.sql`, no en `app.py`.

---

## 3. Hipoteca

Todos estos endpoints leen `hipotecas_materialized` (tabla silver con el cronograma de cuotas
de "Depa Alameda Dolores", una fila por cuota, con `capital_cuota`/`interes_cuota`/
`seguro_propiedad`/`seguro_desgravamen`/`pagado` ya desglosados) — **no** dependen de la
categorización de `fact_transactions`, así que **no** están afectados por el bug de 0.1.

### `/hipoteca-kpis`
- `pagado_en_cuotas`: suma de `cuota_mensual` de las cuotas ya pagadas (`pagado = true`) — el
  total nominal pagado (capital+interés+seguros) vía cronograma normal.
- `amortizaciones_capital`: suma de `amortizacion_capital` de cuotas pagadas — abonos extra a
  capital que ya redujeron el cronograma (no confundir con `capital_cuota`, que es el capital
  incluido en la cuota regular).
- `gastos_colaterales`: gasto real en `fact_transactions` con `valor = 'Depa Alameda Dolores'`,
  excluyendo `concepto IN ('Amortización', 'Cuota inicial', 'Cuota hipoteca')` — o sea, todo lo
  que NO es la cuota/amortización en sí (mudanza, muebles, mantenimiento, notaría, tasación,
  etc.), para no duplicar contra `pagado_en_cuotas`/`amortizaciones_capital`.
- `costo_total`: suma de los tres anteriores — costo total de vida del inmueble hasta hoy,
  cuotas + amortización voluntaria + gasto colateral de adquisición/mantenimiento.

### `/hipoteca-distribucion`
Serie mensual capital vs interés vs seguros vs ingreso por alquiler, una fila por cuota del
cronograma completo (pasadas y futuras, `hipotecas_materialized` sin filtrar por `pagado`).

### `/hipoteca-equity`
`pagado`/`pendiente`/`total` de `capital_total` agrupado por `pagado` — equity ya generado vs
capital que falta por amortizar, sobre el cronograma completo (incluye cuotas futuras aún no
vencidas en `pendiente`).

### `/hipoteca-amortizaciones`
Serie mensual de `amortizacion_capital` (solo cuotas pagadas y con amortización > 0) — abonos
extra a capital a lo largo del tiempo.

**Caveat**: ninguno de estos 4 endpoints está validado en este documento contra la fuente
primaria del banco (tasa de interés pactada, TEA, cronograma oficial) — eso vive en el Google
Sheet fuente (`hip_depa_alameda_dolores`) que carga `finances_bq`, fuera del alcance de
`dashboard_api`. Si se necesita verificar la tasa/TEA real, hay que revisar esa hoja o el
contrato, no este documento.

---

## 4. Costo de vida (costo de oportunidad en horas trabajadas)

Fuente de datos: **real**, tabla gold `agg_costo_en_vida` (cada transacción de gasto con
`costo_en_horas = importe / pago_por_hora` del mes correspondiente, o el último pago por hora
conocido si no hay uno para ese mes — ver `agg_pago_por_hora`).

### `/costo-vida-kpis`
Total de horas trabajadas equivalentes por categoría, con un mapeo manual y **cerrado** a 9
buckets: `comida, facturas, regalos, entretenimiento, transporte, salud, inversiones, mujeres,
equipo_trabajo`. `mujeres` captura por `subcategoria = 'Mujeres'` (prioridad sobre la
categoría); el resto por `categoria` exacta (case-insensitive vía `.lower()` en Python, no en
SQL).

**Caveat de cobertura**: cualquier categoría real que no esté en ese mapeo (`Casa`, `Viajes`,
`Autocuidado`, `Anuncios`, `Auto`, `Lavandería`, `No comestibles`, `Deudas`, `Seguros`,
`Préstamos`, `Comisiones`, `Impuestos`, `Jubilación` — todas existen en `fact_transactions`
según conteo del 2026-08-22) se descarta silenciosamente: su `costo_en_horas` existe en
`agg_costo_en_vida` pero no aparece en ningún campo de la respuesta. El total de horas que ve
el usuario en este endpoint **subestima** el costo de vida real en horas trabajadas.

### `/costo-vida-detalles`
Detalle transaccional de costo en horas para una categoría (o `subcategoria = 'Mujeres'` si se
pide `categoria=Mujeres`), con `horas_trabajadas_mes`/`pago_por_hora_mes` del mes de cada
transacción — permite auditar de dónde sale cada hora del KPI anterior.

### `/costo-vida-grafico`
Igual métrica que `/costo-vida-kpis` pero como serie mensual y con una whitelist de 10
categorías fija (`Comida, Viajes, Regalos, Entretenimiento, Facturas, Salud, Transporte,
Autocuidado, Inversiones, Equipo de trabajo`) — nótese que esta whitelist **no coincide** con
los 9 buckets de `/costo-vida-kpis` (agrega `Viajes` y `Autocuidado`, que en `/costo-vida-kpis`
se pierden por el caveat anterior). Excluye `subcategoria = 'Mujeres'` de todas las categorías
(se reporta aparte en la sección social).

### `/estabilizadores`
Gasto (no horas — `importe_moneda_principal` en soles) en una lista fija de "estabilizadores"
de ánimo/rutina (`Pan, Pancito, Cerveza, Whisky, Café, Café Instantáneo, Popcorn`), con
`Café`/`Café Instantáneo` reetiquetados según si la categoría es `Comida` o `Entretenimiento`.
Es un indicador de hábito de consumo, no un KPI de costo de vida per se.

---

## 5. Libertad financiera

Fuente de datos: **real** (`fact_transactions`; `/libertad-financiera` no usa presupuesto).

### `/ingreso-pasivo`
Serie mensual y acumulada de `importe_moneda_principal` donde `categoria = 'Pasivo'`
(confirmado: 57 transacciones históricas, todas `ingreso_gasto = 'Ingreso'` — es decir, esta
categoría solo se usa para ingresos pasivos, no hay riesgo de mezclar con gastos etiquetados
igual). No excluye nada dentro de esa categoría.

### `/libertad-financiera`
`cobertura = pasivo / indispensable * 100` por mes — el % del gasto indispensable mensual que
ya cubre el ingreso pasivo. Usa su propia definición de "indispensable" (ver 0.3: `Comida,
Transporte, Facturas, Deudas, Salud, Gastos Variables`, sin `Seguros` y con `Deudas` en vez de
`Deudas indispensables`). `latest_cobertura` toma el mes más reciente, salvo que sea el mes en
curso (parcial) y haya un mes anterior disponible, en cuyo caso usa ese para no mostrar un %
artificialmente bajo por datos incompletos del mes actual.

---

## 6. Patrimonio neto

### `/net-worth`
Fuente de datos: **real**, mezcla de `hipotecas_materialized` (equity/deuda) + `fact_transactions`
(FIBRAS, cuentas de alto rendimiento, efectivo). Documentado con docstring propio en el código
(líneas 576-581): **no incluye valorización de activos** (ni plusvalía de FIBRAS ni del
inmueble) — solo importes reales verificables (aportes/saldos/capital amortizado), decisión ya
conversada con el usuario.

Componentes de la serie mensual y del `snapshot` (último mes):
- `equity_hipoteca`: capital acumulado pagado del cronograma (`capital_pagado_mes` acumulado),
  0 antes del mes de la primera cuota (`mes_inicio_hipoteca`) — evita que meses previos a la
  hipoteca muestren deuda pendiente completa por falta de dato.
- `hipoteca_pendiente` (pasivo): `capital_total` **de todo el cronograma** (pasadas y futuras)
  menos `equity_hipoteca` acumulado — deliberadamente no se filtra por fecha para no ignorar el
  capital de cuotas futuras aún no vencidas (ver comentario en código, línea ~582).
- `fibras`: acumulado de `importe_moneda_principal` donde `categoria='Inversiones' AND
  subcategoria='FIBRAS'` (confirmado: 3 transacciones históricas, ~13,428 soles).
- `cuentas_alto_rendimiento`: acumulado neto en `Wow Compartamos`/`Pichincha`/`GNB`, sumando
  `'Ingreso'`/`'Dinero ingresado'` y restando `'Gastos'`/`'Dinero gastado'` (ver 0.4 para el
  contraste con `/crecimiento-kpis`, que no maneja `'Ingreso'`/`'Gastos'` en estas cuentas).
- `efectivo`: balance de caja acumulado de todo el historial (`Ingreso - Gastos` de
  `fact_transactions`, sin filtro de fecha) **menos** `cuentas_alto_rendimiento` — se resta
  porque ese dinero ya se movió a las cuentas de alto rendimiento vía `'Dinero
  ingresado'`/`'Dinero gastado'` (que no descuenta del balance de caja por sí solo, al no ser
  `'Ingreso'`/`'Gastos'`), y si no se restara se contaría el mismo efectivo dos veces.
- `patrimonio_neto`: `equity_hipoteca + fibras + cuentas_alto_rendimiento + efectivo -
  hipoteca_pendiente`.

La serie visible arranca en `2024-05` (`MES_INICIO_SERIE`) porque antes de esa fecha las
transacciones no se registraban de forma consistente — pero los acumulados sí incluyen esos
meses previos (no se descartan del cálculo, solo no se muestran como puntos de la serie), así
que el arranque visible ya trae el acumulado correcto.

---

## 7. Ingresos

### `/ingreso-neto-mensual`
Fuente de datos: **real**, tabla gold `agg_ingresos`. `ingreso_neto` = ingreso bruto
(`categoria IN ('Salario', 'Freelance', 'Pasivo')`) menos descuentos (`categoria IN
('Jubilación', 'Comisiones')` o `concepto IN ('Quinta categoría', 'ITF')`) — ver
`finances_bq/models/gold/agg_ingresos.sql`. Es un ingreso neto de deducciones tipo planilla, no
de gasto de vida.

Este mismo `ingreso_neto` es la base de `mediana_ingreso_neto` en `/crecimiento-kpis` (mediana
vía `PERCENTILE_CONT(..., 0.5)`, no promedio — para no dejarse arrastrar por meses atípicos con
bonos o ingresos extraordinarios).

---

## 8. KPIs compuestos de crecimiento patrimonial

### `/crecimiento-kpis`
El endpoint más complejo: mezcla presupuesto (planeado) y real (transacciones + hipoteca) en un
solo payload. Desglose campo por campo:

- `fondo_emergencia`: **constante hardcodeada en el código** (`30000.0`), no viene de ninguna
  tabla — no es un KPI calculado, es una meta fija de referencia.
- `supervivencia_estricta`: **presupuesto** (planeado) del mes más reciente cargado en
  `presupuesto_materialized`, sumando solo las categorías consideradas gasto mínimo de
  supervivencia (`Comida, Transporte, Facturas, Deudas indispensables, Salud, Gastos
  Variables, Seguros`). Sujeto al bug de 0.2 (`'Seguros'` nunca matchea `'Seguro'` real) y al de
  0.1 (`'Deudas indispensables'` sin contraparte real, aunque aquí como es presupuesto sí tiene
  filas). Tiene fallback a `agg_cumplimiento_presupuesto` si la query principal falla.
- `supervivencia_vida`: **presupuesto** total del mes (todas las categorías, sin filtro) — el
  presupuesto completo de vida, no solo lo estricto.
- `mediana_ingreso_neto`: **real**, mediana de `agg_ingresos.ingreso_neto` en el rango de fechas
  pedido (o todo el historial si no se pasan fechas).
- `tasa_gasto_consumo`, `tasa_construccion_patrimonio`, `tasa_ahorro_caja`: **real**, sobre
  `fact_transactions` del periodo pedido. Se reparte el 100% del ingreso del periodo en tres
  buckets que suman exactamente 100% por construcción:
  - `construccion_patrimonio_periodo` = FIBRAS + amortización voluntaria (`categoria='Deudas'`
    o `categoria='Inversiones' AND subcategoria='Inmuebles' AND concepto='Amortización'`) +
    cuentas de alto rendimiento (ver caveat 0.4) + **capital** (no interés/seguros) de la cuota
    hipotecaria obligatoria del periodo, leído de `hipotecas_materialized.capital_cuota` con
    `pagado=true`.
  - `gasto_consumo_periodo` = gasto total del periodo (`ingreso_gasto='Gastos'`) **menos**
    FIBRAS, amortización voluntaria y el capital hipotecario ya contados arriba como
    patrimonio — lo que queda son interés + seguros de la hipoteca (si aplica) más todo el
    resto de gasto de consumo real. La razón documentada en el código: pagar el capital de la
    cuota (obligatoria o extra) construye equity igual que un abono voluntario; la única
    diferencia real entre "cuota obligatoria" y "abono voluntario" es que ambas separan capital
    (patrimonio) de interés/seguros (consumo real, costo de haber pedido el préstamo) — por
    eso a ambas se les resta el capital del bucket de gasto.
  - `tasa_ahorro_caja` = 100% − las otras dos tasas (residual; puede dar negativo si se retiró
    más efectivo del que entró en el periodo).
- `deuda_indispensable_mensual`: **presupuesto** (no gasto real) para `categoria = 'Deudas
  indispensables'` del mes más reciente. Como es presupuesto, no está afectado por el bug 0.1,
  pero sí significa que `ratio_deuda_ingreso` es "cuota planeada / ingreso mediano", no "cuota
  efectivamente pagada / ingreso" — si el presupuesto de esa categoría estuviera desactualizado
  respecto al monto real de la cuota, el ratio no lo reflejaría.
- `ratio_deuda_ingreso`: `deuda_indispensable_mensual / mediana_ingreso_neto * 100` — un DTI
  (debt-to-income) mensual aproximado, basado en presupuesto planeado, no en pago real
  verificado contra `hipotecas_materialized.cuota_mensual`.

---

## 9. Gasto social / compartido ("Mujeres")

No estaba en la lista original del pedido pero son endpoints con lógica de negocio no trivial
sobre gasto real; se incluyen por completitud.

### `/social-kpis`
- `gasto_bruto`: **real**, suma de `fact_transactions` con `subcategoria = 'Mujeres'` y
  `ingreso_gasto = 'Gastos'`, **sin excluir `categoria = 'Préstamos'`**.
- `reembolsos`: gasto con `categoria = 'Reembolsos' AND subcategoria = 'Mujeres'`.
- `gasto_neto` = `gasto_bruto - reembolsos`.
- `porc_social`: `gasto_mujeres / ingreso_total` leído directamente de la tabla gold
  `agg_social`, cuya definición de `gasto_mujeres` (ver
  `finances_bq/models/gold/agg_social.sql`) **sí excluye** `categoria = 'Préstamos'`.

**Caveat confirmado**: `gasto_neto` (calculado en `app.py` sobre `fact_transactions` sin excluir
Préstamos) y el numerador implícito de `porc_social` (`agg_social.gasto_mujeres`, que sí excluye
Préstamos) usan definiciones distintas de "gasto en Mujeres" dentro del mismo endpoint — no son
comparables entre sí ni deberían sumarse.

### `/social-tendencia`
Serie mensual de `agg_social.gasto_mujeres` (la definición que excluye `Préstamos`) — distinta
de `gasto_bruto`/`gasto_neto` de `/social-kpis` por la razón anterior.

### `/social-top-gastos` y `/social-categoria`
Detalle y desglose por categoría del gasto real con `subcategoria = 'Mujeres'`, excluyendo
`categoria IN ('Préstamos', 'Reembolsos')` — esta sí es consistente con la definición de
`agg_social`.

### `/social-compartido`
Gasto neto mensual por persona (`valor`) para transacciones marcadas con `clave = 'C/'`
("compartido"), excluyendo `categoria = 'Reembolsos'`. No está limitado a `subcategoria =
'Mujeres'` — es gasto compartido con cualquier persona, no solo el bucket social.

---

## 10. Esencial vs. discrecional

### `/gasto-esencial-discrecional`
Ya cubierto en detalle en 0.3 (definición de "esencial") y 0.1 (exclusión de `Inversiones`/
`Deudas` completas, lo que saca la hipoteca entera — capital, interés y seguros — de ambos
buckets). `pct_esencial = esencial / (esencial + discrecional) * 100` por mes. Fuente: **real**
(`fact_transactions`).

---

## Fuera de alcance de este documento

- `/ultima-actualizacion`: metadato técnico (`MAX(fecha_carga)`), no es un KPI financiero.
- Endpoints que solo listan transacciones sin agregación de negocio ya se explican por su
  nombre y parámetros.
