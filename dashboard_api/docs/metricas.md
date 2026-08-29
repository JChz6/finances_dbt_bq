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

**Actualizado 2026-08-29**: se revisó el estado real de cada bug de la sección 0 contra el
código actual (no solo se asumió corregido) — casi todos se arreglaron en una ronda de fixes
(commit `17b3ad4`, 2026-08-28) posterior a la verificación original del 2026-08-22, más un fix
adicional a `agg_cumplimiento_presupuesto` (commit `c1d8ba8`) y un ajuste de neteo en
`/gastos-categoria` (2026-08-29, ver 1.1). El detalle de qué quedó corregido y qué no está en
cada subsección de abajo — en particular, 0.1 quedó **solo parcialmente corregido**.

---

## 0. Problemas transversales (afectan a varios endpoints — leer antes que las secciones)

### 0.1 La cuota de hipoteca no se categoriza como "Deudas" — **corregido a nivel de KPI, NO a nivel de dato crudo**

En `fact_transactions`, todas las transacciones de la cuota de "Depa Alameda Dolores"
(`valor = 'Depa Alameda Dolores'`) siguen bajo `categoria = 'Inversiones'`, con o sin
`subcategoria = 'Inmuebles'` (concepto `Cuota hipoteca`, `Amortización`, `Cuota inicial`,
`Intereses y seguros amortización`, etc.). **Nunca** aparecen con `categoria = 'Deudas'`, y
`categoria = 'Deudas indispensables'` **sigue sin existir como valor real en
`fact_transactions`** (solo vive en `presupuesto_materialized`). Esto no cambió — no fue (ni se
intentó) un fix de recategorización de la transacción cruda.

Lo que sí se corrigió (commit `17b3ad4`, 2026-08-28): `finances_bq/models/silver/
hipotecas_materialized.sql` ahora separa cada cuota en `costo_vida_interes_seguros` (interés +
seguro_propiedad + seguro_desgravamen) cuando el inmueble tiene `proposito = 'Vivienda propia'`
vigente en la fecha de vencimiento (vía el Sheet `tracking_inmuebles`, rango `[fecha_inicio,
fecha_fin]`) — y `costo_operativo_inversion_interes_seguros` cuando el propósito es otro
(inversión de renta). El modelo gold `agg_gasto_esencial` (nuevo, mismo commit) suma
`costo_vida_interes_seguros` directamente al gasto esencial mensual, sin pasar por el nombre de
categoría roto. **Capital** de la cuota nunca se toca por este split — sigue siendo patrimonio
siempre, sin importar el propósito.

**Efecto concreto, endpoint por endpoint:**

- **`/cumplimiento-presupuesto` sigue roto para `categoria = 'Deudas indispensables'`** —
  confirmado contra `agg_cumplimiento_presupuesto.sql` actual: sigue siendo un join directo por
  nombre de categoría entre `fact_transactions` y `presupuesto_materialized`, sin ningún aporte
  de `hipotecas_materialized`. Esa fila sigue mostrando `gasto_acumulado_mes = 0` y
  `cumplimiento = 'CUMPLE'` aunque la cuota sí se pague — el fix de hipoteca no tocó esta tabla
  porque no depende de la categoría "esencial" unificada, depende del nombre crudo de
  categoría. Este caveat ya está reflejado en el tooltip del dashboard para esa tarjeta
  (`index.html`).
- **`/crecimiento-kpis` → `deuda_indispensable_mensual` y `ratio_deuda_ingreso`**: sin cambios,
  siguen leyendo presupuesto (lo planeado), no gasto real — el ratio DTI sigue siendo "cuota
  planeada / ingreso", no "cuota efectivamente pagada / ingreso".
- **`/gasto-esencial-discrecional`, `/libertad-financiera` y `supervivencia_estricta` de
  `/crecimiento-kpis` — corregidos**: los tres ahora leen de `agg_gasto_esencial` (fuente
  única, ver 0.3), que suma `costo_vida_interes_seguros` al bucket esencial. El interés + seguro
  de la hipoteca de vivienda propia **ya cuenta** como gasto esencial real en estos tres
  endpoints — antes quedaba completamente fuera al excluirse `'Inversiones'` en bloque.
- La hipoteca se sigue midiendo correctamente en los endpoints dedicados `/hipoteca-*` y en
  `/net-worth`, que leen `hipotecas_materialized` directo — sin cambios ahí.

### 0.2 Mismatch 'Seguro' (presupuesto) vs 'Seguros' (código) — **corregido**

`presupuesto_materialized.sql` (silver) ahora normaliza el nombre de categoría al cargar desde
el Sheet: `WHEN UPPER(TRIM(categoria)) = 'SEGURO' THEN 'Seguros'` (junto con `'Gastos
variables'` → `'Gastos Variables'`, mismo problema de capitalización inconsistente). Con esto,
`'Seguros'` (plural) sí existe en `presupuesto_materialized` y el join con la lista canónica de
esenciales (ver 0.3) ya no pierde esas filas — `supervivencia_estricta` en `/crecimiento-kpis`
ya no subestima el presupuesto de seguros de la hipoteca.

En `fact_transactions` (gasto real) la categoría ya se llamaba `'Seguros'` (plural) de por sí,
así que ese lado nunca tuvo el problema — el bug vivía exclusivamente en la carga de
`presupuesto_materialized`, y ahí quedó resuelto.

### 0.3 Tres definiciones distintas de "gasto esencial/indispensable" — **unificadas**

`/crecimiento-kpis` (`supervivencia_estricta`), `/gasto-esencial-discrecional` (`esencial`) y
`/libertad-financiera` (`indispensable`) leían tres listas de categorías hardcodeadas distintas
en `app.py` y daban tres números distintos para "gasto esencial" del mismo mes. Corregido
(commit `17b3ad4`): los tres ahora leen del mismo campo (`gasto_esencial_real` /
`presupuesto_esencial`) de la tabla gold `agg_gasto_esencial`, que a su vez usa una lista única
de categorías definida en el macro dbt `categorias_esenciales_supervivencia()`:

```
COMIDA, TRANSPORTE, FACTURAS, SALUD, GASTOS VARIABLES, SEGUROS
```

Nota deliberada: esta lista **excluye** `Deudas`/`Deudas indispensables` a propósito — esos
nombres nunca capturan la hipoteca en `fact_transactions` (ver 0.1) — y en su lugar
`agg_gasto_esencial` suma aparte el interés + seguros real de la hipoteca de vivienda propia
(`hipotecas_materialized.costo_vida_interes_seguros`). El resultado es el mismo número de
"esencial" en los tres endpoints para un mismo mes, y ya incluye el costo real de la hipoteca en
vez de subestimarlo.

### 0.4 Cuentas de alto rendimiento en `/crecimiento-kpis` vs `/net-worth` — **unificadas**

Antes, `/crecimiento-kpis` (`query_construccion_patrimonio`) solo contaba movimientos
`'Dinero ingresado'`/`'Dinero gastado'` en `Wow Compartamos`/`Pichincha`/`GNB`, mientras que
`/net-worth` (`q_cuentas_mensual`) también sumaba/restaba filas con `ingreso_gasto = 'Ingreso'`/
`'Gastos'` en esas mismas cuentas (interés/rendimiento acreditado) — esas filas se veían en
`/net-worth` pero no en `/crecimiento-kpis`.

Corregido (commit `17b3ad4`): ambos endpoints usan ahora exactamente la misma lógica —
`ingreso_gasto IN ('Ingreso', 'Dinero ingresado')` suma, `IN ('Gastos', 'Dinero gastado')`
resta — y la misma lista de cuentas, que además ahora incluye `Global66 - USD` (antes solo
`Wow Compartamos`, `Pichincha`, `GNB` en ambos). Confirmado en el código actual: el bloque
`CASE` de `cuentas_alto_rendimiento` es idéntico carácter por carácter en `/crecimiento-kpis` y
`/net-worth`, con comentario explícito en `/crecimiento-kpis` ("mismas cuentas y misma lógica
que `/net-worth`").

**Fuente única (2026-08-29)**: la lista de cuentas ya no está hardcodeada en `app.py` — ambos
`cuenta IN (...)` fueron reemplazados por `cuenta IN (SELECT cuenta FROM
cuentas_alto_rendimiento)`, la tabla gold nueva `finances_bq/models/gold/
cuentas_alto_rendimiento.sql` (macro `cuentas_alto_rendimiento()`, 4 filas hoy). Motivo: cuando
se agregó `Global66 - USD` se actualizó `app.py` pero no `tracking_inversiones.sql` en
`finances_bq`, y quedaron desincronizados por semanas sin que nadie lo notara. Ahora dbt y la
API leen de la misma tabla — abrir/cerrar una cuenta se actualiza en un solo lugar.

---

## 1. Flujo de caja

Fuente de datos: **real** (`fact_transactions`) en todos los endpoints de esta sección.

### `/flujo-caja`
Serie temporal (diaria o mensual) de ingresos, gastos y balance de caja.

- `ingresos`: suma de `importe_moneda_principal` donde `ingreso_gasto = 'Ingreso' AND categoria
  != 'Reembolsos'` — excluye devoluciones para no inflar el ingreso "real" del periodo (fix
  aplicado junto con la ronda de correcciones de la sección 0; antes sí las incluía).
- `gastos`: suma de `importe_moneda_principal` donde `ingreso_gasto = 'Gastos'`, invertida a
  positivo (`* -1` en el SQL, ya que en la tabla los gastos se guardan en negativo). Sin
  exclusión de `Reembolsos` (no aplica: los reembolsos son filas `ingreso_gasto = 'Ingreso'`,
  nunca `'Gastos'`).
- `balance`: **no usa el mismo filtro que `ingresos`** — es
  `SUM(CASE WHEN ingreso_gasto='Ingreso' THEN importe ... )`, sin excluir `Reembolsos`. Esto es
  deliberado, no una inconsistencia: `balance`/`acumulado` mide caja real movida (para eso el
  reembolso sí es un ingreso de efectivo real), mientras que el campo `ingresos` mide "ingreso"
  en el sentido de ganancia, no de flujo de caja — mismo criterio que distingue `/net-worth`
  (incluye Reembolsos, mide caja) del resto de métricas de ingreso neto (lo excluyen).
- `acumulado`: suma corriente (`SUM() OVER`) de `balance` ordenado por fecha — es la base del
  "efectivo acumulado" que después reaparece en `/net-worth` como `efectivo`.

Sin otras exclusiones de categoría: incluye la hipoteca completa (capital+interés+seguros) como
gasto y cualquier transferencia a cuentas de alto rendimiento marcada como `'Ingreso'`/`'Gastos'`
(no `'Dinero ingresado'`/`'Dinero gastado'`). Es la vista "cruda" de caja, no un KPI de consumo —
para eso ver `/gasto-esencial-discrecional` y `/crecimiento-kpis`.

### `/balance-trimestre`
Igual que `/flujo-caja` pero agregado por trimestre, usando `LOWER(ingreso_gasto) LIKE
'%ingr%'` en vez de comparar contra `'Ingreso'` exacto (matchea también `'Dinero ingresado'` —
inconsistente con el resto del código, que sí distingue ambos casos en otros endpoints).

### `/gastos-categoria` y `/ingresos-categoria`
Suma de `importe_moneda_principal` por `categoria` en un rango de fechas.

- `/gastos-categoria` excluye `concepto IN ('Cambio dólares', 'liquidación', 'Sin concepto')` y
  restringe a una whitelist fija de 18 categorías (incluye `Deudas` e `Inversiones` como
  categorías separadas — ver 0.1: la hipoteca cae dentro de `Inversiones` aquí, no de
  `Deudas`). `Anuncios` y `Préstamos` quedan fuera de esa whitelist a propósito (2026-08-29): se
  muestran netos contra `Reembolsos`, no brutos — mismo criterio que `agg_gasto_esencial` y
  `/costo-vida-kpis` (ver `agg_netos_prestamos_anuncios`). Antes de este fix, `Anuncios` se
  mostraba bruto en este gráfico mientras `/gasto-esencial-discrecional` ya usaba el neto para
  la misma categoría/mes — dos números distintos para el mismo dato en dos vistas del
  dashboard. `Préstamos` no aparecía en el gráfico en absoluto; ahora aparece neteado. Si el
  neto del rango pedido es exactamente 0, la categoría se omite del gráfico (igual que
  cualquier categoría sin transacciones en el rango).
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
`786f7eb` ("Cambio en CTE para cumplimiento de presupuesto") — cambios en el modelo
`agg_cumplimiento_presupuesto.sql`, no en `app.py`. Confirmado (2026-08-29): el fix está
materializado en BigQuery — el `dbt run` más reciente registrado corrió este modelo con éxito
el 2026-08-28, después de aplicado el fix.

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
ya cubre el ingreso pasivo. `indispensable` ahora lee `gasto_esencial_real` de
`agg_gasto_esencial` (fuente única, ver 0.3) — antes tenía su propia lista de 6 categorías,
distinta de las otras dos; ya unificada. `latest_cobertura` toma el mes más reciente, salvo que
sea el mes en curso (parcial) y haya un mes anterior disponible, en cuyo caso usa ese para no
mostrar un % artificialmente bajo por datos incompletos del mes actual.

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
- `cuentas_alto_rendimiento`: acumulado neto en `Wow Compartamos`/`Pichincha`/`GNB`/`Global66 -
  USD`, sumando `'Ingreso'`/`'Dinero ingresado'` y restando `'Gastos'`/`'Dinero gastado'` — misma
  lógica y misma lista de cuentas que `/crecimiento-kpis` (ver 0.4, ya unificadas).
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
- `supervivencia_estricta`: **presupuesto** (planeado) del mes más reciente, leído de
  `agg_gasto_esencial.presupuesto_esencial` (fuente única, ver 0.3) — suma la lista canónica de
  categorías esenciales (`Comida, Transporte, Facturas, Salud, Gastos Variables, Seguros`) más
  el interés+seguros real de la hipoteca de vivienda propia. Los bugs 0.1 y 0.2 que afectaban
  esta métrica ya están corregidos: `'Seguros'` sí matchea contra `presupuesto_materialized`
  (0.2), y la hipoteca ya no depende del nombre roto `'Deudas indispensables'` (0.1). Tiene
  fallback a `agg_cumplimiento_presupuesto` si la query principal falla.
- `supervivencia_vida`: **presupuesto** total del mes (todas las categorías, sin filtro) — el
  presupuesto completo de vida, no solo lo estricto.
- `mediana_ingreso_neto`: **real**, mediana de `agg_ingresos.ingreso_neto` en el rango de fechas
  pedido (o todo el historial si no se pasan fechas).
- `tasa_gasto_consumo`, `tasa_construccion_patrimonio`, `tasa_ahorro_caja`: **real**, sobre
  `fact_transactions` del periodo pedido. Se reparte el 100% del ingreso del periodo en tres
  buckets que suman exactamente 100% por construcción:
  - `construccion_patrimonio_periodo` = FIBRAS + amortización voluntaria (`categoria='Deudas'`
    o `categoria='Inversiones' AND subcategoria='Inmuebles' AND concepto='Amortización'`) +
    cuentas de alto rendimiento (misma lógica que `/net-worth`, ver 0.4, ya unificadas) +
    **capital** (no interés/seguros) de la cuota hipotecaria obligatoria del periodo, leído de
    `hipotecas_materialized.capital_cuota` con `pagado=true`.
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
Ya cubierto en detalle en 0.3 (definición unificada de "esencial") y 0.1. Lee
`agg_gasto_esencial`: `esencial` y `discrecional` excluyen `Inversiones`/`Deudas` en bloque
(construcción de patrimonio, no consumo) — pero **capital** de la hipoteca es lo único que
realmente se pierde por esa exclusión; el **interés + seguros** de la hipoteca de vivienda
propia sí se suma de vuelta a `esencial` (vía `hipotecas_materialized.
costo_vida_interes_seguros`, ver 0.1), y `Anuncios`/`Préstamos` se suman netos (no brutos) a
`discrecional` (ver `agg_netos_prestamos_anuncios`). `pct_esencial = esencial / (esencial +
discrecional) * 100` por mes. Fuente: **real** (`fact_transactions` + `hipotecas_materialized`).

---

## Changelog de fixes

- **2026-08-02** (`c1d8ba8`): `agg_cumplimiento_presupuesto` — el `FULL OUTER JOIN` se
  comportaba como `INNER` por un `WHERE` mal ubicado, y el filtro incremental por
  `fecha_carga` producía filas duplicadas con `month_cat_id = NULL`. Presupuestos de agosto sin
  transacción "Gastos" coincidente no cargaban. Corregido; confirmado corrido en BigQuery el
  2026-08-28.
- **2026-08-28** (`17b3ad4`): ronda grande de correcciones — ver detalle en cada punto de la
  sección 0: split hipoteca vivienda propia/inversión vía `tracking_inmuebles` (0.1, parcial —
  no toca `/cumplimiento-presupuesto`), normalización `Seguro`→`Seguros` en
  `presupuesto_materialized` (0.2), unificación de la lista de "esencial" vía
  `agg_gasto_esencial`/`categorias_esenciales_supervivencia()` en los tres endpoints que la usan
  (0.3), unificación de cuentas de alto rendimiento incluyendo `Global66 - USD` (0.4), y
  exclusión de `Reembolsos` de ingreso/flujo neto en varios endpoints (con la excepción
  deliberada de `/net-worth`, que sí lo incluye por medir caja real).
- **2026-08-29**: `/gastos-categoria` deja de mostrar `Anuncios` bruto (inconsistente con el
  neto ya usado en `/gasto-esencial-discrecional`) y agrega `Préstamos` neteado — ver sección 1.
  Este documento se actualizó para reflejar el estado real del código tras las dos rondas
  anteriores (antes describía como "sin corregir" varios bugs ya arreglados en `17b3ad4`).

---

## Fuera de alcance de este documento

- `/ultima-actualizacion`: metadato técnico (`MAX(fecha_carga)`), no es un KPI financiero.
- Endpoints que solo listan transacciones sin agregación de negocio ya se explican por su
  nombre y parámetros.
