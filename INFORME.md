# Informe - TP Coordinación

## Coordinación entre instancias de Sum

El `MessageHandler` del Gateway cuenta cuántos registros envía cada cliente y agrega ese total al mensaje de EOF: `[client_id, total]`.

Cada Sum mantiene, por `client_id`, un `done_counts` con el conteo de datos que cada Sum procesó.

Existe un exchange de control (`SUM_CONTROL_EXCHANGE`) al que todos los Sums están suscritos. Se usan dos conexiones separadas por Sum (una para consumir control, otra para publicar). Cuando un Sum recibe el EOF desde `INPUT_QUEUE` difunde `["EOF", client_id, total]` por el exchange de control.

Al recibir ese aviso cada Sum publica su conteo acumulado `["DONE", client_id, sum_id, count]` y, a partir de ese momento, re-publica su conteo cada vez que procesa un dato nuevo de ese cliente. Antes de recibir el EOF de control, un Sum no publica DONEs, evita tráfico de control innecesario.

Cada Sum va comparando la suma de los `done_counts` con el `total` del EOF. Cuando se cumple, entonces no queda ningún dato de ese cliente sin procesar en ningún Sum, y cada uno hace el flush hacia los Aggregators seguido de un EOF.

Todo acceso a estado compartido (`totals`, `done_counts`) y a las conexiones de publicación está protegido por un lock, porque lo tocan tanto el hilo de datos como el hilo de control.

## Coordinación entre Aggregators

Cada Sum no envía todas sus frutas a todos los Aggregators, calcula un hash sobre `(client_id, fruit)` y envía cada fruta a un único Aggregator. El EOF de cada Sum se envía a todos los Aggregators.

Esto hace que el total de una fruta dada para un cliente, se concentre siempre en un mismo Aggregator.

Cada Aggregator cuenta cuántos EOFs recibió por cliente (uno por cada Sum). Al llegar a `SUM_AMOUNT` EOFs calcula su top parcial y lo envía al Joiner como `[client_id, top_parcial]`.

## Fusión en el Joiner

El Joiner, a medida que va recibiendo los tops parciales y actualizando el top final por cliente, cuenta cuántos Aggregators le respondieron. Al llegar a `AGGREGATION_AMOUNT` emite un único resultado final hacia el Gateway.

## Escalabilidad

### Clientes

El aislamiento entre clientes concurrentes se logra tageando cada mensaje con un `client_id` único. Sum, Aggregator y Joiner mantienen su estado en diccionarios indexados por `client_id`, de forma que el procesamiento de un cliente nunca interfiere con otro. Escalar la cantidad de clientes no requiere ningún cambio estructural: cada uno simplemente agrega una entrada más a esos diccionarios, que se libera al completarse el flush.

### Volumen de datos en los Aggregators

El particionado por hash `(client_id, fruit)` distribuye la carga entre las instancias de Aggregator de forma pareja, permitiendo que `AGGREGATION_AMOUNT` escale horizontalmente con el volumen total de datos.

### La cantidad de mensajes de control

Inicialmente se publicaba un `DONE` por cada dato procesado, lo que implica `O(D)` mensajes de control por Sum (siendo `D` la cantidad de datos que le tocaron), cada uno con fan-out a `SUM_AMOUNT.

Se realizó una pequeña mejora que evita publicar mientras el EOF de control no se haya recibido. Al recibir el EOF, cada Sum publica una vez su conteo acumulado y de ahí en más sólo re-publica por los datos que llegan después de ese punto (los que estaban en tránsito, en cantidad `K`, mucho menor a `D`). Esto reduce el tráfico de control por Sum, manteniendo el mismo resultado.
