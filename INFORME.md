# Informe - TP Coordinación

## Multiples Clientes

El aislamiento entre clientes concurrentes se logra tageando cada mensaje con un `client_id` único. Sum, Aggregator y Joiner mantienen su estado en diccionarios indexados por `client_id`.

## Coordinación entre instancias de Sum

El `MessageHandler` del Gateway cuenta cuántos registros envía cada cliente y agrega ese total al mensaje de EOF: `[client_id, total]`.

Cada Sum mantiene por `client_id`, un `done_counts` con el conteo de datos que cada Sum procesó.

Existe un exchange de control (`SUM_CONTROL_EXCHANGE`) al que todos los Sums están suscritos. Se usan dos conexiones separadas por Sum (una para consumir control, otra para publicar). Cuando un Sum recibe el EOF desde `INPUT_QUEUE` difunde `["EOF", client_id, total]` por el exchange de control.

Al recibir ese aviso cada Sum publica su conteo acumulado `["DONE", client_id, sum_id, count]` y, a partir de ese momento, re-publica su conteo cada vez que procesa un dato nuevo de ese cliente. Antes de recibir el EOF de control, un Sum no publica DONEs, evita tráfico de control innecesario (inicialmente se publicaba un `DONE` por cada dato procesado sin necesidad de saber si se recibió el EOF).

Cada Sum va comparando la suma de los `done_counts` con el `total` del EOF que llegaba desde el gateway. Cuando se cumple, entonces no queda ningún dato de ese cliente sin procesar en ningún Sum, y cada uno hace el flush hacia los Aggregators seguido de un EOF.

## Coordinación entre Aggregators

Cada Sum calcula un hash sobre `(client_id, fruit)` y envía cada fruta a un único Aggregator. El EOF de cada Sum se envía a todos los Aggregators. El total de una fruta dada para un cliente se concentra siempre en un mismo Aggregator. Este particionado distribuye la carga entre los Aggregators de forma pareja.

Cada Aggregator cuenta cuántos EOFs recibió por cliente. Al llegar a `SUM_AMOUNT` EOFs calcula su top parcial y lo envía al Joiner como `[client_id, top_parcial]`.

## Fusión en el Joiner

El Joiner, a medida que va recibiendo los tops parciales y actualizando el top final por cliente, cuenta cuántos Aggregators le respondieron. Al llegar a `AGGREGATION_AMOUNT` emite el top final de um cliente hacia el Gateway.
