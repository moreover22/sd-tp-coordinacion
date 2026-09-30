# Informe

## Coordinación entre Sum y Aggregation

### Flujo de Datos

La coordinación se establece en tres niveles:

#### Nivel 1: Gateway - Sum

El Gateway recibe datos de clientes TCP y los envía a través de una cola sin distinguir entre clientes (`input_queue`). Todos los Sum consumen de esta misma cola, distribuyendo el trabajo. El Gateway agrega un `client_id` que funciona como request ID que permite identificar la solicitud a lo largo del proceso. Tanto los mensajes de Fruta y EOF contienen este identificador.

Solo uno de los sums va a recibir el EOF (desde `input_queue`), así que para que el resto de sums se entere que llegó el EOF y pueden enviar los datos al siguiente nivel, se utiliza el exchange de control (`SUM_CONTROL_EXCHANGE`) para los sums para informarle el EOF de ese cliente al resto.


#### Nivel 2: Sum - Aggregation

Cada instancia de Sum procesa eventos y almacena pares (fruta, cantidad) acumulados por `client_id`. Al recibir la señal EOF de un cliente, Sum distribuye los datos finales a las instancias de Aggregation así cada evento se manda a un único aggregator. Para determinar a qué aggregator enviarlo se usa `MD5(client_id + fruit) % AGG_AMOUNT`, así la misma fruta para un cliente termina en un solo aggregator y lo puede procesar por completo sin tener que coordinar con los otros aggregator. Se usa `client_id + fruit` para distribuir mejor los eventos en caso de que alguna fruta sea muy popular.

#### Nivel 3: Aggregation - Join

Cada Aggregation aguarda señales EOF de todos los Sum Mantiene un contador por cliente.

Solo cuando recibe EOF de todos los Sum procesa su top parcial local y lo envía al Join. Como cada tipo de fruta es procesado por un único aggregator (por cliente), puede publicar el top parcial (top 3 en el caso de los tests) tranquilamente, porque ningún otro aggregator pudo haber procesado una fruta que maneja el agg actual, eliminando así casos bordes del tipo:

```
A,50
A,50
B,101
C,99
D,102
E,51
F,51
```

En este ejemplo, el top tendría que ser:
```
D,102
B,101
A,100
```

Si más de un aggregator podría manejar el mismo tipo de fruta, podría pasar que:

- Agg1:

```
recibe:

- A,50
- B,101
- D,102
- E,51

y publica el top:

- D,102
- B,101
- E,51
```

- Agg2:

```
recibe y publica:
- C,99
- E,51
- A,50
```

Cuando se hace el Join termina quedando:

```
- D,102
- B,101
- C,99
```

Al no permitir que una misma fruta sea procesado por diferentes Aggs, permite solo calcular los tops_parciales y luego calcular el top_total de forma segura. El mayor riesgo de esta implementación es que la distribución no sea uniforme y algún nodo de aggregator termine sobrecargado. 
