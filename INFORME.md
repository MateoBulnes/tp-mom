# Decisiones de diseño

Implementación en Python.

- La lógica común a los dos modelos (conexión, canal, loop de consumo, cierre y traducción de
  errores) está en una clase base `_RabbitMQMiddleware`. Cada clase particular agrega solo lo que
  cambia, qué topología declara y a dónde publica. Las particulares heredan de la base y de lo provisto por la
  cátedra.

- Cada objeto tiene su propia conexión y su propio canal. No se comparten entre objetos porque el
  `BlockingConnection` de pika no es thread-safe, y porque cerrar un objeto no debería arrastrar a
  otro.

- El exchange es `direct`. El broadcast no lo da el tipo de exchange sino la topología, por lo que un `direct`
  entrega una copia a todas las colas cuya binding key coincida, así que con una cola por suscriptor
  todos reciben el mensaje, y con una cola compartida se reparte el trabajo.

- `prefetch_count=1`. Sin esto el broker pushea los mensajes al primer consumidor que se suscribe y
  los demás se quedan esperando, porque el reparto ocurre al despachar y no al procesar. Con el
  límite en 1 no manda otro mensaje hasta que el anterior esté confirmado.

- Consumo con `auto_ack=False`, para poder confirmar a mano como pide la interfaz y para que lo que
  quedó sin confirmar vuelva a la cola si el consumidor se cancela.

- La cola privada del suscriptor se declara recién en el primer `start_consuming` y no en el
  constructor. Si la declarara en el constructor, un objeto usado solo para publicar crearía igual
  una cola que nadie lee. El punto de hacerlo así es que un suscriptor recibe desde que empieza a 
  consumir y no desde que se construye.
