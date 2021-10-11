# twitter-analysis

## Objetivo

El objetivo de este trabajo es utilizar las siguientes tecnologías vistas durante el Seminario Intensivo de Tópicos Avanzados de la Especialización en Ciencia de Datos del ITBA:
- Docker
- Kafka
- Spark

A su vez, se desea integrar Kafka a una api de Tweeter que permita escuchar un feed de tweets en tiempo real. A partir de estos tweets se realizará un análisis.

## Ambiente

### Paso 1

Clonar el repositorio

~~~
git clone https://github.com/fbartolome/twitter-analysis.git
~~~

### Paso 2

Buildear la imagen de docker

~~~
docker build .
~~~

Levantar el ambiente

~~~
./control-env.sh start
~~~

### Paso 3

Descargar dependencias en los respectivos containers:
- worker1: 
~~~
docker exec -it worker1 bash
apt-get update && apt-get install python-pip && pip install kafka-python==2.0.1 tweepy==3.10.0 bar_chart_race
~~~
- worker2:
~~~
docker exec -it worker2 bash
apt-get update && apt-get install python-pip && pip install kafka-python==2.0.1 emoji pyspark==2.4.5
~~~
- jupyter:
~~~
docker exec -it jupyter bash
apt-get update && apt-get install python-pip && pip install pyspark==2.4.5 bs4 c && apt install ffmpeg
~~~

### Paso 4

Correr el programa

#### Levantar el producer

Para levantar el producer de Kafka, el cual estara publicando datos de tweets de CABA, se debe correr el siguiente comando desde `worker1`
~~~
python /app/twitter_producer.py kafka:9092 tw <consumer_key> <consumer_secret> <access_token> <access_token_secret> <search>
~~~

Los parametros `consumer_key`, `consumer_secret`, `access_token`, y `access_token_secret` permiten la autenticación a Twitter. Para obtener dichos valores se debe tener una cuenta de Twitter. En el siguiente [link](https://developer.twitter.com/en/apply-for-access) se detalla como obtener las credenciales.
El parametro `search` permite filtrar tweets en base a su contenido. El valor de la variable es un string con palabras separadas por comas `','` o espacios `' '`, donde los espacios implican un AND lógico y las comas un OR. Entonces por ejemplo: si el valor de `search` es `"rojo,azul verde"`, se buscarán tweets que contengan o el termino `rojo` o los términos `azul` y `verde`.

#### Levantar el consumer

Para levantar el consumer de Kafka, el cual estará escuchando los mensajes que produce el producer y persistiéndolos con spark, se debe correr el siguiente comando desde `worker2`
~~~
spark-submit \
  --master 'spark://master:7077' \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5 \
  --total-executor-cores 1 \
  /app/twitter_emoji_consumer.py \
  kafka:9092 tw
~~~

Particularmente este producer reconoce los tweets que contengan algún emoji y persiste dichos emojis juntos con la fecha y hora del tweet, de mode que se pueda realizar un análisis de tendencias de emojis posteriormente.

#### Notebook generador del análisis

El análisis consta en realizar un barchar de tendencias animado que muestre cada hora los emojis mas usados en tweets de CABA que contenga el texto que se especificó con el parámetro `search`.
Desde el container de jupyter se puede encontrar el notebook `Twitter emoji bar chart race.ipynb`, el cual contiene la forma de volcar el análisis en un video corto. Para realizar el video se utilizó la librería `bar_chart_race`. Lamentablemente la librería todavía no soporta emojis, por lo que se tuvieron que traducir los emojis a texto para realizar el video.

A continuación se puede ver el resultado de los emojis mas utilizados entre el 10 de Octubre del 2021 a las 12:00 y el 11 de Octubre del 2021 a las 16:00 buscando tweets que contengan 'alberto fernandez':


![bcr_cumulative_alberto_fernandez](https://user-images.githubusercontent.com/7634605/136855170-8be8c2a0-0193-4dd6-a3b1-f865a914879b.gif)
