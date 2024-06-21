## Gestión de la información en Data Lakes
Proyecto de Final de Máster de Redes y Servicios Telemáticos de la UPM.
Durante este proyecto se ha estudiado la inclusión de un lago de datos en una arquitectura previamente diseñada, con el fin de sustituir varios microservicios con el lago de datos.
Se ha usado Delta Lake por ser de código abierto y su gran integración con diferentes aplicaciones y servicios

Realizado por Jaime Martínez Ramón

### INTRUCCIONES PARA EL DESPLIGUE

Para desplegar el proyecto, se debe tener instalado Maven, Docker y Docker Compose en la máquina donde se quiera desplegar.
En primer lugar, ejecute el comando git clone para clonar el repositorio en la máquina.
Antes de desplegar el proyecto, se debe crear la carpeta `target`y el fichero `JAR`.
Para ello, se han de ejecutar las siguientes instrucciones en la carpeta raíz:
1. mvn clean install
2. Borrar el fichero `JAR` de la carpeta `target` si ya existe. Si no existe, saltar este paso.
3. Ejecutar un archivo. Esto generará los ficheros .class necesarios para la ejecución del proyecto.
4. mvn compile
5. mvn package

Tras la generación de la carpeta `target` y el fichero `JAR`, se deben realizar una serie de cambios en el fichero `docker-compose.yml`.
Los cambios a realizar son simplemente actualizar la ruta de los volúmenes de los siguientes servicios:
- `spark`
- `spark-worker-1`
- `spark-worker-2`
- `spark-submit-kafkareaderwriter`
- `spark-submit-bronzetosilver`
- `spark-submit-historicaldbtransformer`
- `jupyter`

Estas rutas deben indicar la ubicación donde se desea almacenar los datos generados por el proyecto en la máquina host. Es decir, solo debe modificarse
la parte local de la ruta, no la parte de la ruta en el contenedor. En caso de modificar la parte de la ruta en el contenedor, los ficheros .scala han
ser modificados.


### ESTRUCTURA DEL PROYECTO

Dentro de la carpeta `src/main/scala/org/tfmupm` se encuentran los ficheros .scala que componen el proyecto. Dentro de esta carpeta, solo son relevantes
para la ejecución en Docker aquellos ficheros que acaben con la palabra 'Docker'. Es en estos ficheros donde se encuentran las funciones que se ejecutarán.
Los demás ficheros sirven para la ejecución en local y permiten una comprobación rápida del buen funcionamiento de las aplicaciones.

#### SparkReadDocker.scala
Este primer fichero es el encargado de convertir los registros históricos descargados de los repositorios a tablas Delta.
Simplemente itera por las carpetas descargadas convirtiendo los ficheros CSV a tablas Delta.
#### KafkaReaderWriterDocker.scala
Este segundo fichero es el encargado de leer los mensajes de dos topics de Kafka y escribirlos en una tabla Delta.
Estos dos topics son 'nifitopic' para los registros ambulatorios y 'nificontinuous' para los registros continuos. En caso de cambiar el nombre de los topics,
se debe modificar el fichero `docker-compose.yml` para que los nombres coincidan.
Los datos recibidos son guardados en las tablas 'bronze_ambulatory_docker' y 'bronze_continuous_docker'.
#### BronzeToSilverDocker.scala
Este fichero es el encargado de transformar los datos de las tablas 'bronze_ambulatory_docker' y 'bronze_continuous_docker' a tablas 'silver'.
Para ello, se realiza un proceso de limpieza y transformación de los datos, eliminando duplicados y registros con valores nulos.
Además, genera una serie de tablas nuevas con la información extraída de las tablas bronze. 
#### AmbulatoryReaderDocker.scala
Este fichero es el encargado de leer los datos de todas las tablas Delta generadas. 
No está desplegada en Docker puesto que está misma labor se puede realizar utilizando 'Jupyter'.
Si se deseara desplegar valdría con añadir el siguiente contenido a `docker-compose.yml`:
```
  spark-submit-tablereader:
    image: bitnami/spark:3.5
    command: /opt/bitnami/spark/bin/spark-submit --class org.tfmupm.AmbulatoryReaderDocker --packages io.delta:delta-spark_2.12:3.1.0 --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" /home/TFMDataLake-1.0-SNAPSHOT.jar
    volumes:
      - D:/Archivos_uni/TFM/TFMDataLake/src/main/scala/org/tfmupm:/home/datalake
      - D:/Archivos_uni/TFM/TFMDataLake/target:/home
```
### ESTRUCTURA EN DOCKER

El proyecto se compone de varios servicios que se ejecutan en contenedores Docker. Estos servicios son:
- `spark`: Inicia el clúster de Spark y coordina los recursos que se darán a los nodos workers.
- `spark-worker-1`: Nodo trabajador que ejecutará las aplicaciones.
- `spark-worker-2`: Nodo trabajador que ejecutará las aplicaciones.
- `spark-submit-kafkareaderwriter`: Aplicación que lee los mensajes de Kafka y los escribe en tablas Delta.
- `spark-submit-bronzetosilver`: Aplicación que transforma los datos de las tablas bronze a tablas silver.
- `spark-submit-historicaldbtransformer`: Aplicación que transforma los datos de los repositorios a tablas Delta.
- `nifi`: Contenedor con Apache NiFi para la descarga de los datos. Recibe la información de las peticiones HTTP y las manda a Kafka.
- `zookeeper`: Contenedor con Zookeeper para la gestión de los brokers de Kafka.
- `kafka`: Contenedor con Kafka para la gestión de los mensajes. Recibe la información de NiFi y la almacena en los topics. Apache Spark recibirá la información de estos topics.
- `jupyter`: Contenedor con Jupyter para la visualización de los datos.

Estos contenedores están conectados de la siguiente forma:
![Estructura de los contenedores](/img/arquitectura.png)

### EJECUCIÓN DEL PROYECTO

Al tener iniciados todos los contenedores, dentro de la interfaz web de Spark ha de estar en estado RUNNING la aplicación KafkaReaderWriter.
Si se está ejecutando cualquier otra aplicación, se deben parar los otros contenedores para no saturar la memoria del servidor u ordenador. 

Una vez esta aplicación esté en estado RUNNING, en NiFi importar el fichero `finalambcont.xml` como template. En este template están definidos todos los procesadores necesarios para recibir la información.
Una vez esté importado y los controladores y procesadores iniciados, se pueden ejecutar los fichero `ambulatorio.sh` y `continuo.sh`.
Al enviar los datos, serán recibidos por la aplicación KafkaReaderWriter y escritos en las tablas Delta.

Cuando se quiera transformar la información de las tablas bronze a las tablas silver, se debe iniciar la aplicación BronzeToSilver. 

### VISUALIZACIÓN EN JUPYTER 

Para visualizar los datos en Jupyter, se debe acceder a la interfaz web de Jupyter y abrir el fichero `Visualizacion.ipynb`.
En este fichero se encuentran las importaciones necesarias para poder consultar las tablas, estas son:
- Pandas: Para la visualización de los datos.
- DeltaLake: Para la lectura de las tablas Delta.
- Matplotlib: Para la generación de gráficos.
- 
![Instalación e importación](/img/install_imports.png)

Una vez se han instalado e importado las dependencias necesarias, para visualizar la tabla debe ejecutarse una celda con un contenido similar al siguiente:
```
df_ejemplo = DeltaLake("%ruta_tabla%").to_pandas()
df_bronze = DeltaLake("home/datalake/data/bronze_continuous_docker").to_pandas()
```
![Ejemplo de visualización](/img/bronze_continuous.png)
También se pueden consultar los metadatos de las tablas
```
df_bronze.history()
```
![Ejemplo de visualización](/img/metadatos.png)

Gracias a Matplotlib también se pueden generar una serie de gráficos con los datos
![Ejemplo de visualización](/img/grafica_lab_sequences.png)