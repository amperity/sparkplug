Docker Spark Cluster
====================

A simple spark-master and two-worker cluster for use in testing and debugging
deployed Spark applications. This setup surfaces serialization and classpath
issues that do not occur in local development contexts.


## Usage

**TODO:** more usage instructions

```
# Launch the containers
$ docker-compose up -d

# Copy uberjar to `jars` dir, your exact steps may vary
$ lein uberjar
$ cp $PROJECT/target/uberjar/my-app.jar docker/jars/

$ ./submit.sh my-app.jar
```


## Endpoints

All of these are from docker host:

* spark-master [http:8080](http://localhost:8080)
* spark-driver [http:4040](http://localhost:4040) (when an application is running)
* legacy submission [spark:7077](spark://localhost:7077)
* REST API submission [spark:6066](spark://localhost:6066)
