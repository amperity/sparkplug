Docker Spark Cluster
====================

A simple spark-master and two-worker cluster for use in testing and debugging
deployed Spark applications. This setup surfaces serialization and classpath
issues that do not occur in local development contexts.


## Usage

Initialize the cluster, containing a master and one worker:

```
docker-compose -f docker-compose.yml up -d master worker-1
```

You can submit an application with the submit script:

```
# Launch the containers
$ docker-compose up -d

# Copy uberjar to `jars` dir, your exact steps may vary
$ lein uberjar
$ cp $PROJECT/target/uberjar/my-app.jar docker/jars/

$ ./submit.sh my-app.jar
```

You can also submit an application using the Spark master's REST API:

```
# Place a JSON request body in a file
$ cat request.json
{
    "action": "CreateSubmissionRequest",
    "appArgs": ["file:///data/hamlet.txt"],
    "appResource": "file:///mnt/jars/spark-word-count.jar",
    "clientSparkVersion": "2.4.4",
    "environmentVariables": {"SPARK_ENV_LOADED": "1"},
    "mainClass": "spark_word_count.main",
    "sparkProperties":
    {
        "spark.jars": "file:///mnt/jars/spark-word-count.jar",
        "spark.executor.cores": 1,
        "spark.executor.count": 1,
        "spark.executor.memory": "1G",
        "spark.driver.cores": 1,
        "spark.driver.memory": "1G",
        "spark.driver.supervise": "false",
        "spark.app.name": "sparkplug",
        "spark.submit.deployMode": "cluster",
        "spark.logConf": "true"
    }
}

$ curl -X POST --data @request.json http://localhost:6066/v1/submissions/create
{
  "action" : "CreateSubmissionResponse",
  "message" : "Driver successfully submitted as driver-20200324235704-0000",
  "serverSparkVersion" : "2.4.4",
  "submissionId" : "driver-20200324235704-0000",
  "success" : true
}
```

## Endpoints

All of these are from docker host:

* spark-master [http:8080](http://localhost:8080)
* spark-driver [http:4040](http://localhost:4040) (when an application is running)
* legacy submission [spark:7077](spark://localhost:7077)
* REST API submission [spark:6066](spark://localhost:6066)
