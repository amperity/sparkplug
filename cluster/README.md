Docker Spark Cluster
====================

A simple spark-master and two-worker cluster for use in testing and debugging
deployed Spark applications. This setup surfaces serialization and classpath
issues that do not occur in local development contexts.


## Usage

Initialize the cluster, containing a master and one worker:

```shell
docker compose up -d
```

You can submit an application with the submit script:

```shell
cp $PROJECT/target/uberjar/my-app.jar cluster/code/
./submit.sh my-app.jar
```

You can also submit an application using the Spark master's REST API. First,
create a JSON file with the request body:

```json
{
    "action": "CreateSubmissionRequest",
    "appArgs": ["file:///data/hamlet.txt"],
    "appResource": "file:///mnt/code/my-app.jar",
    "clientSparkVersion": "3.5.1",
    "environmentVariables": {"SPARK_ENV_LOADED": "1"},
    "mainClass": "my_app.main",
    "sparkProperties":
    {
        "spark.app.name": "my-app",
        "spark.submit.deployMode": "cluster",
        "spark.jars": "file:///mnt/code/my-app.jar",
        "spark.driver.cores": 1,
        "spark.driver.memory": "1G",
        "spark.driver.supervise": "false",
        "spark.executor.cores": 1,
        "spark.executor.count": 1,
        "spark.executor.memory": "1G",
        "spark.logConf": "true"
    }
}
```

Then submit it to the scheduling HTTP endpoint:

```shell
curl http://localhost:6066/v1/submissions/create --data @request.json
```

## Endpoints

All of these are from docker host:

* spark-master [http:8080](http://localhost:8080)
* spark-driver [http:4040](http://localhost:4040) (when an application is running)
* legacy submission [spark:7077](spark://localhost:7077)
* REST API submission [spark:6066](spark://localhost:6066)
