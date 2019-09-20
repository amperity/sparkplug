Spark REPL
==========

This project provides a server providing an interactive REPL experience while
connected to a Spark cluster.


## Usage

First, build the REPL uberjar and copy it into the Docker cluster:

```
$ lein uberjar
$ cp target/uberjar/sparkplug-repl.jar ../cluster/jars
```

Next, start up the REPL container in another terminal:

```
$ docker-compose up repl
```

Finally, connect to the REPL running in the container:

```
$ lein repl :connect 8765
```

If all goes well, you should see the prompt:

```
sparkplug.repl.work=>
```

The currently running Spark application context is available via the
`spark-context` var, and the WebUI runs on http://localhost:4050/. When you're
done with the REPL you can hit `^D` (Control + D) to hang up and leave the
container running, or call `(exit!)` to shut it down cleanly and stop the Spark
application.


## Limitations

Currently, you cannot use any dynamically-defined functions. Because these
classes are defined locally, Spark won't be able to deserialize the instances on
the executors.

If you can express your logic in terms of existing higher-order functions, this
will still work:

```clojure
;; won't work
(spark/map->pairs #(vector % 1))

;; will work!
(spark/map->pairs (juxt identity (constantly 1)))
```
