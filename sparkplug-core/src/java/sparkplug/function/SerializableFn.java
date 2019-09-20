package sparkplug.function;


import clojure.lang.IFn;

import java.io.Serializable;


/**
 * Base class for function classes built for interop with Spark and Scala.
 */
public abstract class SerializableFn implements Serializable {

    protected IFn f;


    /**
     * Default empty constructor.
     */
    private SerializableFn() {
    }


    /**
     * Construct a new serializable wrapper for the function with an explicit
     * set of required namespaces.
     *
     * @param fn Clojure function to wrap
     */
    protected SerializableFn(IFn fn) {
        this.f = fn;
    }

}
