package sparkplug.function;


import clojure.lang.IFn;

import org.apache.spark.api.java.function.VoidFunction;


/**
 * Compatibility wrapper for a Spark `VoidFunction` of one argument.
 */
public class VoidFn extends SerializableFn implements VoidFunction {

    public VoidFn(IFn f) {
        super(f);
    }


    @Override
    @SuppressWarnings("unchecked")
    public void call(Object v1) throws Exception {
        f.invoke(v1);
    }

}
