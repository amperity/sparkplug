package sparkplug.function;


import clojure.lang.IFn;

import org.apache.spark.api.java.function.Function3;


/**
 * Compatibility wrapper for a Spark `Function3` of three arguments.
 */
public class Fn3 extends SerializableFn implements Function3 {

    public Fn3(IFn f) {
        super(f);
    }


    @Override
    @SuppressWarnings("unchecked")
    public Object call(Object v1, Object v2, Object v3) throws Exception {
        return f.invoke(v1, v2, v3);
    }

}
