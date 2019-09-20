package sparkplug.function;


import clojure.lang.IFn;

import org.apache.spark.api.java.function.Function2;


/**
 * Compatibility wrapper for a Spark `Function2` of two arguments.
 */
public class Fn2 extends SerializableFn implements Function2 {

    public Fn2(IFn f) {
        super(f);
    }


    @Override
    @SuppressWarnings("unchecked")
    public Object call(Object v1, Object v2) throws Exception {
        return f.invoke(v1, v2);
    }

}
