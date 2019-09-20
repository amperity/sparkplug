package sparkplug.function;


import clojure.lang.IFn;

import org.apache.spark.api.java.function.Function;


/**
 * Compatibility wrapper for a Spark `Function` of one argument.
 */
public class Fn1 extends SerializableFn implements Function {

    public Fn1(IFn f) {
        super(f);
    }


    @Override
    @SuppressWarnings("unchecked")
    public Object call(Object v1) throws Exception {
        return f.invoke(v1);
    }

}
