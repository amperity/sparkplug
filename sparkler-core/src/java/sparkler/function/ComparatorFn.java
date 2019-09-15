package sparkler.function;


import clojure.lang.IFn;

import java.util.Collection;
import java.util.Comparator;


/**
 * Compatibility wrapper for a Spark `Function2` of two arguments.
 */
public class ComparatorFn extends SerializableFn implements Comparator<Object> {

    public ComparatorFn(IFn f) {
        super(f);
    }


    public ComparatorFn(IFn f, Collection<String> namespaces) {
        super(f, namespaces);
    }


    @Override
    @SuppressWarnings("unchecked")
    public int compare(Object v1, Object v2) {
        return (int)f.invoke(v1, v2);
    }

}
