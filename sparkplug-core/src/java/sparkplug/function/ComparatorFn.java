package sparkplug.function;


import clojure.lang.IFn;

import java.util.Comparator;


/**
 * Compatibility wrapper for a `Comparator` of two arguments.
 */
public class ComparatorFn extends SerializableFn implements Comparator<Object> {

    public ComparatorFn(IFn f) {
        super(f);
    }


    @Override
    @SuppressWarnings("unchecked")
    public int compare(Object v1, Object v2) {
        return (int)f.invoke(v1, v2);
    }

}
