package sparkplug.kryo;


import clojure.lang.IFn;
import clojure.lang.RT;
import clojure.lang.Symbol;

import com.esotericsoftware.kryo.Kryo;

import org.apache.spark.serializer.KryoRegistrator;


/**
 * Spark interop class to register types for serialization with Kryo.
 */
public class ClassPathRegistrator implements KryoRegistrator {

    private static final IFn resolve = RT.var("clojure.core", "requiring-resolve");
    private static final Symbol symbol = Symbol.intern("sparkplug.kryo", "configure!");


    @Override
    public void registerClasses(Kryo kryo) {

        // Resolve the registration function and invoke it.
        IFn configure = (IFn)resolve.invoke(symbol);

        if (configure == null) {
            throw new RuntimeException("Could not resolve sparkplug configuration function " + symbol);
        }

        configure.invoke(kryo);

    }

}
