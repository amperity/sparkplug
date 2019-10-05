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

    /**
     * Wrapper class to efficiently ensure the configuration function is only
     * loaded once.
     */
    private static class Singleton {

        private static final IFn configure;

        static {
            IFn resolve = RT.var("clojure.core", "requiring-resolve");
            Symbol name = Symbol.intern("sparkplug.kryo", "load-configuration");
            IFn loader = (IFn)resolve.invoke(name);
            configure = (IFn)loader.invoke();
        }

    }


    @Override
    public void registerClasses(Kryo kryo) {

        IFn configure = Singleton.configure;

        if (configure == null) {
            throw new RuntimeException("Could not construct kryo configuration function!");
        }

        configure.invoke(kryo);

    }

}
