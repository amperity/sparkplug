package sparkplug.function;


import clojure.lang.Compiler;
import clojure.lang.IFn;
import clojure.lang.RT;
import clojure.lang.Symbol;

import java.io.IOException;
import java.io.InvalidObjectException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
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


    private void writeObject(ObjectOutputStream out) throws IOException {
        // Attempt to derive the needed Clojure namespace from the function's class name.
        String namespace = Compiler.demunge(this.f.getClass().getName()).split("/")[0];
        out.writeObject(namespace);
        out.writeObject(this.f);
    }


    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        String namespace = (String)in.readObject();
        Symbol namespaceSym = Symbol.intern(namespace);
        // TODO: Try optimizing by doing an unsynchronized resolve first.
        synchronized (RT.REQUIRE_LOCK) {
            RT.var("clojure.core", "require").invoke(namespaceSym);
        }
        this.f = (IFn)in.readObject();
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
