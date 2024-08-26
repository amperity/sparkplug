package sparkplug.function;


import clojure.lang.Compiler;
import clojure.lang.IFn;
import clojure.lang.Keyword;
import clojure.lang.RT;
import clojure.lang.Symbol;
import clojure.lang.Var;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

import java.io.IOException;
import java.io.InvalidObjectException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Base class for function classes built for interop with Spark and Scala.
 *
 * This class is designed to be serialized across computation boundaries in a
 * manner compatible with Spark and Kryo, while ensuring that required code is
 * loaded upon deserialization.
 */
public abstract class SerializableFn implements Serializable {

    private static final Logger logger = LoggerFactory.getLogger(SerializableFn.class);
    private static final Var require = RT.var("clojure.core", "require");

    protected IFn f;
    protected List<String> namespaces;


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
     * @param namespaces collection of namespaces required
     */
    protected SerializableFn(IFn fn, Collection<String> namespaces) {
        this.f = fn;
        List<String> namespaceColl = new ArrayList<String>(namespaces);
        Collections.sort(namespaceColl);
        this.namespaces = Collections.unmodifiableList(namespaceColl);
    }


    /**
     * Safely access the value of a field on the given object.
     *
     * @param obj Instance to access a field on
     * @param field Reflective field to access
     * @return the value of the field, or nil on failure
     */
    public static Object accessField(Object obj, Field field) {
        try {
            if (!field.isAccessible()) {
                field.setAccessible(true);
            }
            return field.get(obj);
        } catch (Exception ex) {
            logger.trace("Failed to access field " + field.toString() + ": " + ex.getClass().getName());
            return null;
        }
    }


    /**
     * Walk a value to convert any deserialized booleans back into the
     * canonical java.lang.Boolean values.
     *
     * @param obj Object to walk references of
     */
    private void fixBooleans(Object obj) {
        // Short-circuit objects which can't have nested values to fix.
        if ((obj == null)
                || (obj instanceof Boolean)
                || (obj instanceof String)
                || (obj instanceof Number)
                || (obj instanceof Keyword)
                || (obj instanceof Symbol)
                || (obj instanceof Var)) {
            return;
        }

        // For collection-like objects, just traverse their elements.
        if (obj instanceof Iterable) {
            for (Object el : (Iterable)obj) {
                fixBooleans(el);
            }
            return;
        }

        // Otherwise, look at the object's fields and try to fix any booleans
        // we find and traverse further.
        for (Field field : obj.getClass().getDeclaredFields()) {
            if (!Modifier.isStatic(field.getModifiers())) {
                Object value = accessField(obj, field);
                if (value instanceof Boolean) {
                    Boolean canonical = ((Boolean)value).booleanValue() ? Boolean.TRUE : Boolean.FALSE;
                    try {
                        field.set(obj, canonical);
                    } catch (IllegalAccessException ex) {
                        logger.warn("Failed to set boolean field " + field.toString());
                    }
                } else {
                    fixBooleans(value);
                }
            }
        }
    }


    /**
     * Serialize the function to the provided output stream.
     * An unspoken part of the `Serializable` interface.
     *
     * @param out stream to write the function to
     */
    private void writeObject(ObjectOutputStream out) throws IOException {
        try {
            logger.trace("Serializing " + f);
            // Write the function class name
            // This is only used for debugging
            out.writeObject(f.getClass().getName());
            // Write out the referenced namespaces.
            out.writeInt(namespaces.size());
            for (String ns : namespaces) {
                out.writeObject(ns);
            }
            // Write out the function itself.
            out.writeObject(f);
        } catch (IOException ex) {
            logger.error("Error serializing function " + f, ex);
            throw ex;
        } catch (RuntimeException ex){
            logger.error("Error serializing function " + f, ex);
            throw ex;
        }
    }


    /**
     * Deserialize a function from the provided input stream.
     * An unspoken part of the `Serializable` interface.
     *
     * @param in stream to read the function from
     */
    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        String className = "";
        try {
            // Read the function class name.
            className = (String)in.readObject();
            logger.trace("Deserializing " + className);
            // Read the referenced namespaces and load them.
            int nsCount = in.readInt();
            this.namespaces = new ArrayList<String>(nsCount);
            for (int i = 0; i < nsCount; i++) {
                String ns = (String)in.readObject();
                namespaces.add(ns);
                requireNamespace(ns);
            }
            // Read the function itself.
            this.f = (IFn)in.readObject();
            fixBooleans(this.f);
        } catch (IOException ex) {
            logger.error("IO error deserializing function " + className, ex);
            throw ex;
        } catch (ClassNotFoundException ex) {
            logger.error("Class error deserializing function " + className, ex);
            throw ex;
        } catch (RuntimeException ex) {
            logger.error("Error deserializing function " + className, ex);
            throw ex;
        }
    }


    /**
     * Load the namespace specified by the given symbol.
     *
     * @param namespace string designating the namespace to load
     */
    private static void requireNamespace(String namespace) {
        try {
            logger.trace("(require " + namespace + ")");
            synchronized (RT.REQUIRE_LOCK) {
                Symbol sym = Symbol.intern(namespace);
                require.invoke(sym);
            }
        } catch (Exception ex) {
            logger.warn("Error loading namespace " + namespace, ex);
        }
    }

}
