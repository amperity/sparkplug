package sparkler.function;


import clojure.lang.IFn;
import clojure.lang.IPersistentMap;
import clojure.lang.Keyword;
import clojure.lang.Ref;
import clojure.lang.RT;
import clojure.lang.Symbol;
import clojure.lang.Var;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
     *
     * TODO is this accurate?
     * Inheriting classes must also have an empty constructor to be properly
     * serializable.
     */
    private SerializableFn() {
    }


    /**
     * Construct a new serializable wrapper for the function.
     *
     * Required namespaces will be automatically discovered by walking the
     * object.
     *
     * @param fn Clojure function to wrap
     */
    protected SerializableFn(IFn fn) {
        this.f = fn;
        Set<String> references = findVarNamespaces(f);
        this.namespaces = new ArrayList<String>(references);
        Collections.sort(this.namespaces);
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
        this.namespaces = new ArrayList<String>(namespaces);
        Collections.sort(this.namespaces);
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
            // Write the function class name.
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


    /**
     * Walk the given object to find namespaces referenced by the value.
     *
     * @param obj a Clojure value to walk
     * @return a set of namespace strings
     */
    public static Set<String> findVarNamespaces(Object obj) {
        Set<String> references = new HashSet<String>();
        Set<Object> visited = new HashSet<Object>();
        findVarNamespaces(references, visited, obj);
        references.remove("clojure.core");
        return references;
    }


    /**
     * Walk the given object to find namespaces referenced by the value.
     * Namespace references are added to the given set.
     *
     * @param references set to add discovered namespaces to
     * @param visited set of values which have already been walked
     * @param obj object to walk
     */
    private static void findVarNamespaces(Set<String> references, Set<Object> visited, Object obj) {
        // Simple types that can't have namespace references.
        if (obj == null
                || obj instanceof Boolean
                || obj instanceof String
                || obj instanceof Number
                || obj instanceof Keyword
                || obj instanceof Symbol
                || obj instanceof Ref) {
            return;
        }

        // See if we've already visited this object.
        if (visited.contains(obj)) {
            return;
        }

        visited.add(obj);

        // Vars directly represent a namespace dependency.
        if (obj instanceof Var) {
            Var v = (Var)obj;
            references.add(v.ns.getName().toString());
            return;
        }

        // Special case maps and records to traverse over their contents in
        // addition to their fields.
        if (obj instanceof IPersistentMap) {
            IPersistentMap map = (IPersistentMap)obj;
            for (Object entry : map) {
                findVarNamespaces(references, visited, entry);
            }
        }

        // Traverse the fields of the value for more references.
        for (Field field : obj.getClass().getDeclaredFields()) {
            // Only traverse static fields of maps. (why?)
            if (!(obj instanceof IPersistentMap) || Modifier.isStatic(field.getModifiers())) {
                boolean accessible = field.isAccessible();
                try {
                    field.setAccessible(true);
                    Object val = field.get(obj);
                    // TODO why only these two types?
                    if (val instanceof IFn || val instanceof IPersistentMap) {
                        findVarNamespaces(references, visited, val);
                    }
                } catch (IllegalAccessException ex) {
                    logger.warn("Error resolving namespaces references in " + obj, ex);
                } finally {
                    try {
                        field.setAccessible(accessible);
                    } catch (Exception ex) {
                        // ignored
                    }
                }
            }
        }
    }

}
