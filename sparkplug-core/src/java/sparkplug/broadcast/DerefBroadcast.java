package sparkplug.broadcast;


import clojure.lang.IDeref;

import org.apache.spark.broadcast.Broadcast;

import scala.reflect.ClassTag;


/**
 * This class extends Spark's broadcast type so that it can be used with the
 * Clojure <pre>deref<pre> function and reader macro.
 */
public class DerefBroadcast<T> extends Broadcast<T> implements IDeref {

    public final Broadcast<T> wrapped;


    /**
     * Construct a new DerefBroadcast wrapping the given broadcast value.
     */
    public DerefBroadcast(Broadcast<T> wrapped, Class<T> cls) {
        super(wrapped.id(), ClassTag.apply(cls));
        this.wrapped = wrapped;
    }


    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        } else if (other instanceof DerefBroadcast) {
            DerefBroadcast db = (DerefBroadcast)other;
            return wrapped.equals(db.wrapped);
        } else {
            return false;
        }
    }


    @Override
    public int hashCode() {
        return wrapped.hashCode();
    }


    @Override
    public String toString() {
        return wrapped.toString();
    }


    @Override
    public Object deref() {
        return wrapped.value();
    }


    @Override
    public T getValue() {
        return wrapped.value();
    }


    @Override
    public void doUnpersist(boolean blocking) {
        wrapped.doUnpersist(blocking);
    }


    @Override
    public void doDestroy(boolean blocking) {
        wrapped.doDestroy(blocking);
    }

}
