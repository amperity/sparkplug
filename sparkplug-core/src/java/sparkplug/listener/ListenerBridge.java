package sparkplug.listener;

import clojure.lang.IFn;
import org.apache.spark.SparkFirehoseListener;
import org.apache.spark.scheduler.SparkListenerEvent;

public class ListenerBridge extends SparkFirehoseListener {

    /**
     * Event listener to call for each event.
     */
    private final IFn listener;

    public ListenerBridge(final IFn listener) {
        this.listener = listener;
    }

    @Override
    public final void onEvent(final SparkListenerEvent event) {
        listener.invoke(event);
    }
}
