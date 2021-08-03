package sparkplug.core;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * This is a simple wrapper to call the `union` method on `JavaSparkContext`.
 *
 * It is written in Java because:
 *
 * The non-varargs version of `union` was removed in Spark 3, leaving the varargs version as the
 * only one that is compatible with both Spark 2 and Spark 3. See:
 * <https://issues.apache.org/jira/browse/SPARK-25737>
 *
 * Unfortunately, Clojure is unable to call the varargs version, due to a compiler bug. Doing so
 * will fail with errors such as:
 *
 * IllegalArgumentException: Can't call public method of non-public class: public final
 * org.apache.spark.api.java.JavaPairRDD
 * org.apache.spark.api.java.JavaSparkContextVarargsWorkaround.union(org.apache.spark.api.java.JavaPairRDD[])
 *
 * See: <https://clojure.atlassian.net/browse/CLJ-1243>
 */
public class UnionHelper {
  public static JavaRDD unionJavaRDDs(JavaSparkContext jsc, JavaRDD[] rdds) {
    return jsc.union(rdds);
  }

  public static JavaPairRDD unionJavaPairRDDs(JavaSparkContext jsc, JavaPairRDD[] rdds) {
    return jsc.union(rdds);
  }
}
