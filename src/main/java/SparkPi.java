import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import java.util.ArrayList;
import java.util.List;
/**
 * Computes an approximation to pi
 * Usage: JavaSparkPi [slices]
 * https://github.com/apache/spark/blob/master/pom.xml
 */
public final class SparkPi {
    static boolean runOnCluster = false;
    public static void main(String[] args) throws Exception {
        SparkConf sparkConf = new SparkConf().setAppName("SparkPi");
        int slices = 0;
        JavaSparkContext jsc = null;
        if (!runOnCluster) {
            sparkConf.setMaster("local[*]");
            sparkConf
                    .setJars(new String[] { "target/eduonix_spark-deploy.jar" });
            slices = 10;
            jsc = new JavaSparkContext(sparkConf);
        } else {
            slices = (args.length == 1) ? Integer.parseInt(args[0]) : 2;
            jsc = new JavaSparkContext(sparkConf);
        }
        int n = 100000 * slices;
        List<Integer> l = new ArrayList<Integer>(n);
        for (int i = 0; i < n; i++) {
            l.add(i);
        }
        JavaRDD<Integer> dataSet = jsc.parallelize(l, slices);
        int count = dataSet.map(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer integer) {
                double x = Math.random() * 2 - 1;
                double y = Math.random() * 2 - 1;
                return (x * x + y * y < 1) ? 1 : 0;
            }
        }).reduce(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) {
                return integer + integer2;
            }
        });
        System.out.println("Pi is roughly " + 4.0 * count / n);
        jsc.stop();
    }
}