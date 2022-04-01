import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

public class WordCount0 {
    public static void main(String[] args) {
        String logFile = "D:\\Program Files\\spark-3.0.0-bin-without-hadoop\\README.md";
        SparkSession spark = SparkSession.builder().appName("Simple Word Count Application").master("local[*]").getOrCreate();
        Dataset<String> logData = spark.read().textFile(logFile).cache();

        long numAs = logData.filter((FilterFunction<String>)(s -> s.contains("a"))).count();
        long numBs = logData.filter((FilterFunction<String>) (s-> s.contains("b"))).count();

        System.out.println("Lines with a: " + numAs + ", lines with Bs: " + numBs);

        spark.stop();
    }
}

//D:\Program Files\spark-3.0.0-bin-without-hadoop
