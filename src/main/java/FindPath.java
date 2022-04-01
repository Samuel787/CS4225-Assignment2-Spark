import org.apache.hadoop.shaded.org.ehcache.impl.internal.concurrent.ConcurrentHashMap;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.*;

public class FindPath {

    static boolean runOnCluster = false;

    // From: https://stackoverflow.com/questions/3694380/calculating-distance-between-two-points-using-latitude-longitude
    private static double distance(double lat1, double lat2, double lon1, double lon2) {
        final int R = 6371; // Radius of the earth
        double latDistance = Math.toRadians(lat2 - lat1);
        double lonDistance = Math.toRadians(lon2 - lon1);
        double a = Math.sin(latDistance / 2) * Math.sin(latDistance / 2)
                + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2))
                * Math.sin(lonDistance / 2) * Math.sin(lonDistance / 2);
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
        double distance = R * c * 1000; // convert to meters
        double height = 0; // For this assignment, we assume all locations have the same height.
        distance = Math.pow(distance, 2) + Math.pow(height, 2);
        return Math.sqrt(distance);
    }

    public static void main(String[] args) throws Exception {
        SparkConf sparkConf = new SparkConf().setAppName("FindPath");
        SparkSession spark = SparkSession.builder().appName("FindPath").master("local[*]").getOrCreate();
        String OSM_FILE_PATH = "D:\\NUSY4S2\\BigDataProj\\Assignment2\\data\\NUS.osm";
        StructType nodeSchema = new StructType(new StructField[] {
                new StructField("_id", DataTypes.LongType, false, Metadata.empty()),
                new StructField("_lat", DataTypes.DoubleType, false, Metadata.empty()),
                new StructField("_lon", DataTypes.DoubleType, false, Metadata.empty())
        });

        Map<Long, Set<Long>> adjacencyMap = new ConcurrentHashMap<>();

        Dataset nodeDF = spark.read()
                .format("xml")
                .option("rootTag", "osm")
                .option("rowTag", "node")
                .schema(nodeSchema)
                .load(OSM_FILE_PATH);

        nodeDF.toJavaRDD().foreach(new VoidFunction<Row>() {
            @Override
            public void call(Row row) throws Exception {
                if (!adjacencyMap.containsKey(row.get(0))) {
                    adjacencyMap.put((Long) row.get(0), new HashSet<>());
                }
            }
        });
        nodeDF.show(10);


        Dataset wayDF = spark.read()
                .format("xml")
                .option("rootTag", "osm")
                .option("rowTag", "way")
                .load(OSM_FILE_PATH);

        Dataset<Row> highwayDF = wayDF
                .select(wayDF.col("nd._ref"), wayDF.col("tag._k").as("tag"))
                .where(functions.array_contains(wayDF.col("tag._k"), "highway"));

        JavaPairRDD<Long, Set<Long>> neighbourNodes = highwayDF.toJavaRDD().flatMapToPair(row -> {
            List<Tuple2<Long, Set<Long>>> result = new LinkedList<>();
            Boolean isOneWay = row.getList(1).contains("oneway");
            int numNodes = row.getList(0).size();
            if (numNodes > 0) {
                Long prevNode = (Long) row.getList(0).get(0);
                for (int i = 1; i < numNodes; i++) {
                    Long currNode = (Long) row.getList(0).get(i);
                    Set<Long> mSet = new HashSet<>();
                    mSet.add(currNode);
                    result.add(new Tuple2<Long, Set<Long>>(prevNode, mSet));
                    if (!isOneWay) {
                        Set<Long> nSet = new HashSet<>();
                        mSet.add(prevNode);
                        result.add(new Tuple2<Long, Set<Long>>(currNode, nSet));
                    }
                    prevNode = currNode;
                }
            }
            return result.iterator();
        }).reduceByKey(new Function2<Set<Long>, Set<Long>, Set<Long>>() {
            @Override
            public Set<Long> call(Set<Long> setA, Set<Long> setB) throws Exception {
                Set<Long> resultSet = new HashSet<>();
                Iterator<Long> itr = setA.iterator();
                while (itr.hasNext()) {
                    resultSet.add(itr.next());
                }
                itr = setB.iterator();
                while (itr.hasNext()) {
                    resultSet.add(itr.next());
                }
                return resultSet;
            }
        });

        JavaRDD<String> adjMapOutput = neighbourNodes.map(new Function<Tuple2<Long, Set<Long>>, String>() {
            @Override
            public String call(Tuple2<Long, Set<Long>> longSetTuple2) throws Exception {
                List<Long> list = new ArrayList<Long>(longSetTuple2._2);
                Collections.sort(list);
                StringBuilder outputString = new StringBuilder();
                outputString.append(longSetTuple2._1);
                for (Long i: list) {
                    outputString.append(" " + i);
                }
                return outputString.toString();
            }
        });

        adjMapOutput.saveAsTextFile("out/adjmap.txt");
        spark.stop();

    }
}
