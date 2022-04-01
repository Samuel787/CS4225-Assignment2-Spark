import org.apache.hadoop.shaded.org.ehcache.impl.internal.concurrent.ConcurrentHashMap;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
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

        JavaPairRDD<Long, Long> neighbourNodes = highwayDF.toJavaRDD().flatMapToPair(row -> {
            List<Tuple2<Long, Long>> result = new LinkedList<>();
            Boolean isOneWay = row.getList(1).contains("oneway");
            int numNodes = row.getList(0).size();
            if (numNodes > 0) {
                Long prevNode = (Long) row.getList(0).get(0);
                for (int i = 1; i < numNodes; i++) {
                    Long currNode = (Long) row.getList(0).get(i);
                    result.add(new Tuple2<>(prevNode, currNode));
                    if (!isOneWay) {
                        result.add(new Tuple2<>(currNode, prevNode));
                    }
                    prevNode = currNode;
                }
            }
            return result.iterator();
        });

        JavaPairRDD<Long, Set<Long>> x = neighbourNodes.reduceByKey(new Function2<Long, Long, Set<Long>>() {

            @Override
            public Set<Long> call(Long aLong, Long aLong2) throws Exception {
                return null;
            }
        });

        neighbourNodes.foreach(x -> {
            System.out.println("this is for each x: " + x);
        });


        highwayDF.show(10);
//        JavaPairRDD<Long, Long> neighbourNodes = highwayDF.flatMap(new FlatMapFunction<Row, Tuple2<Long, Long>>() {
//            @Override
//            public Iterator<Tuple2<Long, Long>> call(Row row) throws Exception {
//                return null;
//            }
//
////            @Override
////            public Iterator<Tuple2<>> call(Row row) throws Exception {
////                List<Tuple2<Long, Long>> x = new LinkedList<>();
////                return x.iterator();
////            }
//        });

        // below somewhat works
//        highwayDF.toJavaRDD().foreach(new VoidFunction<Row>() {
//            @Override
//            public void call(Row o) throws Exception {
//                Boolean isOneWay = o.getList(1).contains("oneway");
//                int numNodes = o.getList(0).size();
//                if (numNodes > 0) {
//                    Long prevNode = (Long) o.getList(0).get(0);
//                    for (int i = 1; i < numNodes; i++) {
//                        Long currNode = (Long) o.getList(0).get(i);
//                        if (!adjacencyMap.containsKey(prevNode)) {
//                            adjacencyMap.put(prevNode, new HashSet<>());
//                        }
//                        adjacencyMap.get(prevNode).add(currNode);
//                        if (!isOneWay) {
//                            if (!adjacencyMap.containsKey(currNode)) {
//                                adjacencyMap.put(currNode, new HashSet<>());
//                            }
//                            adjacencyMap.get(currNode).add(prevNode);
//                        }
//                        prevNode = currNode;
//                        System.out.println(adjacencyMap.size());
//                    }
//                }
//            }
//        });
        spark.stop();
        // print the adjacency map
        for (Long key: adjacencyMap.keySet()) {
            StringBuilder result = new StringBuilder();
            for (Long val: adjacencyMap.get(key)) {
                result.append(val + " ");
            }
            System.out.println(key + " --> " + result);
        }
    }

    public static void printAdjacencyMap(Map<Long, Set<Long>> adjacencyMap) {
        System.out.println("Function is called : " + adjacencyMap.size());

        for (Long key: adjacencyMap.keySet()) {
            StringBuilder result = new StringBuilder();
            for (Long val: adjacencyMap.get(key)) {
                result.append(val + " ");
            }
            System.out.println(key + " --> " + result);
        }
    }
}
