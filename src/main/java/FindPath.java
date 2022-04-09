import org.apache.hadoop.shaded.org.ehcache.impl.internal.concurrent.ConcurrentHashMap;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.*;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.graphframes.GraphFrame;
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

        Dataset nodeDF = spark.read()
                .format("xml")
                .option("rootTag", "osm")
                .option("rowTag", "node")
                .schema(nodeSchema)
                .load(OSM_FILE_PATH);

        JavaPairRDD<Long, Integer> nodeRDD = nodeDF.toJavaRDD().mapToPair(new PairFunction<Row, Long, Integer>() {
            @Override
            public Tuple2<Long, Integer> call(Row row) throws Exception {
                return new Tuple2<Long, Integer>((Long)row.get(0), 1);
            }
        });

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
                    // if (!isOneWay) {
                        Set<Long> nSet = new HashSet<>();
                        nSet.add(prevNode);
                        result.add(new Tuple2<Long, Set<Long>>(currNode, nSet));
                    // }
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

        JavaPairRDD<Long, Tuple2<Integer, Optional<Set<Long>>>> neighbourNodesFull = nodeRDD.leftOuterJoin(neighbourNodes).sortByKey();
        neighbourNodesFull.cache();

//        JavaPairRDD<Long, Long> adjNodes = neighbourNodesFull.flatMapToPair(new PairFlatMapFunction<Tuple2<Long,
//                Tuple2<Integer, Optional<Set<Long>>>>, Long, Long>() {
//            @Override
//            public Iterator<Tuple2<Long, Long>> call(Tuple2<Long, Tuple2<Integer, Optional<Set<Long>>>> longTuple2Tuple2) throws Exception {
//                List<Tuple2<Long, Long>> emitList = new LinkedList<>();
//                if (longTuple2Tuple2._2._2.isPresent()) {
//                    Iterator<Long> itr = longTuple2Tuple2._2._2.get().iterator();
//                    while(itr.hasNext()) {
//                        emitList.add(new Tuple2<>(longTuple2Tuple2._1, itr.next()));
//                    }
//                }
//                return emitList.iterator();
//            }
//        });
//        adjNodes.cache();

        JavaRDD<String> adjListResult = neighbourNodesFull.map(new Function<Tuple2<Long, Tuple2<Integer, Optional<Set<Long>>>>, String>() {
            @Override
            public String call(Tuple2<Long, Tuple2<Integer, Optional<Set<Long>>>> longTuple2Tuple2) throws Exception {
                StringBuilder outputString = new StringBuilder();
                outputString.append(longTuple2Tuple2._1);
                if (longTuple2Tuple2._2._2.isPresent()) {
                    List<Long> list = new ArrayList<>(longTuple2Tuple2._2._2.get());
                    Collections.sort(list);
                    for (Long i: list) {
                        outputString.append(" " + i);
                    }
                }
                return outputString.toString();
            }
        });

        adjListResult.saveAsTextFile("out/adjmap.txt");




//
//        neighbourNodes.cache();
//        JavaPairRDD<Long, Long> adjNodesPairs = neighbourNodes.flatMapToPair(new PairFlatMapFunction<Tuple2<Long, Set<Long>>,
//                Long, Long>() {
//            @Override
//            public Iterator<Tuple2<Long, Long>> call(Tuple2<Long, Set<Long>> longSetTuple2) throws Exception {
//                List<Tuple2<Long, Long>> emitList = new LinkedList<>();
//                Iterator<Long> itr = longSetTuple2._2.iterator();
//                while (itr.hasNext()) {
//                    emitList.add(new Tuple2<>(longSetTuple2._1, itr.next()));
//                }
//                return emitList.iterator();
//            }
//        });
//        adjNodesPairs.cache();
//
//        Dataset<Row> gInput = spark.createDataset(adjNodesPairs.collect(), Encoders.tuple(Encoders.LONG(), Encoders.LONG())).toDF("src", "dst");
//        JavaRDD<String> adjMapOutput = neighbourNodes.map(new Function<Tuple2<Long, Set<Long>>, String>() {
//            @Override
//            public String call(Tuple2<Long, Set<Long>> longSetTuple2) throws Exception {
//                List<Long> list = new ArrayList<Long>(longSetTuple2._2);
//                Collections.sort(list);
//                StringBuilder outputString = new StringBuilder();
//                outputString.append(longSetTuple2._1);
//                for (Long i: list) {
//                    outputString.append(" " + i);
//                }
//                return outputString.toString();
//            }
//        });
//
//



        // comment out the code below temporarily
        // -- haven't reached this stage yet ---

//
//        gInput.withColumn("property", functions.lit(1));
//        gInput.show(10);
//
////        Dataset meow = gInput.join(nodeDF, gInput.col("src").equalTo(nodeDF.col("_id")), "left_outer")
////                            .join(nodeDF, gInput.col("dst").equalTo(nodeDF.col("_id")), "left_outer");
////        meow.withColumn("song", distance(meow.col("hi"), ))
////        System.out.println("This is meow: ");
////        meow.show(10);
////
////        Dataset woof = nodeDF.select(nodeDF.col("*"))
////                .where(nodeDF.col("_id").equalTo())
//        // generate the adjacency list
//        adjMapOutput.saveAsTextFile("out/adjmap.txt");
//
//        // Generating the shortest paths
//        GraphFrame g = new GraphFrame(nodeDF.select(nodeDF.col("_id").as("id")), gInput);
//        Long start = 2391320044L;
//        Long end = 9170734738L;
//        Dataset result = g.bfs().fromExpr("id = '2391320044'").toExpr("id = '9170734738'").run();
//
//        System.out.println("Final result");
//        result.show(20);
//        //        ArrayList<Object> input = new ArrayList<>();
////        input.add(2391320044L);
////        input.add(9170734738L);
////        Dataset<Row> results1 = g.shortestPaths().landmarks(input).run();
////        results1.select("id", "distances").show();
//

        spark.stop();
    }
}
