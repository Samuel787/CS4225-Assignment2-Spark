import algebra.lattice.Bool;
import dk.brics.automaton.Datatypes;
import org.apache.hadoop.shaded.org.ehcache.impl.internal.concurrent.ConcurrentHashMap;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.*;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import org.graphframes.GraphFrame;
import scala.Tuple2;
import scala.collection.mutable.WrappedArray;

import javax.xml.crypto.Data;
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

        StructType waySchema = new StructType(new StructField[] {
                new StructField("nd", DataTypes.createArrayType(
                        DataTypes.createStructType(new StructField[] {
                                new StructField("_ref", DataTypes.LongType, true, Metadata.empty())
                        })
                ), false, Metadata.empty()),
                new StructField("tag", DataTypes.createArrayType(
                        DataTypes.createStructType(new StructField[] {
                                new StructField("_k", DataTypes.StringType, false, Metadata.empty()),
                                new StructField("_v", DataTypes.StringType, false, Metadata.empty())
                        })
                ), true, Metadata.empty()),
        });

        Dataset<Row> wayDF = spark.read()
                .format("xml")
                .option("rootTag", "osm")
                .option("rowTag", "way")
                .schema(waySchema)
                .load(OSM_FILE_PATH);

        Dataset<Row> wayDFModified = wayDF.select(wayDF.col("nd._ref"), wayDF.col("tag._k").as("tag_keys"), wayDF.col("tag._v").as("tag_vals"))
                .where(functions.array_contains(wayDF.col("tag._k"), "highway"));

        JavaPairRDD<Long, Set<Long>> neighbourNodes = wayDFModified.toJavaRDD().flatMapToPair(row -> {
            List<Tuple2<Long, Set<Long>>> result = new LinkedList<>();
            int numNodes = row.getList(0).size();
            if (numNodes == 0) {
                return result.iterator();
            }
            Boolean isOneWay = false;
            int posOneWay = row.getList(1).indexOf("oneway");
            if (posOneWay >= 0) {
                isOneWay = row.getList(2).get(posOneWay).equals("yes");
            }
            Long prevNode = -1L;
            Long currNode;
            for (int i = 0; i < numNodes; i++) {
                currNode = (Long) row.getList(0).get(i);
                Set<Long> forwardSet = new HashSet<>(1);
                if (prevNode != -1L) {
                    forwardSet.add(currNode);
                    result.add(new Tuple2<>(prevNode, forwardSet));
                    if (!isOneWay) {
                        Set<Long> backwardSet = new HashSet<>(1);
                        backwardSet.add(prevNode);
                        result.add(new Tuple2<>(currNode, backwardSet));
                    }
                }
                // very last node
                if (i == (numNodes - 1)) {
                    result.add(new Tuple2<>(currNode, new HashSet<>(0)));
                }
                prevNode = currNode;
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

        JavaRDD<String> adjList = neighbourNodes.sortByKey().map(new Function<Tuple2<Long, Set<Long>>, String>() {
            @Override
            public String call(Tuple2<Long, Set<Long>> longSetTuple2) throws Exception {
                StringBuilder sb = new StringBuilder();
                sb.append(longSetTuple2._1);
                if (longSetTuple2._2.size() > 0) {
                    List<Long> neighbours = new ArrayList<>(longSetTuple2._2);
                    Collections.sort(neighbours);
                    for (Long neighbour: neighbours) {
                        sb.append(" " + neighbour);
                    }
                }
                return sb.toString();
            }
        });

        adjList.saveAsTextFile("out/adjmap.txt");

        spark.stop();
    }
}
