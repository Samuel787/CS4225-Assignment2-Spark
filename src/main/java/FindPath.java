
// hello world
/**
 *  Matric Number: A0182488N
 *  Name: Suther David Samuel
 */
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.*;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import org.graphframes.GraphFrame;
import scala.Tuple2;

import java.io.*;
import java.util.*;

public class FindPath {

    static boolean runOnCluster = true;
    private static double distance(double lat1, double lat2, double lon1, double lon2) {
        final int R = 6371;
        double latDistance = Math.toRadians(lat2 - lat1);
        double lonDistance = Math.toRadians(lon2 - lon1);
        double a = Math.sin(latDistance / 2) * Math.sin(latDistance / 2)
                + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2))
                * Math.sin(lonDistance / 2) * Math.sin(lonDistance / 2);
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
        double distance = R * c * 1000;
        double height = 0;
        distance = Math.pow(distance, 2) + Math.pow(height, 2);
        return Math.sqrt(distance);
    }

    public static void main(String[] args) throws Exception {

        String OSM_FILE_PATH;
        String INPUT_FILE_PATH;
        String ADJ_LIST_OUTPUT_PATH;
        String ROUTE_OUTPUT_PATH;
        String ADJ_LIST_OUTPUT_TEMP_PATH = "output/part1";
        String ROUTE_OUTPUT_TEMP_PATH = "output/part2";
        if (runOnCluster) {
            OSM_FILE_PATH = args[0];
            INPUT_FILE_PATH = args[1];
            ADJ_LIST_OUTPUT_PATH = args[2];
            ROUTE_OUTPUT_PATH = args[3];
        } else {
            OSM_FILE_PATH = System.getProperty("user.dir") + File.separator + args[0];
            INPUT_FILE_PATH = System.getProperty("user.dir") + File.separator + args[1];
            ADJ_LIST_OUTPUT_PATH = System.getProperty("user.dir") + File.separator + args[2];
            ROUTE_OUTPUT_PATH = System.getProperty("user.dir") + File.separator + args[3];
        }

        SparkSession spark = SparkSession.builder().appName("FindPath").master("local[*]").getOrCreate();
        // SparkSession spark = SparkSession.builder().appName("FindPath").getOrCreate();
        FileSystem fs = FileSystem.get(spark.sparkContext().hadoopConfiguration());
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

        Dataset<Row> inputQueriesDF = spark.read().text(INPUT_FILE_PATH);
        List<String> inputQueries = inputQueriesDF.as(Encoders.STRING()).collectAsList();
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

        neighbourNodes.cache();

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
        adjList.coalesce(1).saveAsTextFile(ADJ_LIST_OUTPUT_TEMP_PATH);
        Path adjListTempPath = new Path(ADJ_LIST_OUTPUT_TEMP_PATH + "/part-00000");
        Path adjListPath = new Path(ADJ_LIST_OUTPUT_PATH);
        if (fs.exists(adjListTempPath)) {
            fs.rename(adjListTempPath, adjListPath);
        }

        JavaPairRDD<Long, Long> adjNodesPair = neighbourNodes.flatMapToPair(new PairFlatMapFunction<Tuple2<Long,
                Set<Long>>, Long, Long>() {
            @Override
            public Iterator<Tuple2<Long, Long>> call(Tuple2<Long, Set<Long>> longSetTuple2) throws Exception {
                List<Tuple2<Long, Long>> emitList = new LinkedList<>();
                Iterator<Long> itr = longSetTuple2._2.iterator();
                while (itr.hasNext()) {
                    emitList.add(new Tuple2<>(longSetTuple2._1, itr.next()));
                }
                return emitList.iterator();
            }
        });

        Dataset<Row> gEdges = spark.createDataset(adjNodesPair.collect(), Encoders.tuple(Encoders.LONG(), Encoders.LONG())).toDF("src", "dst").dropDuplicates();
        gEdges.cache();
        StructType nodeSchema = new StructType(new StructField[] {
                new StructField("_id", DataTypes.LongType, false, Metadata.empty()),
                new StructField("_lat", DataTypes.DoubleType, false, Metadata.empty()),
                new StructField("_lon", DataTypes.DoubleType, false, Metadata.empty())
        });
        Dataset<Row> nodeDF = spark.read()
                .format("xml")
                .option("rootTag", "osm")
                .option("rowTag", "node")
                .schema(nodeSchema)
                .load(OSM_FILE_PATH);
        Dataset<Row> gVertices = nodeDF.select(nodeDF.col("_id").as("id"));
        gVertices.cache();
        GraphFrame g = new GraphFrame(gVertices, gEdges);

        Path routeTempPath = new Path(ROUTE_OUTPUT_TEMP_PATH + "/part-00000");
        Path routeOutputPath = new Path(ROUTE_OUTPUT_PATH);
        Path routeTempFolder = new Path(ROUTE_OUTPUT_TEMP_PATH);
        if (fs.exists(routeTempFolder)) {
            fs.delete(routeTempFolder, true);
        }
        List<String> queryResultsList = new ArrayList<>();
        for (String query: inputQueries) {
            String[] points = query.split(" ");
            if (points.length != 2) {
                System.out.println("invalid input detected: " + query);
                continue;
            }
            String fromExpr = "id = '" + points[0] + "'";
            String toExpr = "id = '" + points[1] + "'";
            Dataset<Row> result = g.bfs().fromExpr(fromExpr).toExpr(toExpr).run();
            String currPath = result.toJavaRDD().map(row -> {
                StringBuilder sb = new StringBuilder();
                int numCols = row.size();
                for (int i = 0; i < numCols; i += 2) {
                    if (i == 0) {
                        sb.append(row.get(i).toString().replace("[", "").replace("]", ""));
                    } else {
                        sb.append(" -> " + row.get(i).toString().replace("[", "").replace("]", ""));
                    }
                }
                return sb;
            }).reduce(StringBuilder::append).toString();
            queryResultsList.add(currPath);
            if (fs.exists(routeTempFolder)) {
                fs.delete(routeTempFolder, true);
            }
            spark.createDataset(queryResultsList, Encoders.STRING()).toJavaRDD().coalesce(1).saveAsTextFile(ROUTE_OUTPUT_TEMP_PATH);
            if (fs.exists(routeOutputPath)) {
                fs.delete(routeOutputPath, true);
            }
            if (fs.exists(routeTempPath)) {
                fs.rename(routeTempPath, routeOutputPath);
            }
        }
        spark.stop();
    }
}
