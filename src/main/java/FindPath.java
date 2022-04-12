import org.apache.spark.SparkConf;
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

    static boolean runOnCluster = false;
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
        String OSM_FILE_PATH = System.getProperty("user.dir") + File.separator + args[0];
        String INPUT_FILE_PATH = System.getProperty("user.dir") + File.separator + args[1];
        String ADJ_LIST_OUTPUT_PATH = System.getProperty("user.dir") + File.separator + args[2];
        String ROUTE_OUTPUT_PATH = System.getProperty("user.dir") + File.separator + args[3];

        try {
            int idx = ADJ_LIST_OUTPUT_PATH.lastIndexOf("/");
            File outputDir;
            if (idx != -1) {
                outputDir = new File(ADJ_LIST_OUTPUT_PATH.substring(0, idx));
                outputDir.mkdirs();
            }
            idx = ROUTE_OUTPUT_PATH.lastIndexOf("/");
            if (idx != -1) {
                outputDir = new File(ADJ_LIST_OUTPUT_PATH.substring(0, idx));
                outputDir.mkdirs();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        SparkConf sparkConf = new SparkConf().setAppName("FindPath");
        SparkSession spark = SparkSession.builder().appName("FindPath").master("local[*]").getOrCreate();
        System.out.println("This is the file path: " + OSM_FILE_PATH);
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
        List<String> adjListRows = adjList.collect();
        File mapFile = new File(ADJ_LIST_OUTPUT_PATH);
        if (!mapFile.exists()) {
            mapFile.createNewFile();
        }
        try {
            BufferedWriter bw = new BufferedWriter(new FileWriter(ADJ_LIST_OUTPUT_PATH, true));
            for (String row: adjListRows) {
                bw.append(row);
                bw.newLine();
            }
            bw.close();
        } catch (IOException e) {
            System.out.println("Error occurred while writing the adj list result file");
            e.printStackTrace();
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
        gEdges.show(10);
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
        gVertices.show(10);
        GraphFrame g = new GraphFrame(gVertices, gEdges);
        g.cache();

        ArrayList<String> inputLines = new ArrayList<>();
        try {
            File inputFile = new File(INPUT_FILE_PATH);
            FileReader fr = new FileReader(inputFile);
            BufferedReader br = new BufferedReader(fr);
            String line;
            while ((line = br.readLine()) != null) {
                System.out.println("This line has been read from the input file: " + line);
                inputLines.add(line);
            }
            fr.close();
        } catch (IOException e) {
            System.out.println("Error occurred while reading the input file");
            e.printStackTrace();
        }

        for (String query: inputLines) {
            File routeFile = new File(ROUTE_OUTPUT_PATH);
            if (!routeFile.exists()) {
                routeFile.createNewFile();
            }
            String[] points = query.split(" ");
            if (points.length != 2) {
                System.out.println("invalid input detected: " + query);
                try {
                    BufferedWriter bw = new BufferedWriter(new FileWriter(ROUTE_OUTPUT_PATH, true));
                    bw.append("");
                    bw.newLine();
                    bw.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
                continue;
            }
            String fromExpr = "id = '" + points[0] + "'";
            String toExpr = "id = '" + points[1] + "'";
            Dataset<Row> result = g.bfs().fromExpr(fromExpr).toExpr(toExpr).run();
            JavaRDD<String> path = result.toJavaRDD().map(row -> {
                StringBuilder sb = new StringBuilder();
                int numCols = row.size();
                for (int i = 0; i < numCols; i += 2) {
                    if (i == 0) {
                        sb.append(row.get(i).toString());
                    } else {
                        sb.append(" -> " + row.get(i).toString());
                    }
                }
                return sb.toString();
            });
            List<String> resultRows = path.collect();
            if (resultRows.size() == 0) {
                System.out.println("invalid input detected: " + query);

                try {
                    BufferedWriter bw = new BufferedWriter(new FileWriter(ROUTE_OUTPUT_PATH, true));
                    bw.append("");
                    bw.newLine();
                    bw.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
                continue;
            }
            String finalResult = resultRows.get(0).replace("[", "").replace("]", "");
            try {
                BufferedWriter bw = new BufferedWriter(new FileWriter(ROUTE_OUTPUT_PATH, true));
                bw.append(finalResult);
                bw.newLine();
                bw.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        spark.stop();
    }
}
