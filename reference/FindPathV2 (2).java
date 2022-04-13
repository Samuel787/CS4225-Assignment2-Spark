
// Matric Number: A0200618R
// Name: Koh Zhe Hao
// FindPath.java
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.*;
import org.apache.spark.sql.*;
import org.graphframes.GraphFrame;

import scala.Serializable;
import scala.collection.mutable.WrappedArray;

import java.io.IOException;
import java.util.*;

public class FindPath {
    public static void main(String[] args) {
        SparkSession.Builder sparkBuilder = SparkSession.builder();
        SparkSession spark;
        spark = sparkBuilder.master("local[*]").appName("FindPath").getOrCreate();

        String mapPath = args[0];
        String nodePath = args[1];
        String adjPath = args[2];
        String outPath = args[3];

        dfr dfr = spark.read().format("com.databricks.spark.xml");
        Dataset<Row> ohf = dfr.option("rowTag", "node").load(mapPath);
        Dataset<Row> orf = dfr.option("rowTag", "way").load(mapPath);
        Dataset<Row> nhf = spark.read().text(nodePath);

        GraphFrame graph = buildRoadGraph(osmNodeDf, orf, spark);

        getNeighborOutput(graph, outAdjMapPath);
        getPathOutput(graph, nodePairDf, outPath, spark);

        try {
            cleanUp(outAdjMapPath, "/temp1", spark);
            cleanUp(outPath, "/temp2", spark);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void getNeighborOutput(GraphFrame graph, String outAdjMapPath) {
        List<Row> edgeList = graph.edges().toJavaRDD().collect();
        JavaRDD<String> content = graph.vertices().toJavaRDD().map(r -> {
            long currId = r.getLong(0);
            Set<String> neighbors = new TreeSet<>();

            for (Row e : edgeList) {
                if (e.getLong(e.fieldIndex("src")) == currId) {
                    neighbors.add(String.valueOf(e.getLong(e.fieldIndex("dst"))));
                }
            }
            List<String> sortedNbs = new ArrayList<>(neighbors);
            Collections.sort(sortedNbs);

            return currId + " " + String.join(" ", sortedNbs);
        });

        content.coalesce(1).saveAsTextFile("output/temp1");
    }

    private static void getPathOutput(GraphFrame graph, Dataset<Row> nodePairDf, String outPath, SparkSession spark) {
        Row[] rows = (Row[]) nodePairDf.collect();
        List<String> content = new ArrayList<>();
        for (Row r : rows) {
            String[] nodePair = r.getString(0).split(" ");
            Dataset<Row> paths = graph.bfs().fromExpr("id = " + nodePair[0]).toExpr("id = " + nodePair[1]).run();

            content.add(paths.toJavaRDD().map(p -> {
                StringBuilder res = new StringBuilder();
                int toIndex = p.fieldIndex("to");
                for (int i = 0; i <= toIndex; i += 2) {
                    Row node = p.getAs(i);
                    res.append(node.getLong(0));
                    if (i < toIndex - 1)
                        res.append(" -> ");
                }

                return res;
            }).reduce(StringBuilder::append).toString());
        }

        spark.createDataset(content, Encoders.STRING()).toJavaRDD().coalesce(1).saveAsTextFile("output/temp2");
    }

    private static GraphFrame buildRoadGraph(Dataset<Row> osmNodeDf, Dataset<Row> osmRoadDf, SparkSession spark) {
        JavaRDD<Node> allNodeJavaRDD = osmNodeDf.toJavaRDD().map((Function<Row, Node>) row -> {
            double lat = row.getDouble(row.fieldIndex("_lat"));
            double lon = row.getDouble(row.fieldIndex("_lon"));
            long id = row.getLong(row.fieldIndex("_id"));
            return new Node(id, lat, lon);
        });

        List<Node> nodeList = allNodeJavaRDD.collect();
        HashMap<Long, Node> nodeMap = new HashMap<>();

        for (Node node : nodeList) {
            nodeMap.put(node.id, node);
        }

        JavaRDD<Edge> edgeJavaRDD = osmRoadDf.toJavaRDD().filter((Function<Row, Boolean>) row -> {
            WrappedArray<Row> tags = row.getAs(row.fieldIndex("tag"));

            if (tags == null) {
                return false;
            }

            Row[] tagArr = (Row[]) tags.array();
            boolean isHighway = false;

            for (Row value : tagArr) {
                if (value.get(1).equals("highway")) {
                    isHighway = true;
                    break;
                }
            }

            return isHighway;
        }).flatMap((FlatMapFunction<Row, Edge>) row -> {
            WrappedArray<Row> tags = row.getAs(row.fieldIndex("tag"));
            WrappedArray<Row> nodes = row.getAs(row.fieldIndex("nd"));

            Row[] tagArr = (Row[]) tags.array();
            boolean isOneway = false;

            for (Row value : tagArr) {
                if (value.get(1).equals("oneway") && value.get(2).equals("yes")) {
                    isOneway = true;
                    break;
                }
            }

            Row[] nodeArr = (Row[]) nodes.array();
            Set<Edge> edges = new TreeSet<>();

            for (int i = 0; i < nodeArr.length - 1; i++) {
                long src = nodeArr[i].getLong(1);
                long dst = nodeArr[i + 1].getLong(1);
                Node srcNode = nodeMap.get(src);
                Node dstNode = nodeMap.get(dst);
                double dist = getDistance(srcNode, dstNode);

                edges.add(new Edge(src, dst, dist));

                if (!isOneway) {
                    edges.add(new Edge(dst, src, dist));
                }
            }

            return edges.iterator();
        });

        JavaRDD<Node> nodeJavaRDD = edgeJavaRDD.flatMap(r -> List.of(nodeMap.get(r.src), nodeMap.get(r.dst)).iterator())
                .distinct();

        Dataset<Row> nodeDataset = spark.createDataFrame(nodeJavaRDD, Node.class);
        Dataset<Row> edgeDataset = spark.createDataFrame(edgeJavaRDD, Edge.class);

        return GraphFrame.apply(nodeDataset, edgeDataset);
    }

    private static void cleanUp(String src, String temp, SparkSession spark) throws IOException {
        FileSystem fs = FileSystem.get(spark.sparkContext().hadoopConfiguration());
        Path srcPath = new Path("output" + temp + "/part-00000");
        Path dstPath = new Path(src);

        if (fs.exists(srcPath)) {
            fs.rename(srcPath, dstPath);
        }
    }

    private static double getDistance(Node n1, Node n2) {
        return distance(n1.lat, n2.lat, n1.lon, n2.lon);
    }

    // From:
    // https://stackoverflow.com/questions/3694380/calculating-distance-between-two-points-using-latitude-longitude
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

    public static class Node implements Serializable, Comparable<Node> {
        private final Long id;
        private final Double lat, lon;
        private Boolean isInHighway = false;

        Node(long id, double lat, double lon) {
            this.id = id;
            this.lat = lat;
            this.lon = lon;
        }

        @Override
        public int compareTo(Node node) {
            if (this.id == node.id) {
                return 0;
            } else
                return this.id - node.id > 0 ? 1 : -1;
        }

        public void setInHighway(Boolean inHighway) {
            this.isInHighway = inHighway;
        }

        public Boolean getInHighway() {
            return isInHighway;
        }

        public Double getLat() {
            return lat;
        }

        public Double getLon() {
            return lon;
        }

        public Long getId() {
            return id;
        }
    }

    public static class Edge implements Comparable<Edge>, Serializable {
        private final Double length;
        private final Long src, dst;

        Edge(long src, long dst, double length) {
            this.length = length;
            this.src = src;
            this.dst = dst;
        }

        @Override
        public int compareTo(Edge edge) {
            if (this.src == edge.src && this.dst == edge.dst) {
                return 0;
            } else
                return (int) (this.src - edge.src);
        }

        public Double getLength() {
            return length;
        }

        public Long getDst() {
            return dst;
        }

        public Long getSrc() {
            return src;
        }
    }
}
