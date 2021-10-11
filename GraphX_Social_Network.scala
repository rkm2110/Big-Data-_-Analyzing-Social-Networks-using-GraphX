import org.apache.spark.sql.SparkSession
import java.io.{BufferedWriter, File, FileWriter}
import org.graphframes.GraphFrame
import org.apache.spark.sql.functions.{desc, lit}


object GraphX_Social_Network {
  def main(args: Array[String]): Unit = {

    if (args.length != 2) {
      println("Error.")
      println("Usage: InputPath OutputPath")
      return
    }

    val spark =
      SparkSession.builder()
        .appName("test").getOrCreate()

    //Start Spark Context
    val sc = spark.sparkContext
    //Read input file
    val df = spark.read.option("delimiter", ",").option("header", "true").csv(args(0))

    //Create Edges
    val GraphEdges = df.withColumnRenamed("FromNodeId", "src").withColumnRenamed("ToNodeId", "dst")

    //Source Vertices
    val src_vertex = df.select("FromNodeId").toDF("id")
    //Destination Vertices
    val dst_vertex = df.select("ToNodeId").toDF("id")
    //Combine all vertices
    val vertex_combo = src_vertex.join(dst_vertex, Seq("id"), "outer")
    //Keep only distinct vertices
    val vertex_combo_distinct = vertex_combo.distinct.toDF

    //Combine vertices and edges to form graph
    val graph_frames = GraphFrame(vertex_combo_distinct, GraphEdges)
    //Put graph in memory
    graph_frames.cache()

    //Get out degrees of nodes
    val out_degree = graph_frames.outDegrees
    //Collect the top 5 nodes with highest out-degree and save it to output text file
    out_degree.orderBy(desc("outDegree")).toDF().limit(5).rdd.saveAsTextFile(args(1) + "/out_degree")

    ///Get in degrees of nodes
    val in_degree = graph_frames.inDegrees
    //Collect the top 5 nodes with highest in-degree and save it to output text file
    in_degree.orderBy(desc("inDegree")).toDF().limit(5).rdd.saveAsTextFile(args(1) + "/in_degree")

    //Get PageRank for the nodes
    val page_ranks = graph_frames.pageRank.resetProbability(0.15).maxIter(10).run()
    //Collect the top 5 nodes with highest page rank and save it to output text file
    page_ranks.vertices.orderBy(desc("pagerank")).select("id", "pagerank").toDF().limit(5).rdd.saveAsTextFile(args(1) + "/page_rank")

    spark.sparkContext.setCheckpointDir("/tmp/checkpoints")
    //Get the connected components
    val conn_comp = graph_frames.connectedComponents.run()
    //Collect the top 5 components with largest number of nodes and save it to output text file
    conn_comp.groupBy("component").count.orderBy(desc("count")).select("component","count").toDF().limit(5).rdd.saveAsTextFile(args(1) + "/connected_components")

    //Get the triangle counts for each vertex
    val tri_count = graph_frames.triangleCount.run()
    //Collect the top 5 vertices with largest number of triangle counts and save it to output text file
    tri_count.orderBy(desc("count")).select("id", "count").toDF().limit(5).rdd.saveAsTextFile(args(1) + "/triangle_count")
  }
}