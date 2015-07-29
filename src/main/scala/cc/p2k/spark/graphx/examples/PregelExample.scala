package cc.p2k.spark.graphx.examples

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by pavel on 22.07.15.
 */
object PregelExample {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Spark Pi").setMaster("local")
    val sc = new SparkContext(conf)


    val vertices: RDD[(VertexId, Double)] = sc.parallelize(Array(
      (1L, 1.0), (2L, 1.0), (3L, 1.0), (4L, 1.0)
    ))

    val edges: RDD[Edge[Double]] = sc.parallelize(Array(
      Edge(1L, 2L, 1.0), Edge(2L, 3L, 1.0),
      Edge(2L, 1L, 1.0), Edge(3L, 2L, 1.0)
    ))

    val graph = Graph(vertices, edges)

    val initGraph = graph
    val initMessage = 1.0

    /**
     * @param x VertexId
     * @param a Double VD
     * @param b Double message type
     * @return VD
     */
    def vertexProgram(x: VertexId, a: Double, b: Double) = a + b

    /**
     * @param edge EdgeTriplet[Double, Double]  EdgeTriplet[VD, ED]
     * @return Iterator[(VertexId, A)]
     */
    def sendMessage(edge: EdgeTriplet[Double, Double]) ={
      if (edge.srcAttr < 3) {
        Iterator((edge.dstId, edge.srcAttr + 1.0))
      } else {
        Iterator.empty
      }
    }

    /**
     * @param a Double VD
     * @param b Double VD
     * @return VD
     */
    def messageCombiner(a: Double, b: Double): Double = a + b

    val res = Pregel(initGraph, initMessage)(vertexProgram, sendMessage, messageCombiner)

    print(res.vertices.collect().mkString("\n"))
    //print(res.edges.collect().mkString("\n"))
  }

}
