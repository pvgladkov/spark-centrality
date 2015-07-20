package cc.p2k.spark.graphx.lib

import org.apache.spark.graphx.lib.ShortestPaths
import org.apache.spark.{Logging}
import org.apache.spark.graphx._
import scala.language.postfixOps


object HarmonicCentrality extends Logging {

  def personalizedHarmonicCentrality(vertexId: Long, graph: Graph[Double, Double]): Double ={
    val initialGraph = graph.mapVertices(
      (id, _) => {
        if (id == vertexId) 0.0
        else Double.PositiveInfinity
      }
    )

    val vp = (id: VertexId, dist: Double, newDist:Double) => math.min(dist, newDist)

    def mergeMsg(a: Double, b: Double) = math.min(a,b)

    def sendMsg(triplet: EdgeTriplet[Double, Double]) = {
      if (triplet.srcAttr + triplet.attr < triplet.dstAttr) {
        Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
      } else {
        Iterator.empty
      }
    }

    val paths = initialGraph.pregel(Double.PositiveInfinity)(vp, sendMsg, mergeMsg)

    val hr = paths.vertices
      .map[Double](
        tuple => {
          if (tuple._1 == vertexId) 0
          else 1 / tuple._2
        }
      )
      .reduce( (a, b) => a + b )

    hr
  }

  def harmonicCentrality(graph: Graph[Double, Double]): Graph[Double, Double] = {
    val vertices = graph.vertices.collect().map(tuple => tuple._1)
    val paths = ShortestPaths.run(graph, vertices)

    val hc = paths.mapVertices((id, sMap) => {
      var s = 0.0
      for ((k,v) <- sMap){
        if (v != 0) s += 1.0/v
      }
      s
    })

    hc
  }

}
