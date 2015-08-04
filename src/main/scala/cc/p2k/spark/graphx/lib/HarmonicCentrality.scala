package cc.p2k.spark.graphx.lib

import org.apache.spark.Logging
import org.apache.spark.graphx._
import collection.mutable
import scala.language.postfixOps
import com.twitter.algebird._


object HarmonicCentrality extends Logging {

  /** маппинг соседей по расстоянию */
  type NMap = mutable.HashMap[Int, HLL]

  /**
   * Harmonic Centrality for node
   * @param vertexId VertexId
   * @param graph Graph
   * @return
   */
  def personalizedHarmonicCentrality(vertexId: VertexId, graph: Graph[Double, Double]): Double ={
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

  /**
   * @param graph Graph
   * @param maxDistance Int
   * @return
   */
  def harmonicCentrality(graph: Graph[Double, Int], maxDistance: Int = 6): Graph[NMap, Int] = {

    val BIT_SIZE = 12

    val hll = new HyperLogLogMonoid(BIT_SIZE)

    val initGraph: Graph[NMap, Int] = graph.mapVertices(
      (id: VertexId, v: Double) => mutable.HashMap[Int, HLL](
        (0, hll.create(id.toString.getBytes))
      )
    )

    val initMessage = new mutable.HashMap[Int, HLL]() {
      override def default(key:Int) = new HyperLogLogMonoid(BIT_SIZE).zero
    }

    def incrementNMap(p: NMap): NMap = p.map {
      case (v, d) if v < maxDistance => (v + 1) -> d
    }

    /**
     * @param x VertexId
     * @param vertexValue Double VD
     * @param message Double message type
     * @return VD
     */
    def vertexProgram(x: VertexId, vertexValue: NMap, message: NMap) = {
      println(x + " rec " + message)
      println(x + " has val: " + vertexValue)
      for ((distance, _hll) <- message) {
        val op = vertexValue.get(distance)
        if (op.isEmpty){
          vertexValue(distance) = new HyperLogLogMonoid(BIT_SIZE).zero
        }
        vertexValue(distance) += _hll
      }
      println("res val for " + x + " : "+ vertexValue)
      vertexValue
    }

    /**
     * @param edge EdgeTriplet  EdgeTriplet[VD, ED]
     * @return Iterator[(VertexId, A)]
     */
    def sendMessage(edge: EdgeTriplet[NMap, Int]): Iterator[(VertexId, NMap)] ={
      println(edge.srcId + " before send " + edge.srcAttr)
      val newAttr = incrementNMap(edge.srcAttr)
      println(edge.srcId + " send " + newAttr + " to " + edge.dstId)
      if (edge.dstAttr != newAttr) {
        Iterator((edge.dstId, newAttr))
      } else {
        Iterator.empty
      }
    }

    /**
     * @param a Double VD
     * @param b Double VD
     * @return VD
     */
    def messageCombiner(a: NMap, b: NMap): NMap = {
      //println("a: " + a)
      //println("b: " + b)
      val result = new mutable.HashMap[Int, HLL]()
      for (i <- 1 to 6){
        val a_hll = a.get(i)
        val b_hll = b.get(i)
        result(i) = new HyperLogLogMonoid(BIT_SIZE).zero
        if (a_hll.isDefined){
          //println("a: " + a(i).estimatedSize + " i: " + i)
          result(i) += a(i)
        }
        if (b_hll.isDefined){
          //println("b: " + b(i).estimatedSize + " i: " + i)
          result(i) += b(i)
        }
      }
      result
    }

    Pregel(initGraph, initMessage, activeDirection = EdgeDirection.In)(
      vertexProgram, sendMessage, messageCombiner)
  }

  private def calculateForNode(distances: NMap) = {
    var harmonic = 0.0
    for ((k, v) <- distances){
      val current = v.estimatedSize
      val prev = distances(k-1).estimatedSize
      harmonic += BigDecimal((current - prev) / k)
        .setScale(5, BigDecimal.RoundingMode.HALF_UP)
        .toDouble
    }


  }

}
