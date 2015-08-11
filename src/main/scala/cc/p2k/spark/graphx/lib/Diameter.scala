package cc.p2k.spark.graphx.lib

import org.apache.spark.Logging
import org.apache.spark.graphx._
import scala.reflect.ClassTag


object Diameter extends Logging {

  type SPMap = Map[VertexId, Int]

  private def makeMap(x: (VertexId, Int)*) = Map(x: _*)

  private def incrementMap(spmap: SPMap): SPMap = spmap.map { case (v, d) => v -> (d + 1) }

  private def addMaps(spmap1: SPMap, spmap2: SPMap): SPMap =
    (spmap1.keySet ++ spmap2.keySet).map {
      k => k -> math.min(spmap1.getOrElse(k, Int.MaxValue), spmap2.getOrElse(k, Int.MaxValue))
    }.toMap

  def apply[VD, ED: ClassTag](graph: Graph[VD, ED]): Int = {

    val spGraph = graph.mapVertices { (vid, attr) =>
      makeMap(vid -> 0)
    }

    val initialMessage = makeMap()

    def vertexProgram(id: VertexId, attr: SPMap, msg: SPMap): SPMap = {
      addMaps(attr, msg)
    }

    def sendMessage(edge: EdgeTriplet[SPMap, _]): Iterator[(VertexId, SPMap)] = {
      val newAttr = incrementMap(edge.dstAttr)
      if (edge.srcAttr != addMaps(newAttr, edge.srcAttr)) Iterator((edge.srcId, newAttr))
      else Iterator.empty
    }

    val resultGraph = Pregel(spGraph, initialMessage)(vertexProgram, sendMessage, addMaps)

    resultGraph.mapVertices((id: VertexId, mapping: SPMap) => {
      mapping.valuesIterator.max
    }).vertices.max()._2
  }
}
