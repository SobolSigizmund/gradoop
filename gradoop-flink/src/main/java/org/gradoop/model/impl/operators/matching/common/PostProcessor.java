package org.gradoop.model.impl.operators.matching.common;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMEdgeFactory;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.api.EPGMVertexFactory;
import org.gradoop.model.impl.functions.epgm.EdgeFromId;
import org.gradoop.model.impl.functions.epgm.Id;
import org.gradoop.model.impl.functions.epgm.VertexFromId;
import org.gradoop.model.impl.functions.join.RightSide;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.operators.matching.simulation.dual.functions.VertexIds;
import org.gradoop.model.impl.operators.matching.simulation.dual.tuples.TripleWithCandidates;

/**
 * Provides methods for post-processing query results.
 */
public class PostProcessor {

  public static DataSet<Tuple1<GradoopId>> extractVertexIds(
    DataSet<TripleWithCandidates> result) {
    return result.flatMap(new VertexIds()).distinct();
  }

  public static DataSet<Tuple1<GradoopId>> extractEdgeIds(
    DataSet<TripleWithCandidates> result) {
    return result.project(0);
  }

  public static DataSet<Tuple3<GradoopId, GradoopId, GradoopId>> extractTriples(
    DataSet<TripleWithCandidates> result) {
    return result.project(0, 1, 2);
  }

  public static <V extends EPGMVertex>
  DataSet<V> extractVertices(DataSet<TripleWithCandidates> result,
    EPGMVertexFactory<V> vertexFactory) {
    return extractVertexIds(result).map(new VertexFromId<>(vertexFactory));
  }

  public static <E extends EPGMEdge>
  DataSet<E> extractEdges(DataSet<TripleWithCandidates> result,
    EPGMEdgeFactory<E> edgeFactory) {
    return extractTriples(result).map(new EdgeFromId<>(edgeFactory));
  }

  public static <V extends EPGMVertex>
  DataSet<V> extractVerticesWithData(DataSet<TripleWithCandidates> result,
    DataSet<V> inputVertices) {
    return extractVertexIds(result)
      .join(inputVertices)
      .where(0).equalTo(new Id<V>())
      .with(new RightSide<Tuple1<GradoopId>, V>());
  }

  public static <E extends EPGMEdge>
  DataSet<E> extractEdgesWithData(DataSet<TripleWithCandidates> result,
    DataSet<E> inputEdges) {
    return extractEdgeIds(result)
      .join(inputEdges)
      .where(0).equalTo(new Id<E>())
      .with(new RightSide<Tuple1<GradoopId>, E>());
  }

}
