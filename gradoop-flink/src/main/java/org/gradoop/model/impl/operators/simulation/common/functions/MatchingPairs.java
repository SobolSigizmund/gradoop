package org.gradoop.model.impl.operators.simulation.common.functions;

import org.apache.flink.api.common.functions.RichFlatJoinFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.operators.simulation.common.query.QueryHandler;
import org.gradoop.model.impl.operators.simulation.common.tuples.MatchingPair;
import org.s1ck.gdl.model.Edge;
import org.s1ck.gdl.model.Vertex;

import java.util.Collection;

import static org.gradoop.model.impl.operators.simulation.common.matching.EntityMatcher.match;

/**
 * Filters a {@link MatchingPair} based on its occurrence in the given GDL
 * query pattern.
 *
 * @param <V> EPGM vertex type
 * @param <E> EPGM edge type
 */
public class MatchingPairs<V extends EPGMVertex, E extends EPGMEdge> extends
  RichFlatJoinFunction<V, E, MatchingPair<V, E>> {

  private final String query;

  private transient QueryHandler queryHandler;

  private final MatchingPair<V, E> reuseTuple;

  public MatchingPairs(final String query) {
    this.query = query;
    this.reuseTuple = new MatchingPair<>();
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    queryHandler = QueryHandler.fromString(query);
  }

  @Override
  public void join(V sourceVertex, E edge, Collector<MatchingPair<V, E>> collector) throws
    Exception {

    Collection<Vertex> queryVertices = queryHandler
      .getVerticesByLabel(sourceVertex.getLabel());

    for (Vertex queryVertex : queryVertices) {
      Collection<Edge> queryEdges =
        queryHandler.getEdgesBySourceVertexId(queryVertex.getId());
      if (queryEdges != null) {
        for (Edge queryEdge : queryEdges) {
          boolean match = match(sourceVertex, queryVertex) && match(edge, queryEdge);
          System.out.println(String.format(
            "(%d:%s)-[%2d:%s]->() == (%d:%s)-[%2d:%s]->() => %s",
            sourceVertex.getPropertyValue("id").getInt(), sourceVertex.getLabel(),
            edge.getPropertyValue("id").getInt(), edge.getLabel(),
            queryVertex.getId(), queryVertex.getLabel(),
            queryEdge.getId(), queryEdge.getLabel(),
            match));
          if (match) {
            reuseTuple.setVertex(sourceVertex);
            reuseTuple.setEdge(edge);
            collector.collect(reuseTuple);
          }
        }
      }
    }
  }
}
