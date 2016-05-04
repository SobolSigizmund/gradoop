package org.gradoop.model.impl.operators.simulation.dual;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple1;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.api.operators.UnaryGraphToGraphOperator;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.functions.epgm.Id;
import org.gradoop.model.impl.functions.epgm.SourceId;
import org.gradoop.model.impl.functions.join.RightSide;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.operators.simulation.dual.functions.*;
import org.gradoop.model.impl.operators.simulation.common.functions.MatchingEdges;
import org.gradoop.model.impl.operators.simulation.common.functions.MatchingPairs;
import org.gradoop.model.impl.operators.simulation.common.functions.MatchingTriples;
import org.gradoop.model.impl.operators.simulation.common.functions.MatchingVertices;
import org.gradoop.model.impl.operators.simulation.common.tuples.MatchingPair;
import org.gradoop.model.impl.operators.simulation.common.tuples.MatchingTriple;
import org.gradoop.model.impl.operators.simulation.dual.tuples.TripleWithCandidates;
import org.gradoop.model.impl.operators.simulation.dual.tuples.TripleWithDeletions;
import org.gradoop.model.impl.operators.simulation.dual.tuples.TripleWithPredCandidates;

public class DualSimulation
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  implements UnaryGraphToGraphOperator<G, V, E> {

  private final String query;

  public DualSimulation(String query) {
    Preconditions.checkState(!Strings.isNullOrEmpty(query),
      "Query must not be null or empty");
    this.query = query;
  }

  @Override
  public LogicalGraph<G, V, E> execute(LogicalGraph<G, V, E> graph) {

    //--------------------------------------------------------------------------
    // Only necessary for debug
    //--------------------------------------------------------------------------

//    DataSet<Tuple2<GradoopId, Integer>> vertexMapping = graph.getVertices()
//      .map(new GradoopIdToDebugId<V>("id"));
//    DataSet<Tuple2<GradoopId, Integer>> edgeMapping = graph.getEdges()
//      .map(new GradoopIdToDebugId<E>("id"));

    //--------------------------------------------------------------------------
    // Pre-processing (filter candidates)
    //--------------------------------------------------------------------------

    // filter vertices by query predicates
    DataSet<V> filteredVertices = graph.getVertices()
      .filter(new MatchingVertices<V>(query));

    // TODO: the following is only necessary if diameter(query) > 0

    // filter edges by query predicates
    DataSet<E> filteredEdges = graph.getEdges()
      .filter(new MatchingEdges<E>(query));

    // filter vertex-edge-tuples by query predicates
    DataSet<MatchingPair<V, E>> pairs = filteredVertices
      .join(filteredEdges)
      .where(new Id<V>()).equalTo(new SourceId<E>())
      .with(new MatchingPairs<V, E>(query));

    // filter vertex-edge-vertex triples by query predicates
    final DataSet<MatchingTriple> triples = pairs
      .join(filteredVertices)
      .where("f1.targetId").equalTo(new Id<V>())
      .with(new MatchingTriples<V, E>(query));

    // TODO: the following is only necessary if diameter(query) > 1

    //--------------------------------------------------------------------------
    // Dual Simulation
    //--------------------------------------------------------------------------

    // append predecessors
    DataSet<TripleWithPredCandidates> triplesWithPreCandidates = triples
      .leftOuterJoin(triples)
      .where(1).equalTo(2)
      .with(new TriplesWithPreCandidates())
      .groupBy(0, 1)
      .combineGroup(new GroupedTriplesWithPredCandidates())
      .groupBy(0, 1)
      .reduceGroup(new GroupedTriplesWithPredCandidates());

    // append successors
    final DataSet<TripleWithCandidates> triplesWithCandidates = triplesWithPreCandidates
      .leftOuterJoin(triples)
      .where(2).equalTo(1)
      .with(new TriplesWithCandidates())
      .groupBy(0, 2)
      .combineGroup(new GroupedTriplesWithCandidates())
      .groupBy(0, 2)
      .reduceGroup(new GroupedTriplesWithCandidates());

    // ITERATION HEAD
    IterativeDataSet<TripleWithCandidates> workingSet =
      triplesWithCandidates.iterate(5);

    // ITERATION BODY

    // validate neighborhood of each triple
    DataSet<TripleWithDeletions> deletions = workingSet
      .filter(new UpdatedTriples())
      .flatMap(new TriplesWithDeletions(query));

    // build next working set
    DataSet<TripleWithCandidates> nextWorkingSet = workingSet
      // update candidates
      .leftOuterJoin(deletions)
      .where(0).equalTo(0) // edgeId == edgeId
      .with(new UpdatedCandidates())
      // filter non-zero candidate triples
      .filter(new ValidTriples())
      // update predecessors
      .leftOuterJoin(deletions)
      .where(2).equalTo(1) // targetVertexId == sourceVertexId
      .with(new UpdatedPredecessors())
      // update successors
      .leftOuterJoin(deletions)
      .where(1).equalTo(2) // sourceVertexId == targetVertexId
      .with(new UpdatedSuccessors());

    // ITERATION FOOTER
    DataSet<TripleWithCandidates> result = workingSet.closeWith(nextWorkingSet, deletions);

    //--------------------------------------------------------------------------
    // Post-processing (build maximum match graph)
    //
    // The join in this step could me made optional, since the user may only be
    // interested in the respective vertex and edge identifiers.
    //--------------------------------------------------------------------------

    DataSet<V> matchVertices = result
      .flatMap(new VertexIds())
      .distinct()
      .join(graph.getVertices())
      .where(0).equalTo(new Id<V>())
      .with(new RightSide<Tuple1<GradoopId>, V>());

    DataSet<E> matchEdges = result
      .<Tuple1<GradoopId>>project(0)
      .join(graph.getEdges())
      .where(0).equalTo(new Id<E>())
      .with(new RightSide<Tuple1<GradoopId>, E>());

//    try {
//      result
//        .map(new DebugTripleWithCandidates())
//        .withBroadcastSet(vertexMapping, DebugTripleWithCandidates.VERTEX_MAPPING)
//        .withBroadcastSet(edgeMapping, DebugTripleWithCandidates.EDGE_MAPPING)
//        .print();
//    } catch (Exception e) {
//      e.printStackTrace();
//    }

    return LogicalGraph.fromDataSets(
      matchVertices, matchEdges, graph.getConfig());
  }

  @Override
  public String getName() {
    return DualSimulation.class.getName();
  }
}
