package org.gradoop.model.impl.operators.matching.simulation.dual;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.api.operators.UnaryGraphToGraphOperator;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.operators.matching.common.PostProcessor;
import org.gradoop.model.impl.operators.matching.common.PreProcessor;
import org.gradoop.model.impl.operators.matching.common.tuples.MatchingTriple;
import org.gradoop.model.impl.operators.matching.simulation.dual.functions.*;
import org.gradoop.model.impl.operators.matching.simulation.dual.tuples.TripleWithCandidates;
import org.gradoop.model.impl.operators.matching.simulation.dual.tuples.TripleWithDeletions;
import org.gradoop.model.impl.operators.matching.simulation.dual.tuples.TripleWithPredCandidates;

public class DualSimulation
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  implements UnaryGraphToGraphOperator<G, V, E> {

  /**
   * GDL based query string
   */
  private final String query;

  /**
   * True, if the resulting vertices/edges shall have their original data
   * attached.
   */
  private final boolean attachData;

  /**
   * Creates a new operator instance.
   *
   * @param query GDL based query
   */
  public DualSimulation(String query) {
    this(query, true);
  }

  /**
   * Creates a new operator instance.
   *
   * @param query GDL based query
   * @param attachData true, if the original data (label, properties) shall be
   *                   attached to the matching vertices and edges. If false,
   *                   the resulting vertices and edges only contain mandatory
   *                   identifiers (id, source id, target id).
   */
  public DualSimulation(String query, boolean attachData) {
    Preconditions.checkState(!Strings.isNullOrEmpty(query),
      "Query must not be null or empty");
    this.query = query;
    this.attachData = attachData;
  }

  @Override
  public LogicalGraph<G, V, E> execute(LogicalGraph<G, V, E> graph) {

    //--------------------------------------------------------------------------
    // Pre-processing (filter candidates)
    //--------------------------------------------------------------------------

    // TODO: the following is only necessary if diameter(query) > 0

    final DataSet<MatchingTriple> triples = preProcess(graph);

    //--------------------------------------------------------------------------
    // Dual Simulation
    //--------------------------------------------------------------------------

    // TODO: the following is only necessary if diameter(query) > 1

    final DataSet<TripleWithCandidates> triplesWithCandidates =
      getTriplesWithCandidates(triples);

    DataSet<TripleWithCandidates> result = simulate(triplesWithCandidates);

    //--------------------------------------------------------------------------
    // Post-processing (build maximum match graph)
    //--------------------------------------------------------------------------

    return postProcess(graph, result);
  }

  /**
   * Extracts valid triples from the input graph based on the query.
   *
   * @param graph input graph
   *
   * @return triples that have a match in the query
   */
  protected DataSet<MatchingTriple> preProcess(LogicalGraph<G, V, E> graph) {
    // filter vertex-edge-vertex triples by query predicates
    return PreProcessor
      .filterTriplets(graph, query);
  }

  /**
   * Adds query candidates for pre- and succeeding edges to each triple.
   *
   * @param triples matching triples
   *
   * @return triples with corresponding pre- and successors candidates
   */
  protected DataSet<TripleWithCandidates> getTriplesWithCandidates(
    DataSet<MatchingTriple> triples) {
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
    return triplesWithPreCandidates
      .leftOuterJoin(triples)
      .where(2).equalTo(1)
      .with(new TriplesWithCandidates())
      .groupBy(0, 2)
      .combineGroup(new GroupedTriplesWithCandidates())
      .groupBy(0, 2)
      .reduceGroup(new GroupedTriplesWithCandidates());
  }

  /**
   * Executes the dual simulation process on the triples.
   *
   * @param triplesWithCandidates matching triples with candidates
   * @return valid triples according to dual simulation
   */
  protected DataSet<TripleWithCandidates> simulate(
    DataSet<TripleWithCandidates> triplesWithCandidates) {
    // ITERATION HEAD
    IterativeDataSet<TripleWithCandidates> workingSet =
      triplesWithCandidates.iterate(Integer.MAX_VALUE);

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
    return workingSet
      .closeWith(nextWorkingSet, deletions);
  }

  /**
   * Extracts vertices and edges from the query result and constructs a maximum
   * match graph. Optionally attaches original data to vertices and edges.
   *
   * @param graph input graph
   * @param result valid triples after simulation
   * @return maximum match graph
   */
  protected LogicalGraph<G, V, E> postProcess(LogicalGraph<G, V, E> graph,
    DataSet<TripleWithCandidates> result) {
    DataSet<V> matchVertices = attachData ?
      PostProcessor.extractVerticesWithData(result, graph.getVertices()) :
      PostProcessor.extractVertices(result, graph.getConfig().getVertexFactory());

    DataSet<E> matchEdges = attachData ?
      PostProcessor.extractEdgesWithData(result, graph.getEdges()) :
      PostProcessor.extractEdges(result, graph.getConfig().getEdgeFactory());

    return LogicalGraph.fromDataSets(matchVertices, matchEdges, graph.getConfig());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getName() {
    return DualSimulation.class.getName();
  }
}
