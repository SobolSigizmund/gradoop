package org.gradoop.model.impl.operators.matching.simulation.dual.functions;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.gradoop.model.impl.operators.matching.common.QueryHandler;
import org.gradoop.model.impl.operators.matching.simulation.dual.tuples.TripleWithCandidates;
import org.gradoop.model.impl.operators.matching.simulation.dual.tuples.TripleWithDeletions;

import java.util.List;

/**
 * Validates incoming {@link TripleWithCandidates} based on their neighborhood.
 * If a triple is not valid for a candidate, that candidate will be removed from
 * the triple. The output {@link TriplesWithDeletions} contains all deleted
 * candidates for a single triple.
 */
@FunctionAnnotation.ForwardedFields("f0;f1;f2")
public class TriplesWithDeletions
  extends RichFlatMapFunction<TripleWithCandidates, TripleWithDeletions> {

  private final String query;

  private transient QueryHandler queryHandler;

  private final TripleWithDeletions reuseTuple;

  public TriplesWithDeletions(String query) {
    this.query = query;
    this.reuseTuple = new TripleWithDeletions();
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    queryHandler = QueryHandler.fromString(query);
  }

  @Override
  public void flatMap(TripleWithCandidates triple,
    Collector<TripleWithDeletions> collector) throws Exception {

    List<Long> deletions = Lists.newArrayList();

    for (Long candidate : triple.getQueryCandidates()) {
      if (!isValid(candidate,
        triple.getPredecessorQueryCandidates(),
        triple.getSuccessorQueryCandidates())) {
        deletions.add(candidate);
      }
    }

    if (deletions.size() > 0) {
      reuseTuple.setEdgeId(triple.getEdgeId());
      reuseTuple.setSourceVertexId(triple.getSourceVertexId());
      reuseTuple.setTargetVertexId(triple.getTargetVertexId());
      reuseTuple.setDeletions(deletions);
      collector.collect(reuseTuple);
    }
  }

  private boolean isValid(Long candidate, List<Long> predecessors, List<Long> successors) {
    return predecessors.containsAll(queryHandler.getPredecessorIds(candidate))
      && successors.containsAll(queryHandler.getSuccessorIds(candidate));
  }
}
