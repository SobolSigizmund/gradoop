package org.gradoop.model.impl.operators.matching.simulation.dual.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.gradoop.model.impl.operators.matching.common.tuples.MatchingTriple;
import org.gradoop.model.impl.operators.matching.simulation.dual.tuples
  .TripleWithCandidates;
import org.gradoop.model.impl.operators.matching.simulation.dual.tuples
  .TripleWithPredCandidates;

import java.util.ArrayList;

/**
 * Copyright 2016 martin.
 */
public class TriplesWithCandidates implements
  JoinFunction<TripleWithPredCandidates, MatchingTriple, TripleWithCandidates> {

  @Override
  public TripleWithCandidates join(
    TripleWithPredCandidates tripleWithPred,
    MatchingTriple successor) throws Exception {

    TripleWithCandidates result = new TripleWithCandidates();
    result.setEdgeId(tripleWithPred.getEdgeId());
    result.setSourceVertexId(tripleWithPred.getSourceVertexId());
    result.setTargetVertexId(tripleWithPred.getTargetVertexId());
    result.setQueryCandidates(tripleWithPred.getQueryCandidates());
    result.setPredecessorQueryCandidates(tripleWithPred.getPredecessorQueryCandidates());
    result.isUpdated(true);

    if (successor != null) {
      result.setSuccessorQueryCandidates(successor.getQueryCandidates());
    } else {
      result.setSuccessorQueryCandidates(new ArrayList<Long>());
    }
    return result;
  }
}
