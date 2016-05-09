package org.gradoop.model.impl.operators.matching.simulation.dual.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.gradoop.model.impl.operators.matching.common.tuples.MatchingTriple;
import org.gradoop.model.impl.operators.matching.simulation.dual.tuples.TripleWithPredCandidates;

import java.util.ArrayList;

/**
 * Takes two {@link MatchingTriple} where the left is the successor edge of the
 * second. The function updates the predecessor candidates field of the left
 * triple using the candidate values of the right triple.
 */
@FunctionAnnotation.ForwardedFieldsFirst("0;1;2;3")
@FunctionAnnotation.ForwardedFieldsSecond("3->4")
public class TriplesWithPreCandidates implements
  JoinFunction<MatchingTriple, MatchingTriple, TripleWithPredCandidates> {

  private final TripleWithPredCandidates reuseTuple;

  public TriplesWithPreCandidates() {
    reuseTuple = new TripleWithPredCandidates();
  }

  @Override
  public TripleWithPredCandidates join(MatchingTriple triple,
    MatchingTriple predecessor) throws Exception {
    reuseTuple.setEdgeId(triple.getEdgeId());
    reuseTuple.setSourceVertexId(triple.getSourceVertexId());
    reuseTuple.setTargetVertexId(triple.getTargetVertexId());
    reuseTuple.setQueryCandidates(triple.getQueryCandidates());
    if (predecessor != null) {
      reuseTuple.setPredecessorQueryCandidates(predecessor.getQueryCandidates());
    } else {
      reuseTuple.setPredecessorQueryCandidates(new ArrayList<Long>());
    }
    return reuseTuple;
  }
}
