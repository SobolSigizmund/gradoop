package org.gradoop.model.impl.operators.simulation.dual.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.gradoop.model.impl.operators.simulation.dual.tuples
  .TripleWithCandidates;
import org.gradoop.model.impl.operators.simulation.dual.tuples
  .TripleWithDeletions;

import static org.gradoop.util.Lists.removeEach;

/**
 * Updates the predecessors according to the deletions.
 *
 * Forwarding:
 *
 * f0: edge id
 * f1: source vertex id
 * f2: target vertex id
 * f3: candidates
 * f5: successor candidates
 *
 * Read fields first:
 *
 * f5: successor query candidates
 *
 * Read fields second:
 *
 * f3: deletions
 */
@FunctionAnnotation.ForwardedFieldsFirst("f0;f1;f2;f3;f5")
@FunctionAnnotation.ReadFieldsFirst("f5")
@FunctionAnnotation.ReadFieldsSecond("f3")
public class UpdatedPredecessors implements JoinFunction
  <TripleWithCandidates, TripleWithDeletions, TripleWithCandidates> {

  @Override
  public TripleWithCandidates join(TripleWithCandidates tripleWithCandidates,
    TripleWithDeletions tripleWithDeletions) throws Exception {
    tripleWithCandidates.isUpdated(false);
    if (tripleWithDeletions != null) {
      tripleWithCandidates.isUpdated(removeEach(
              tripleWithCandidates.getSuccessorQueryCandidates(),
              tripleWithDeletions.getDeletions()));
    }
    return tripleWithCandidates;
  }
}
