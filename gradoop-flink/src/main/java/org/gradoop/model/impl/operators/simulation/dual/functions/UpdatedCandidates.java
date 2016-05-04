package org.gradoop.model.impl.operators.simulation.dual.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.gradoop.model.impl.operators.simulation.dual.tuples.TripleWithCandidates;
import org.gradoop.model.impl.operators.simulation.dual.tuples.TripleWithDeletions;

/**
 * Removes the deleted candidates from the candidates.
 *
 * Forwarding:
 *
 * f0: edge id
 * f1: source vertex id
 * f2: target vertex id
 * f4: predecessor candidates
 * f5: successor candidates
 * f6: update flag
 *
 * Read fields first:
 *
 * f3: query candidates
 *
 * Read fields second:
 *
 * f3: deletions
 */
@FunctionAnnotation.ForwardedFieldsFirst("f0;f1;f2;f4;f6")
@FunctionAnnotation.ReadFieldsFirst("f3")
@FunctionAnnotation.ReadFieldsSecond("f3")
public class UpdatedCandidates implements JoinFunction
  <TripleWithCandidates, TripleWithDeletions, TripleWithCandidates> {

  @Override
  public TripleWithCandidates join(TripleWithCandidates tripleWithCandidates,
    TripleWithDeletions tripleWithDeletions) throws Exception {
    if (tripleWithDeletions != null) {
      tripleWithCandidates.getQueryCandidates()
        .removeAll(tripleWithDeletions.getDeletions());
    }
    tripleWithCandidates.isUpdated(false);
    return tripleWithCandidates;
  }
}
