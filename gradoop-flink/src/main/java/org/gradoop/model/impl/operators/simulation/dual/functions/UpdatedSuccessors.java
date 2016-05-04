package org.gradoop.model.impl.operators.simulation.dual.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.gradoop.model.impl.operators.simulation.dual.tuples.TripleWithCandidates;
import org.gradoop.model.impl.operators.simulation.dual.tuples.TripleWithDeletions;



import java.util.List;

import static org.gradoop.util.Lists.removeEach;

/**
 * Updates a successor triples according to the deletions.
 *
 * Forwarding:
 *
 * f0: edge id
 * f1: source vertex id
 * f2: target vertex id
 * f3: candidates
 * f4: predecessor candidates
 *
 * Read fields first:
 *
 * f4: predecessor query candidates
 *
 * Read fields second:
 *
 * f3: deletions
 */
@FunctionAnnotation.ForwardedFieldsFirst("f0;f1;f2;f3;f4")
public class UpdatedSuccessors implements JoinFunction
  <TripleWithCandidates, TripleWithDeletions, TripleWithCandidates> {

  @Override
  public TripleWithCandidates join(TripleWithCandidates tripleWithCandidates,
    TripleWithDeletions tripleWithDeletions) throws Exception {
    if (tripleWithDeletions != null) {
      if (removeEach(
        tripleWithCandidates.getPredecessorQueryCandidates(),
        tripleWithDeletions.getDeletions())) {
        tripleWithCandidates.isUpdated(true);
      }
    }
    return tripleWithCandidates;
  }
}
