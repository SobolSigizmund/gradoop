package org.gradoop.model.impl.operators.simulation.dual.functions;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.gradoop.model.impl.operators.simulation.dual.tuples
  .TripleWithCandidates;

/**
 * Filters triples that have been updated in the previous iteration.
 *
 * Read fields:
 *
 * f6: update flag
 */
@FunctionAnnotation.ReadFields("f6")
public class UpdatedTriples implements FilterFunction<TripleWithCandidates> {

  @Override
  public boolean filter(TripleWithCandidates triple) throws
    Exception {
    return triple.isUpdated();
  }
}
