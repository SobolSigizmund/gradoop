package org.gradoop.model.impl.operators.matching.simulation.dual.functions;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.gradoop.model.impl.operators.matching.simulation.dual.tuples.TripleWithCandidates;

/**
 * Filters triples that have more than zero candidates left.
 *
 * Read fields:
 *
 * f3: query candidates
 */
@FunctionAnnotation.ReadFields("f3")
public class ValidTriples implements FilterFunction<TripleWithCandidates> {

  @Override
  public boolean filter(TripleWithCandidates triple) throws
    Exception {
    return triple.getQueryCandidates().size() > 0;
  }
}
