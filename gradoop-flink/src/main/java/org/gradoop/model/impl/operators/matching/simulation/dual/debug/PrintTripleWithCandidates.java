package org.gradoop.model.impl.operators.matching.simulation.dual.debug;

import org.apache.commons.lang3.StringUtils;
import org.gradoop.model.impl.operators.matching.common.debug.Printer;
import org.gradoop.model.impl.operators.matching.simulation.dual.tuples.TripleWithCandidates;

/**
 * Copyright 2016 martin.
 */
public class PrintTripleWithCandidates extends Printer<TripleWithCandidates> {

  private final boolean isIterative;

  public PrintTripleWithCandidates() {
    this(false);
  }

  public PrintTripleWithCandidates(boolean isIterative) {
    this.isIterative = isIterative;
  }

  @Override
  public TripleWithCandidates map(TripleWithCandidates t) throws Exception {
    System.out.println(String.format("%d: (%d,%d,%d,[%s],[%s],[%s],%s)",
      isIterative ? getIterationRuntimeContext().getSuperstepNumber() : 0,
      edgeMap.get(t.getEdgeId()),
      vertexMap.get(t.getSourceVertexId()),
      vertexMap.get(t.getTargetVertexId()),
      StringUtils.join(t.getQueryCandidates(), ','),
      StringUtils.join(t.getPredecessorQueryCandidates(), ','),
      StringUtils.join(t.getSuccessorQueryCandidates(), ','),
      t.isUpdated()
    ));

    return t;
  }
}
