package org.gradoop.model.impl.operators.matching.simulation.dual.debug;

import org.apache.commons.lang3.StringUtils;
import org.gradoop.model.impl.operators.matching.common.debug.Printer;
import org.gradoop.model.impl.operators.matching.simulation.dual.tuples
  .TripleWithDeletions;

/**
 * Copyright 2016 martin.
 */
public class PrintTripleWithDeletions extends Printer<TripleWithDeletions> {

  private final boolean isIterative;

  public PrintTripleWithDeletions() {
    this(false);
  }

  public PrintTripleWithDeletions(boolean isIterative) {
    this.isIterative = isIterative;
  }


  @Override
  public TripleWithDeletions map(TripleWithDeletions t) throws Exception {
    System.out.println(String.format("%d: (%d,%d,%d,[%s])",
      isIterative ? getIterationRuntimeContext().getSuperstepNumber() : 0,
      edgeMap.get(t.getEdgeId()),
      vertexMap.get(t.getSourceVertexId()),
      vertexMap.get(t.getTargetVertexId()),
      StringUtils.join(t.getDeletions(), ',')
    ));
    return t;
  }
}
