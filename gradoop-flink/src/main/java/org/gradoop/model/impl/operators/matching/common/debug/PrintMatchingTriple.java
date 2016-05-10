package org.gradoop.model.impl.operators.matching.common.debug;

import org.apache.commons.lang3.StringUtils;
import org.gradoop.model.impl.operators.matching.common.tuples.MatchingTriple;

/**
 * Copyright 2016 martin.
 */
public class PrintMatchingTriple extends Printer<MatchingTriple> {

  @Override
  public MatchingTriple map(MatchingTriple t) throws Exception {
    System.out.println(String.format("(%d,%d,%d,[%s])",
      edgeMap.get(t.getEdgeId()),
      vertexMap.get(t.getSourceVertexId()),
      vertexMap.get(t.getTargetVertexId()),
      StringUtils.join(t.getQueryCandidates(), ',')
    ));

    return t;
  }
}
