package org.gradoop.model.impl.operators.simulation.dual.tuples;

import org.apache.flink.api.java.tuple.Tuple4;
import org.gradoop.model.impl.id.GradoopId;

import java.util.List;

/**
 * Copyright 2016 martin.
 */
public class TripleWithDeletions extends
  Tuple4<GradoopId, GradoopId, GradoopId, List<Long>> {

  public GradoopId getEdgeId() {
    return f0;
  }

  public void setEdgeId(GradoopId edgeId) {
    f0 = edgeId;
  }

  public GradoopId getSourceVertexId() {
    return f1;
  }

  public void setSourceVertexId(GradoopId sourceVertexId) {
    f1 = sourceVertexId;
  }

  public GradoopId getTargetVertexId() {
    return f2;
  }

  public void setTargetVertexId(GradoopId targetVertexId) {
    f2 = targetVertexId;
  }

  public List<Long> getDeletions() {
    return f3;
  }

  public void setDeletions(List<Long> deletions) {
    f3 = deletions;
  }

}
