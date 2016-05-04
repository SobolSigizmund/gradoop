package org.gradoop.model.impl.operators.simulation.dual.tuples;

import org.apache.flink.api.java.tuple.Tuple5;
import org.gradoop.model.impl.id.GradoopId;

import java.util.List;

/**
 * Represents a triple including the candidates for its predecessors.
 *
 * f0: edge id
 * f1: source vertex id
 * f2: target vertex id
 * f3: query candidates
 * f4: predecessor query candidates
 */
public class TripleWithPredCandidates extends
  Tuple5<GradoopId, GradoopId, GradoopId, List<Long>, List<Long>> {

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

  public List<Long> getQueryCandidates() {
    return f3;
  }

  public void setQueryCandidates(List<Long> queryCandidates) {
    f3 = queryCandidates;
  }

  public List<Long> getPredecessorQueryCandidates() {
    return f4;
  }

  public void setPredecessorQueryCandidates(List<Long> preQueryCandidates) {
    f4 = preQueryCandidates;
  }
}
