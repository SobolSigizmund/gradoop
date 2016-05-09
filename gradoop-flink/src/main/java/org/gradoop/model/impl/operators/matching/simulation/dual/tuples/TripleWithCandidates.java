package org.gradoop.model.impl.operators.matching.simulation.dual.tuples;

import org.apache.flink.api.java.tuple.Tuple7;
import org.gradoop.model.impl.id.GradoopId;

import java.util.List;

/**
 * Represents a triple including the candidates for its predecessors and
 * successors.
 *
 * f0: edge id
 * f1: source vertex id
 * f2: target vertex id
 * f3: query candidates
 * f4: predecessor query candidates
 * f5: successor query candidates
 * f6: updated flag
 */
public class TripleWithCandidates extends Tuple7
  <GradoopId, GradoopId, GradoopId, List<Long>, List<Long>, List<Long>, Boolean> {

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

  public void setPredecessorQueryCandidates(List<Long> predecessorQueryCandidates) {
    f4 = predecessorQueryCandidates;
  }

  public List<Long> getSuccessorQueryCandidates() {
    return f5;
  }

  public void setSuccessorQueryCandidates(List<Long> successorQueryCandidates) {
    f5 = successorQueryCandidates;
  }

  public Boolean isUpdated() {
    return f6;
  }

  public void isUpdated(Boolean isUpdated) {
    f6 = isUpdated;
  }
}
