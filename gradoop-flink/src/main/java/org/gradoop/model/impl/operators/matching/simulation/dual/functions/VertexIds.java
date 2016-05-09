package org.gradoop.model.impl.operators.matching.simulation.dual.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.util.Collector;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.operators.matching.simulation.dual.tuples.TripleWithCandidates;

/**
 * Extracts the source and the target vertex id from the given triple.
 */
public class VertexIds implements
  FlatMapFunction<TripleWithCandidates, Tuple1<GradoopId>> {

  private final Tuple1<GradoopId> reuseTuple1 = new Tuple1<>();

  private final Tuple1<GradoopId> reuseTuple2 = new Tuple1<>();

  @Override
  public void flatMap(TripleWithCandidates tripleWithCandidates,
    Collector<Tuple1<GradoopId>> collector) throws Exception {
    reuseTuple1.f0 = tripleWithCandidates.getSourceVertexId();
    reuseTuple2.f0 = tripleWithCandidates.getTargetVertexId();
    collector.collect(reuseTuple1);
    collector.collect(reuseTuple2);
  }
}
