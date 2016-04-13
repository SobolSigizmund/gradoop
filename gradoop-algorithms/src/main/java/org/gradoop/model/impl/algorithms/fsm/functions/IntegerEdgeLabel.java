package org.gradoop.model.impl.algorithms.fsm.functions;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.model.impl.algorithms.fsm.tuples.IntegerLabeledEdge;
import org.gradoop.model.impl.id.GradoopId;

/**
 * Created by peet on 13.04.16.
 */
public class IntegerEdgeLabel implements
  KeySelector<Tuple2<GradoopId, IntegerLabeledEdge>, Integer> {
  @Override
  public Integer getKey(Tuple2<GradoopId, IntegerLabeledEdge> pair) throws
    Exception {
    return pair.f1.getLabel();
  }
}
