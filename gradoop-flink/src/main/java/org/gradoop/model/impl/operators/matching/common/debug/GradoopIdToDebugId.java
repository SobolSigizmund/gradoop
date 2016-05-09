package org.gradoop.model.impl.operators.matching.common.debug;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.model.api.EPGMElement;
import org.gradoop.model.impl.id.GradoopId;

/**
 * Copyright 2016 martin.
 */
public class GradoopIdToDebugId<EL extends EPGMElement> implements
  MapFunction<EL, Tuple2<GradoopId, Integer>> {

  private final String propertyKey;

  public GradoopIdToDebugId(String propertyKey) {
    this.propertyKey = propertyKey;
  }
  @Override
  public Tuple2<GradoopId, Integer> map(EL el) throws Exception {
    return new Tuple2<>(el.getId(), el.getPropertyValue(propertyKey).getInt());
  }
}
