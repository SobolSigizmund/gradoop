package org.gradoop.model.impl.operators.matching.common.debug;

import com.google.common.collect.Maps;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.gradoop.model.impl.id.GradoopId;

import java.util.List;
import java.util.Map;

/**
 * Copyright 2016 martin.
 */
public abstract class Printer<IN> extends RichMapFunction<IN, IN> {

  public static final String VERTEX_MAPPING = "vertexMapping";

  public static final String EDGE_MAPPING = "edgeMapping";

  protected Map<GradoopId, Integer> vertexMap;

  protected Map<GradoopId, Integer> edgeMap;

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    List<Tuple2<GradoopId, Integer>> vertexMapping = getRuntimeContext()
      .getBroadcastVariable(VERTEX_MAPPING);
    vertexMap = initMapping(vertexMapping);
    List<Tuple2<GradoopId, Integer>> edgeMapping = getRuntimeContext()
      .getBroadcastVariable(EDGE_MAPPING);
    edgeMap = initMapping(edgeMapping);
  }

  private Map<GradoopId, Integer> initMapping(List<Tuple2<GradoopId, Integer>> tuples) {
    Map<GradoopId, Integer> map = Maps.newHashMap();
    for (Tuple2<GradoopId, Integer> tuple : tuples) {
      map.put(tuple.f0, tuple.f1);
    }
    return map;
  }
}
