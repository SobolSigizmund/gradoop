/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.model.impl.algorithms.fsm.common.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.id.GradoopId;

/**
 * EPGMVertex => (graphId, vertexId, vertexLabel),..
 * @param <V> vertex type
 */
public class GraphIdVertexIdLabel<V extends EPGMVertex>
  implements FlatMapFunction<V, Tuple2<GradoopId, String>> {

  @Override
  public void flatMap(V vertex,
    Collector<Tuple2<GradoopId, String>> collector) throws
    Exception {

    String label = vertex.getLabel();

    for (GradoopId graphId : vertex.getGraphIds()) {
      collector.collect(
        new Tuple2<>(graphId, label));
    }
  }
}