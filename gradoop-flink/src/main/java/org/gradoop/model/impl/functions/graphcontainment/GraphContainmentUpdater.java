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

package org.gradoop.model.impl.functions.graphcontainment;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.gradoop.model.api.EPGMGraphElement;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.impl.id.GradoopId;

/**
 * Adds the given graph head identifier to the graph element.
 *
 * @param <G>   EPGM graph head type
 * @param <GE>  EPGM graph element type
 */
@FunctionAnnotation.ForwardedFields("id;label;properties")
public class GraphContainmentUpdater
  <G extends EPGMGraphHead, GE extends EPGMGraphElement>
  implements MapFunction<GE, GE> {

  /**
   * Graph head identifier which gets added to the graph element.
   */
  private final GradoopId graphHeadId;

  /**
   * Creates a new GraphContainmentUpdater
   *
   * @param graphHead graph head used for updating
   */
  public GraphContainmentUpdater(G graphHead) {
    this.graphHeadId = graphHead.getId();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GE map(GE graphElement) throws Exception {
    graphElement.addGraphId(graphHeadId);
    return graphElement;
  }
}
