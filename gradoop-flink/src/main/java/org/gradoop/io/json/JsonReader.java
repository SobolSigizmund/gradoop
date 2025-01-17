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

package org.gradoop.io.json;

import org.apache.flink.api.common.functions.MapFunction;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMEdgeFactory;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMGraphHeadFactory;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.api.EPGMVertexFactory;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.id.GradoopIdSet;
import org.gradoop.model.impl.properties.PropertyList;

/**
 * Used to convert json documents into vertices, edges and graphs.
 */
public class JsonReader extends JsonIO {

  /**
   * Reads vertex data from a json document. The document contains at least
   * the vertex id, an embedded data document and an embedded meta document.
   * The data document contains all key-value pairs stored at the vertex, the
   * meta document contains the vertex label and an optional list of graph
   * identifiers the vertex is contained in.
   * <p>
   * Example:
   * <p>
   * {
   * "id":0,
   * "data":{"name":"Alice","gender":"female","age":42},
   * "meta":{"label":"Employee", "out-edges":[0,1,2,3], in-edges:[4,5,6,7],
   * "graphs":[0,1,2,3]}
   * }
   *
   * @param <V> EPGM vertex type class
   */
  public static class JsonToVertexMapper<V extends EPGMVertex> extends
    JsonToEntityMapper implements MapFunction<String, V> {

    /**
     * Creates vertex data objects.
     */
    private final EPGMVertexFactory<V> vertexFactory;

    /**
     * Creates map function
     *
     * @param vertexFactory vertex data factory
     */
    public JsonToVertexMapper(EPGMVertexFactory<V> vertexFactory) {
      this.vertexFactory = vertexFactory;
    }

    /**
     * Constructs a vertex from a given JSON string representation.
     *
     * @param s json string
     * @return Gelly vertex storing gradoop vertex data
     * @throws Exception
     */
    @Override
    public V map(String s) throws Exception {
      JSONObject jsonVertex = new JSONObject(s);
      GradoopId vertexID = getID(jsonVertex);
      String label = getLabel(jsonVertex);
      PropertyList properties = PropertyList.createFromMap(
        getProperties(jsonVertex));
      GradoopIdSet graphs = getGraphs(jsonVertex);

      return vertexFactory.initVertex(vertexID, label, properties, graphs);
    }
  }

  /**
   * Reads edge data from a json document. The document contains at least
   * the edge id, the source vertex id, the target vertex id, an embedded
   * data document and an embedded meta document.
   * The data document contains all key-value pairs stored at the edge, the
   * meta document contains the edge label and an optional list of graph
   * identifiers the edge is contained in.
   * <p>
   * Example:
   * <p>
   * {
   * "id":0,"start":15,"end":12,
   * "data":{"since":2015},
   * "meta":{"label":"worksFor","graphs":[1,2,3,4]}
   * }
   *
   * @param <E> EPGM edge type
   */
  public static class JsonToEdgeMapper<E extends EPGMEdge> extends
    JsonToEntityMapper implements MapFunction<String, E> {

    /**
     * Edge data factory.
     */
    private final EPGMEdgeFactory<E> edgeFactory;

    /**
     * Creates map function.
     *
     * @param edgeFactory edge data factory
     */
    public JsonToEdgeMapper(EPGMEdgeFactory<E> edgeFactory) {
      this.edgeFactory = edgeFactory;
    }

    /**
     * Creates an edge from JSON string representation.
     *
     * @param s json string
     * @return Gelly edge storing gradoop edge data
     * @throws Exception
     */
    @Override
    public E map(String s) throws Exception {
      JSONObject jsonEdge = new JSONObject(s);
      GradoopId edgeID = getID(jsonEdge);
      String edgeLabel = getLabel(jsonEdge);
      GradoopId sourceID = getSourceId(jsonEdge);
      GradoopId targetID = getTargetId(jsonEdge);
      PropertyList properties = PropertyList.createFromMap(
        getProperties(jsonEdge));
      GradoopIdSet graphs = getGraphs(jsonEdge);

      return edgeFactory.initEdge(edgeID, edgeLabel, sourceID, targetID,
        properties, graphs);
    }

    /**
     * Reads the source vertex identifier from the json object.
     *
     * @param jsonEdge json string representation
     * @return source vertex identifier
     * @throws JSONException
     */
    private GradoopId getSourceId(JSONObject jsonEdge
    ) throws JSONException {

      return GradoopId.fromString(jsonEdge.getString(EDGE_SOURCE));
    }

    /**
     * Reads the target vertex identifier from the json object.
     *
     * @param jsonEdge json string representation
     * @return target vertex identifier
     * @throws JSONException
     */
    private GradoopId getTargetId(JSONObject jsonEdge
    ) throws JSONException {

      return GradoopId.fromString(jsonEdge.getString(EDGE_TARGET));
    }
  }

  /**
   * Reads graph data from a json document. The document contains at least
   * the graph id, an embedded data document and an embedded meta document.
   * The data document contains all key-value pairs stored at the graphs, the
   * meta document contains the graph label and the vertex/edge identifiers
   * of vertices/edges contained in that graph.
   * <p>
   * Example:
   * <p>
   * {
   * "id":0,
   * "data":{"title":"Graph Databases"},
   * "meta":{"label":"Community","vertices":[0,1,2],"edges":[4,5,6]}
   * }
   *
   * @param <G> EPGM graph head type
   */
  public static class JsonToGraphHeadMapper<G extends EPGMGraphHead> extends
    JsonToEntityMapper implements MapFunction<String, G> {

    /**
     * Creates graph data objects
     */
    private final EPGMGraphHeadFactory<G> graphHeadFactory;

    /**
     * Creates map function
     *
     * @param graphHeadFactory graph data factory
     */
    public JsonToGraphHeadMapper(EPGMGraphHeadFactory<G> graphHeadFactory) {
      this.graphHeadFactory = graphHeadFactory;
    }

    /**
     * Creates graph data from JSON string representation.
     *
     * @param s json string representation
     * @return Subgraph storing graph data
     * @throws Exception
     */
    @Override
    public G map(String s) throws Exception {
      JSONObject jsonGraph = new JSONObject(s);
      GradoopId graphID = getID(jsonGraph);
      String label = getLabel(jsonGraph);
      PropertyList properties = PropertyList.createFromMap(
        getProperties(jsonGraph));

      return graphHeadFactory.initGraphHead(graphID, label, properties);
    }
  }
}
