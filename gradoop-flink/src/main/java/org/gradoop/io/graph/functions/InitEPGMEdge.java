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

package org.gradoop.io.graph.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.gradoop.io.graph.tuples.ImportEdge;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMEdgeFactory;
import org.gradoop.model.impl.id.GradoopId;

/**
 * Initializes an EPGM edge from the given {@link ImportEdge}.
 *
 * @param <E> EPGM edge type
 * @param <K> Import Edge/Vertex identifier type
 */
@FunctionAnnotation.ForwardedFieldsFirst(
  "f2->f0;" +           // import target vertex id
  "f3->f1.label;" +     // edge label
  "f4->f1.properties")  // edge properties
@FunctionAnnotation.ForwardedFieldsSecond(
  "f1->f1.sourceId"     // EPGM source vertex id
)
public class InitEPGMEdge<E extends EPGMEdge, K extends Comparable<K>>
  extends InitEPGMElement<E, K>
  implements JoinFunction<ImportEdge<K>, Tuple2<K, GradoopId>, Tuple2<K, E>>,
  ResultTypeQueryable<Tuple2<K, E>> {

  /**
   * Used to create new EPGM edge.
   */
  private final EPGMEdgeFactory<E> edgeFactory;

  /**
   * Reduce object instantiation.
   */
  private final Tuple2<K, E> reuseTuple;

  /**
   * Creates a new join function.
   *
   * @param edgeFactory         edge factory
   * @param lineagePropertyKey  property key to store import identifier
   *                            (can be {@code null})
   * @param keyTypeInfo         type info for the import edge identifier
   */
  public InitEPGMEdge(EPGMEdgeFactory<E> edgeFactory, String lineagePropertyKey,
    TypeInformation<K> keyTypeInfo) {
    super(lineagePropertyKey, keyTypeInfo);
    this.edgeFactory        = edgeFactory;
    this.reuseTuple         = new Tuple2<>();
  }

  /**
   * Outputs a pair of import target vertex id and new EPGM edge. The target
   * vertex id is used for further joining the tuple with the import vertices.
   *
   * @param importEdge    import edge
   * @param vertexIdPair  pair of import id and corresponding Gradoop vertex id
   * @return pair of import target vertex id and EPGM edge
   * @throws Exception
   */
  @Override
  public Tuple2<K, E> join(ImportEdge<K> importEdge,
    Tuple2<K, GradoopId> vertexIdPair) throws Exception {
    reuseTuple.f0 = importEdge.getTargetVertexId();

    E edge = edgeFactory.createEdge(importEdge.getLabel(),
      vertexIdPair.f1, GradoopId.get(), importEdge.getProperties());

    reuseTuple.f1 = updateLineage(edge, importEdge.getId());

    return reuseTuple;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public TypeInformation<Tuple2<K, E>> getProducedType() {
    return new TupleTypeInfo<>(getKeyTypeInfo(),
      TypeExtractor.createTypeInfo(edgeFactory.getType()));
  }
}
