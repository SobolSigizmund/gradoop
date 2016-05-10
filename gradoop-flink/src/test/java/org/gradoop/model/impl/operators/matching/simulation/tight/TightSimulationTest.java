package org.gradoop.model.impl.operators.matching.simulation.tight;

import org.gradoop.model.GradoopFlinkTestBase;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.operators.matching.TestData;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.VertexPojo;
import org.gradoop.util.FlinkAsciiGraphLoader;
import org.junit.Test;
import org.s1ck.gdl.GDLHandler;
import org.s1ck.gdl.model.Edge;
import org.s1ck.gdl.model.Vertex;

public class TightSimulationTest extends GradoopFlinkTestBase {

  @Test
  public void testVertexLabeledMultigraph() throws Exception {
    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader =
      getLoaderFromString(TestData.LABELED_MULTIGRAPH);

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> db =
      loader.getLogicalGraphByVariable("db");

    // pattern
    String query = "(a:A)-[:a]->(b:B)-[:a]->(a);(b)-[:b]->(:C)";
    GDLHandler gdlHandler = new GDLHandler.Builder().buildFromString(query);
    System.out.println("Pattern:");
    for (Vertex vertex : gdlHandler.getVertices()) {
      System.out.println(vertex);
    }
    for (Edge edge : gdlHandler.getEdges()) {
      System.out.println(edge);
    }

    // expected result
    loader.appendToDatabaseFromString(
      "expected_1[" +
        "(v2)-[e3]->(v6)" +
        "(v6)-[e7]->(v2)" +
        "(v6)-[e9]->(v7)" +
      "]");

    // create operator
    TightSimulation<GraphHeadPojo, VertexPojo, EdgePojo> op =
      new TightSimulation<>(query);

    // execute and validate
    collectAndAssertTrue(op.execute(db).equalsByGraphElementIds(loader
      .getGraphCollectionByVariables("expected_1", "expected_2")));
  }
}
