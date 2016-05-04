package org.gradoop.model.impl.operators.simulation.dual;

import org.gradoop.model.GradoopFlinkTestBase;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.operators.simulation.TestData;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.VertexPojo;
import org.gradoop.util.FlinkAsciiGraphLoader;
import org.junit.Test;
import org.s1ck.gdl.GDLHandler;
import org.s1ck.gdl.model.Edge;
import org.s1ck.gdl.model.Vertex;

public class DualSimulationTest extends GradoopFlinkTestBase {

  @Test
  public void testVertexLabeledMultigraph() throws Exception {
    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader =
      getLoaderFromString(TestData.VERTEX_LABELED_MULTIGRAPH);

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
    loader.appendToDatabaseFromString("expected[" +
      "(v1)-[e2]->(v6)" +
      "(v2)-[e3]->(v6)" +
      "(v4)-[e4]->(v1)" +
      "(v4)-[e5]->(v3)" +
      "(v5)-[e6]->(v4)" +
      "(v6)-[e7]->(v2)" +
      "(v6)-[e8]->(v5)" +
      "(v6)-[e9]->(v7)" +
      "]");

    // create operator
    DualSimulation<GraphHeadPojo, VertexPojo, EdgePojo> op =
      new DualSimulation<>(query);

//    op.execute(db);
//    printLogicalGraph(op.execute(db));

    // execute and validate
    collectAndAssertTrue(op.execute(db)
      .equalsByElementIds(loader.getLogicalGraphByVariable("expected")));
  }
}
