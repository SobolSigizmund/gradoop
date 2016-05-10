package org.gradoop.model.impl.operators.matching.simulation.dual;

import org.gradoop.model.GradoopFlinkTestBase;
import org.gradoop.model.impl.EPGMDatabase;
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

import java.io.InputStream;

import static org.gradoop.model.impl.GradoopFlinkTestUtils.printLogicalGraph;

public class DualSimulationTest extends GradoopFlinkTestBase {

  @Test
  public void test1() throws Exception {
    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader =
      getLoaderFromString(TestData.LABELED_MULTIGRAPH);

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> db =
      loader.getLogicalGraphByVariable("db");

    // pattern
    String query = "(a:A)-[:a]->(b:B)-[:a]->(a);(b)-[:b]->(:C)<-[:a]-(:B)";
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
      "(v1)-[e0]->(v0)" +
      "(v0)-[e1]->(v4)" +
      "(v0)-[e2]->(v4)" +
      "(v0)-[e3]->(v3)" +
      "(v3)-[e4]->(v5)" +
      "(v5)-[e5]->(v1)" +
      "(v1)-[e6]->(v6)" +
      "(v6)-[e7]->(v2)" +
      "(v2)-[e8]->(v6)" +
      "(v5)-[e9]->(v4)" +
      "(v5)-[e10]->(v4)" +
      "(v6)-[e11]->(v7)" +
      "(v8)-[e12]->(v7)" +
      "(v10)-[e13]->(v5)" +
      "(v6)-[e14]->(v10)" +
      "]");

    // create operator
    DualSimulation<GraphHeadPojo, VertexPojo, EdgePojo> op =
      new DualSimulation<>(query);

    // execute and validate
    collectAndAssertTrue(op.execute(db)
      .equalsByElementIds(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void test2() throws Exception {
    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader =
      getLoaderFromString(TestData.LABELED_MULTIGRAPH);

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> db =
      loader.getLogicalGraphByVariable("db");

    // pattern
    String query = "(vq1:B)-[eq1:b]->(vq2:C)<-[eq2:a]-(vq3:A)";
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
      "]");

    // create operator
    DualSimulation<GraphHeadPojo, VertexPojo, EdgePojo> op =
      new DualSimulation<>(query);

    // execute and validate
    collectAndAssertTrue(op.execute(db)
      .equalsByElementIds(loader.getLogicalGraphByVariable("expected")));
  }
}
