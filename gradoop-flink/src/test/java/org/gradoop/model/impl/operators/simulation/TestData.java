package org.gradoop.model.impl.operators.simulation;

public class TestData {

  public static String VERTEX_LABELED_MULTIGRAPH = "db [" +
    "(v0:B {id=0})" +
    "(v1:A {id=1})" +
    "(v2:A {id=2})" +
    "(v3:C {id=3})" +
    "(v4:B {id=4})" +
    "(v5:A {id=5})" +
    "(v6:B {id=6})" +
    "(v7:C {id=7})" +
    "(v8:B {id=8})" +
    "(v9:C {id=9})" +
    "(v10:D {id=10})" +
    "(v0)-[e0:a {id=0}]->(v1)" +
    "(v0)-[e1:a {id=1}]->(v3)" +
    "(v1)-[e2:a {id=2}]->(v6)" +
    "(v2)-[e3:a {id=3}]->(v6)" +
    "(v4)-[e4:a {id=4}]->(v1)" +
    "(v4)-[e5:b {id=5}]->(v3)" +
    "(v5)-[e6:a {id=6}]->(v4)" +
    "(v6)-[e7:a {id=7}]->(v2)" +
    "(v6)-[e8:a {id=8}]->(v5)" +
    "(v6)-[e9:b {id=9}]->(v7)" +
    "(v8)-[e10:a {id=10}]->(v5)" +
    "(v5)-[e11:a {id=11}]->(v9)" +
    "(v0)-[e12:c {id=12}]->(v3)" +
    "(v9)-[e13:a {id=13}]->(v10)" +
    "]";
}
