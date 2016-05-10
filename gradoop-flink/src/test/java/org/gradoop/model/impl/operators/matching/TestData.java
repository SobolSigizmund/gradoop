package org.gradoop.model.impl.operators.matching;

public class TestData {

  public static String LABELED_MULTIGRAPH = "db [" +
    "(v0:B {id=0})" +
    "(v1:A {id=1})" +
    "(v2:A {id=2})" +
    "(v3:A {id=3})" +
    "(v4:C {id=4})" +
    "(v5:B {id=5})" +
    "(v6:B {id=6})" +
    "(v7:C {id=7})" +
    "(v8:B {id=8})" +
    "(v9:B {id=9})" +
    "(v10:A {id=10})" +
    "(v11:C {id=11})" +
    "(v12:D {id=12})" +
    "(v1)-[e0:a {id=0}]->(v0)" +
    "(v0)-[e1:b {id=1}]->(v4)" +
    "(v0)-[e2:a {id=2}]->(v4)" +
    "(v0)-[e3:a {id=3}]->(v3)" +
    "(v3)-[e4:a {id=4}]->(v5)" +
    "(v5)-[e5:a {id=5}]->(v1)" +
    "(v1)-[e6:a {id=6}]->(v6)" +
    "(v6)-[e7:a {id=7}]->(v2)" +
    "(v2)-[e8:a {id=8}]->(v6)" +
    "(v5)-[e9:a {id=9}]->(v4)" +
    "(v5)-[e10:b {id=10}]->(v4)" +
    "(v6)-[e11:b {id=11}]->(v7)" +
    "(v8)-[e12:a {id=12}]->(v7)" +
    "(v10)-[e13:a {id=13}]->(v5)" +
    "(v6)-[e14:a {id=14}]->(v10)" +
    "(v9)-[e15:d {id=15}]->(v9)" +
    "(v9)-[e16:a {id=16}]->(v10)" +
    "(v10)-[e17:d {id=17}]->(v11)" +
    "(v11)-[e18:a {id=18}]->(v12)" +
    "]";
}
