package org.gradoop.model.impl.operators.matching.simulation.dual.functions;

import org.apache.flink.api.common.functions.GroupCombineFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.util.Collector;
import org.gradoop.model.impl.operators.matching.simulation.dual.tuples.TripleWithCandidates;

import java.util.Iterator;

/**
 * Copyright 2016 martin.
 */
public class GroupedTriplesWithCandidates implements
  GroupCombineFunction<TripleWithCandidates, TripleWithCandidates>,
  GroupReduceFunction<TripleWithCandidates, TripleWithCandidates> {

  @Override
  public void combine(Iterable<TripleWithCandidates> triples,
    Collector<TripleWithCandidates> collector) throws Exception {
    reduce(triples, collector);
  }

  @Override
  public void reduce(Iterable<TripleWithCandidates> triples,
    Collector<TripleWithCandidates> collector) throws Exception {
    Iterator<TripleWithCandidates> tripleIt = triples.iterator();
    TripleWithCandidates result = tripleIt.next();
    while (tripleIt.hasNext()) {
      result.getSuccessorQueryCandidates()
        .addAll(tripleIt.next().getSuccessorQueryCandidates());
    }
    collector.collect(result);
  }
}
