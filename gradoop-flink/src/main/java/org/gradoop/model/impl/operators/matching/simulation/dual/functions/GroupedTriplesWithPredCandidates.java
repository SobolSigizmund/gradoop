package org.gradoop.model.impl.operators.matching.simulation.dual.functions;

import org.apache.flink.api.common.functions.GroupCombineFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.util.Collector;
import org.gradoop.model.impl.operators.matching.simulation.dual.tuples.TripleWithPredCandidates;

import java.util.Iterator;

/**
 *
 */
public class GroupedTriplesWithPredCandidates implements
  GroupCombineFunction<TripleWithPredCandidates, TripleWithPredCandidates>,
  GroupReduceFunction<TripleWithPredCandidates, TripleWithPredCandidates> {

  @Override
  public void combine(Iterable<TripleWithPredCandidates> triples,
    Collector<TripleWithPredCandidates> collector) throws Exception {
    reduce(triples, collector);
  }

  @Override
  public void reduce(Iterable<TripleWithPredCandidates> triples,
    Collector<TripleWithPredCandidates> collector) throws Exception {
    Iterator<TripleWithPredCandidates> tripleIt = triples.iterator();
    TripleWithPredCandidates result = tripleIt.next();
    while (tripleIt.hasNext()) {
      result.getPredecessorQueryCandidates()
        .addAll(tripleIt.next().getPredecessorQueryCandidates());
    }
    collector.collect(result);
  }
}
