package org.gradoop.model.impl.operators.matching.common.functions;

import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.configuration.Configuration;
import org.gradoop.model.api.EPGMElement;
import org.gradoop.model.impl.operators.matching.common.QueryHandler;

public abstract class MatchingElements
  <EL1 extends EPGMElement>
  extends RichFilterFunction<EL1> {

  private final String query;

  private transient QueryHandler queryHandler;

  public MatchingElements(final String query) {
    this.query = query;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    queryHandler = QueryHandler.fromString(query);
  }

  protected QueryHandler getQueryHandler() {
    return queryHandler;
  }
}
