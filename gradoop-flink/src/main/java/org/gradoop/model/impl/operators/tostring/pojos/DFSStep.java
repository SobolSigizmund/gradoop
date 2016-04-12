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

package org.gradoop.model.impl.operators.tostring.pojos;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;

import java.io.Serializable;

/**
 * pojo representing an DFS traversal step
 */
public class DFSStep implements Serializable {
  /**
   * discovery time of traversal start vertex
   */
  private final int fromTime;
  /**
   * label of traversal start vertex
   */
  private final String fromLabel;
  /**
   * true, if edge was traversed in direction
   */
  private final Boolean outgoing;
  /**
   * label of the traversed edge
   */
  private final String edgeLabel;
  /**
   * discovery time of traversal end vertex
   */
  private final int toTime;
  /**
   * label of traversal end vertex
   */
  private final String toLabel;

  /**
   * constructor
   * @param fromTime discovery time of traversal start vertex
   * @param fromLabel label of traversal start vertex
   * @param outgoing true, if edge was traversed in direction
   * @param edgeLabel label of the traversed edge
   * @param toTime discovery time of traversal end vertex
   * @param toLabel label of traversal end vertex
   */
  public DFSStep(int fromTime, String fromLabel, Boolean outgoing,
    String edgeLabel, int toTime, String toLabel) {
    this.fromTime = fromTime;
    this.fromLabel = fromLabel;
    this.outgoing = outgoing;
    this.edgeLabel = edgeLabel;
    this.toTime = toTime;
    this.toLabel = toLabel;
  }

  @Override
  public String toString() {
    return "(" + fromTime + ":" + fromLabel + ")" +
      (outgoing ? "" : "<") + "-" + edgeLabel + "-" + (outgoing ? ">" : "") +
      "(" + toTime + ":" + toLabel + ")";
  }

  public int getFromTime() {
    return fromTime;
  }

  public String getFromLabel() {
    return fromLabel;
  }

  public Boolean isOutgoing() {
    return outgoing;
  }

  public String getEdgeLabel() {
    return edgeLabel;
  }

  public int getToTime() {
    return toTime;
  }

  public String getToLabel() {
    return toLabel;
  }

  @Override
  public boolean equals(Object obj) {
    boolean equals = obj == this;

    if (!equals && obj != null && obj.getClass() == getClass()) {

      DFSStep other = (DFSStep) obj;

      EqualsBuilder builder = new EqualsBuilder();

      builder.append(this.isOutgoing(), other.isOutgoing());
      builder.append(this.getFromTime(), other.getFromTime());
      builder.append(this.getToTime(), other.getToTime());
      builder.append(this.getFromLabel(), other.getFromLabel());
      builder.append(this.getEdgeLabel(), other.getEdgeLabel());
      builder.append(this.getToLabel(), other.getToLabel());

      equals = builder.isEquals();
    }

    return equals;
  }

  @Override
  public int hashCode() {
    HashCodeBuilder builder = new HashCodeBuilder();

    builder.append(isOutgoing());
    builder.append(getFromTime());
    builder.append(getToTime());
    builder.append(getFromLabel());
    builder.append(getEdgeLabel());
    builder.append(getToLabel());

    return builder.hashCode();
  }

  public Boolean isLoop() {
    return fromTime == toTime;
  }

  public Boolean isForward() {
    return getFromTime() < getToTime() || getToTime() == 0;
  }

  public Boolean isBackward() {
    return !isForward();
  }
}