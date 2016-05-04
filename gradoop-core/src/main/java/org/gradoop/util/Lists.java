package org.gradoop.util;

import java.util.List;

/**
 * Helper class for collections.
 */
public class Lists {

  /**
   * Removes each element from the deletions list from the given list exactly
   * once.
   *
   * Example:
   *
   * list:      [1, 1, 1, 2, 2]
   * deletions: [1, 2]
   * result:    [1, 1, 2]
   *
   * @param list list to remove from
   * @param deletions elements to delete from list
   * @param <E> element type
   * @return list with remaining elements
   */
  public static <E> List<E> removeEach(List<E> list, List<E> deletions) {
    for (E deletion : deletions) {
      list.remove(deletion);
    }
    return list;
  }
}
