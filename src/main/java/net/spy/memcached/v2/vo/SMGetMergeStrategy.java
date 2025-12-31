package net.spy.memcached.v2.vo;

/**
 * Strategy for merging BTree SMGet results from multiple nodes.
 */
public enum SMGetMergeStrategy {
  /**
   * TimSort algorithm (Java's default sort).
   * Collects all elements and sorts them using Collections.sort().
   * Simple but less efficient for large datasets.
   */
  TIMSORT,

  /**
   * K-way merge algorithm using a priority queue.
   * More efficient when merging multiple pre-sorted lists.
   * Better performance when dealing with many nodes.
   */
  KWAY_MERGE,

  /**
   * Two-pointer merge algorithm (similar to legacy API).
   * Incrementally merges results as they arrive from each node.
   * Most efficient strategy - only keeps count elements in memory.
   * Best performance for most use cases.
   */
  TWO_POINTER
}