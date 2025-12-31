package net.spy.memcached.v2.vo;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.PriorityQueue;

import net.spy.memcached.ops.StatusCode;

public final class SMGetElements<V> {
  private final List<Element<V>> elements;
  private final List<MissedKey> missedKeys;
  private final List<TrimmedKey> trimmedKeys;

  public SMGetElements(List<Element<V>> elements,
                       List<MissedKey> missedKeys,
                       List<TrimmedKey> trimmedKeys) {
    if (elements == null || missedKeys == null || trimmedKeys == null) {
      throw new IllegalArgumentException("Arguments cannot be null");
    }
    this.elements = elements;
    this.missedKeys = missedKeys;
    this.trimmedKeys = trimmedKeys;
  }

  public static <T> SMGetElements<T> mergeSMGetElements(List<SMGetElements<T>> smGetElementsList,
                                                        boolean ascending,
                                                        boolean unique, int count) {
    return mergeSMGetElements(smGetElementsList, ascending, unique, count,
        SMGetMergeStrategy.TIMSORT);
  }

  // Enable/disable merge latency output for performance testing
  private static volatile boolean PRINT_MERGE_LATENCY = false;

  public static void setPrintMergeLatency(boolean enable) {
    PRINT_MERGE_LATENCY = enable;
  }

  public static <T> SMGetElements<T> mergeSMGetElements(List<SMGetElements<T>> smGetElementsList,
                                                        boolean ascending,
                                                        boolean unique, int count,
                                                        SMGetMergeStrategy strategy) {
    long startTime = PRINT_MERGE_LATENCY ? System.nanoTime() : 0;
    SMGetElements<T> result;

    if (strategy == SMGetMergeStrategy.KWAY_MERGE) {
      result = mergeWithKWayMerge(smGetElementsList, ascending, unique, count);
    } else if (strategy == SMGetMergeStrategy.TWO_POINTER) {
      result = mergeWithTwoPointer(smGetElementsList, ascending, unique, count);
    } else {
      result = mergeWithTimSort(smGetElementsList, ascending, unique, count);
    }

    if (PRINT_MERGE_LATENCY) {
      long latencyNs = System.nanoTime() - startTime;
      double latencyMs = latencyNs / 1_000_000.0;
      System.out.printf("[MERGE LATENCY] Strategy=%s, Keys=%d, TotalElements=%d, Latency=%.6f ms%n",
          strategy,
          smGetElementsList.size(),
          smGetElementsList.stream().mapToInt(e -> e.getElements().size()).sum(),
          latencyMs);
    }

    return result;
  }

  private static <T> SMGetElements<T> mergeWithTimSort(List<SMGetElements<T>> smGetElementsList,
                                                        boolean ascending,
                                                        boolean unique, int count) {
    List<Element<T>> allElements = new ArrayList<>();
    List<MissedKey> allMissedKeys = new ArrayList<>();
    List<TrimmedKey> allTrimmedKeys = new ArrayList<>();

    // 1) Collect elements (deduplicate while collecting if unique is true)
    if (unique) {
      collectUniqueElements(smGetElementsList, allElements, allMissedKeys, allTrimmedKeys,
          ascending);
    } else {
      collectDuplicatedElements(smGetElementsList, allElements, allMissedKeys, allTrimmedKeys);
    }

    // 2) Sort elements, missed keys, and trimmed keys
    if (ascending) {
      allElements.sort(Comparator.naturalOrder());
    } else {
      allElements.sort(Comparator.reverseOrder());
    }
    Collections.sort(allMissedKeys);
    Collections.sort(allTrimmedKeys);

    // 3) Trim elements to the requested count
    if (allElements.size() > count) {
      allElements = new ArrayList<>(allElements.subList(0, count));
    }

    // 4) Remove trimmed keys outside the final element range
    if (!allElements.isEmpty()) {
      BKey lastBKey = allElements.get(allElements.size() - 1).getbTreeElement().getBkey();
      allTrimmedKeys.removeIf(trimmedKey -> {
        int comp = trimmedKey.getBKey().compareTo(lastBKey);
        return ascending ? comp >= 0 : comp <= 0;
      });
    }

    return new SMGetElements<>(allElements, allMissedKeys, allTrimmedKeys);
  }

  private static <T> SMGetElements<T> mergeWithKWayMerge(List<SMGetElements<T>> smGetElementsList,
                                                          boolean ascending,
                                                          boolean unique, int count) {
    List<MissedKey> allMissedKeys = new ArrayList<>();
    List<TrimmedKey> allTrimmedKeys = new ArrayList<>();

    // Collect missed keys and trimmed keys
    for (SMGetElements<T> smGetElements : smGetElementsList) {
      allMissedKeys.addAll(smGetElements.getMissedKeys());
      allTrimmedKeys.addAll(smGetElements.getTrimmedKeys());
    }

    List<Element<T>> mergedElements;
    if (unique) {
      mergedElements = kWayMergeUnique(smGetElementsList, ascending, count);
    } else {
      mergedElements = kWayMergeDuplicated(smGetElementsList, ascending, count);
    }

    // Sort missed keys and trimmed keys
    Collections.sort(allMissedKeys);
    Collections.sort(allTrimmedKeys);

    // Remove trimmed keys outside the final element range
    if (!mergedElements.isEmpty()) {
      BKey lastBKey = mergedElements.get(mergedElements.size() - 1).getbTreeElement().getBkey();
      allTrimmedKeys.removeIf(trimmedKey -> {
        int comp = trimmedKey.getBKey().compareTo(lastBKey);
        return ascending ? comp >= 0 : comp <= 0;
      });
    }

    return new SMGetElements<>(mergedElements, allMissedKeys, allTrimmedKeys);
  }

  /**
   * Two-pointer merge algorithm (similar to legacy API).
   * Incrementally merges results from multiple nodes using a two-pointer technique.
   * Most memory-efficient - only keeps count elements at any time.
   */
  private static <T> SMGetElements<T> mergeWithTwoPointer(List<SMGetElements<T>> smGetElementsList,
                                                           boolean ascending,
                                                           boolean unique, int count) {
    List<MissedKey> allMissedKeys = new ArrayList<>();
    List<TrimmedKey> allTrimmedKeys = new ArrayList<>();

    // Collect missed keys and trimmed keys
    for (SMGetElements<T> smGetElements : smGetElementsList) {
      allMissedKeys.addAll(smGetElements.getMissedKeys());
      allTrimmedKeys.addAll(smGetElements.getTrimmedKeys());
    }

    // Incrementally merge results
    List<Element<T>> mergedResult = new ArrayList<>(count);
    for (SMGetElements<T> eachResult : smGetElementsList) {
      mergedResult = mergeTwoLists(mergedResult, eachResult.getElements(),
          ascending, unique, count);
    }

    // Sort missed keys and trimmed keys
    Collections.sort(allMissedKeys);
    Collections.sort(allTrimmedKeys);

    // Remove trimmed keys outside the final element range
    if (!mergedResult.isEmpty()) {
      BKey lastBKey = mergedResult.get(mergedResult.size() - 1).getbTreeElement().getBkey();
      allTrimmedKeys.removeIf(trimmedKey -> {
        int comp = trimmedKey.getBKey().compareTo(lastBKey);
        return ascending ? comp >= 0 : comp <= 0;
      });
    }

    return new SMGetElements<>(mergedResult, allMissedKeys, allTrimmedKeys);
  }

  /**
   * Merge two sorted lists using two-pointer technique.
   * This is the core algorithm similar to SMGetResult.mergeSMGetElements in legacy API.
   */
  private static <T> List<Element<T>> mergeTwoLists(List<Element<T>> mergedResult,
                                                     List<Element<T>> eachResult,
                                                     boolean ascending,
                                                     boolean unique, int count) {
    if (mergedResult.isEmpty()) {
      // First merge, just add all elements up to count
      if (eachResult.size() <= count) {
        return new ArrayList<>(eachResult);
      } else {
        return new ArrayList<>(eachResult.subList(0, count));
      }
    }

    int eachSize = eachResult.size();
    int oldMergedSize = mergedResult.size();
    List<Element<T>> newMergedResult = new ArrayList<>(count);

    int eachPos = 0;
    int oldMergedPos = 0;

    // Two-pointer merge
    while (eachPos < eachSize && oldMergedPos < oldMergedSize
        && newMergedResult.size() < count) {
      Element<T> eachElem = eachResult.get(eachPos);
      Element<T> oldMergedElem = mergedResult.get(oldMergedPos);

      int comp = eachElem.compareTo(oldMergedElem);
      boolean bkeyDuplicated = eachElem.getbTreeElement().getBkey()
          .equals(oldMergedElem.getbTreeElement().getBkey());

      if (bkeyDuplicated && comp != 0) {
        // Same bkey but different key - need to compare keys for ordering
        comp = eachElem.getKey().compareTo(oldMergedElem.getKey());
      }

      if ((ascending && comp < 0) || (!ascending && comp > 0)) {
        // eachElem comes first
        newMergedResult.add(eachElem);
        eachPos++;

        if (unique && bkeyDuplicated) {
          // Skip the duplicate in merged result
          oldMergedPos++;
        }
      } else {
        // oldMergedElem comes first
        newMergedResult.add(oldMergedElem);
        oldMergedPos++;

        if (unique && bkeyDuplicated) {
          // Skip the duplicate in each result
          eachPos++;
        }
      }
    }

    // Add remaining elements from eachResult
    while (eachPos < eachSize && newMergedResult.size() < count) {
      newMergedResult.add(eachResult.get(eachPos++));
    }

    // Add remaining elements from mergedResult
    while (oldMergedPos < oldMergedSize && newMergedResult.size() < count) {
      newMergedResult.add(mergedResult.get(oldMergedPos++));
    }

    return newMergedResult;
  }

  private static <T> List<Element<T>> kWayMergeDuplicated(
      List<SMGetElements<T>> smGetElementsList,
      boolean ascending,
      int count) {

    List<Element<T>> result = new ArrayList<>();

    // Priority queue to hold the current smallest/largest element from each list
    Comparator<ElementWithIndex<T>> comparator = ascending
        ? Comparator.naturalOrder()
        : Comparator.reverseOrder();
    PriorityQueue<ElementWithIndex<T>> pq = new PriorityQueue<>(comparator);

    // Initialize the priority queue with the first element from each list
    for (int i = 0; i < smGetElementsList.size(); i++) {
      List<Element<T>> elements = smGetElementsList.get(i).getElements();
      if (!elements.isEmpty()) {
        pq.offer(new ElementWithIndex<>(elements.get(0), i, 0));
      }
    }

    // Merge elements
    while (!pq.isEmpty() && result.size() < count) {
      ElementWithIndex<T> current = pq.poll();
      result.add(current.element);

      // Add the next element from the same list
      int nextIndex = current.elementIndex + 1;
      List<Element<T>> sourceList = smGetElementsList.get(current.listIndex).getElements();
      if (nextIndex < sourceList.size()) {
        pq.offer(new ElementWithIndex<>(sourceList.get(nextIndex),
            current.listIndex, nextIndex));
      }
    }

    return result;
  }

  private static <T> List<Element<T>> kWayMergeUnique(
      List<SMGetElements<T>> smGetElementsList,
      boolean ascending,
      int count) {

    // Use LinkedHashMap to maintain insertion order while ensuring uniqueness
    Map<BKey, Element<T>> uniqueElements = new java.util.LinkedHashMap<>();

    // Priority queue to hold the current smallest/largest element from each list
    Comparator<ElementWithIndex<T>> comparator = ascending
        ? Comparator.naturalOrder()
        : Comparator.reverseOrder();
    PriorityQueue<ElementWithIndex<T>> pq = new PriorityQueue<>(comparator);

    // Initialize the priority queue with the first element from each list
    for (int i = 0; i < smGetElementsList.size(); i++) {
      List<Element<T>> elements = smGetElementsList.get(i).getElements();
      if (!elements.isEmpty()) {
        pq.offer(new ElementWithIndex<>(elements.get(0), i, 0));
      }
    }

    // Merge elements with deduplication
    while (!pq.isEmpty() && uniqueElements.size() < count) {
      ElementWithIndex<T> current = pq.poll();
      BKey bkey = current.element.getbTreeElement().getBkey();

      // Check if we've seen this BKey before
      Element<T> existingElement = uniqueElements.get(bkey);
      if (existingElement == null) {
        // First time seeing this BKey, add it
        uniqueElements.put(bkey, current.element);
      } else {
        // Duplicate BKey found
        // Keep the element from the smaller key (ascending) or larger key (descending)
        int keyComparison = existingElement.getKey().compareTo(current.element.getKey());
        if ((ascending && keyComparison > 0) || (!ascending && keyComparison < 0)) {
          // Replace the existing element in map (O(1) operation)
          uniqueElements.put(bkey, current.element);
        }
      }

      // Add the next element from the same list
      int nextIndex = current.elementIndex + 1;
      List<Element<T>> sourceList = smGetElementsList.get(current.listIndex).getElements();
      if (nextIndex < sourceList.size()) {
        pq.offer(new ElementWithIndex<>(sourceList.get(nextIndex),
            current.listIndex, nextIndex));
      }
    }

    return new ArrayList<>(uniqueElements.values());
  }

  private static class ElementWithIndex<T> implements Comparable<ElementWithIndex<T>> {
    private final Element<T> element;
    private final int listIndex;
    private final int elementIndex;

    ElementWithIndex(Element<T> element, int listIndex, int elementIndex) {
      this.element = element;
      this.listIndex = listIndex;
      this.elementIndex = elementIndex;
    }

    @Override
    public int compareTo(ElementWithIndex<T> o) {
      return this.element.compareTo(o.element);
    }
  }

  private static <T> void collectUniqueElements(
      List<SMGetElements<T>> smGetElementsList,
      List<Element<T>> allElements,
      List<MissedKey> allMissedKeys,
      List<TrimmedKey> allTrimmedKeys,
      boolean ascending) {
    Map<BKey, Element<T>> uniqueMap = new HashMap<>();

    for (SMGetElements<T> smGetElements : smGetElementsList) {
      for (Element<T> element : smGetElements.getElements()) {
        BKey bkey = element.getbTreeElement().getBkey();

        uniqueMap.compute(bkey, (k, existing) -> {
          if (existing == null) {
            return element;
          }

          // Remain only one element when bkey is duplicated:
          // - smaller key if ascending bkey range
          // - larger key if descending bkey range
          int keyComparison = existing.getKey().compareTo(element.getKey());
          if (ascending) {
            return keyComparison <= 0 ? existing : element;
          } else {
            return keyComparison >= 0 ? existing : element;
          }
        });
      }
      allMissedKeys.addAll(smGetElements.getMissedKeys());
      allTrimmedKeys.addAll(smGetElements.getTrimmedKeys());
    }

    allElements.addAll(uniqueMap.values());
  }

  private static <T> void collectDuplicatedElements(
      List<SMGetElements<T>> smGetElementsList,
      List<Element<T>> allElements,
      List<MissedKey> allMissedKeys,
      List<TrimmedKey> allTrimmedKeys) {
    for (SMGetElements<T> smgetElements : smGetElementsList) {
      allElements.addAll(smgetElements.getElements());
      allMissedKeys.addAll(smgetElements.getMissedKeys());
      allTrimmedKeys.addAll(smgetElements.getTrimmedKeys());
    }
  }

  public List<Element<V>> getElements() {
    return Collections.unmodifiableList(elements);
  }

  public List<MissedKey> getMissedKeys() {
    return Collections.unmodifiableList(missedKeys);
  }

  public List<TrimmedKey> getTrimmedKeys() {
    return Collections.unmodifiableList(trimmedKeys);
  }

  public static final class Element<V> implements Comparable<Element<V>> {

    private final String key;
    private final BTreeElement<V> bTreeElement;

    public Element(String key, BTreeElement<V> element) {
      if (key == null || element == null) {
        throw new IllegalArgumentException("key or element cannot be null");
      }
      this.key = key;
      this.bTreeElement = element;
    }

    @Override
    public int compareTo(Element<V> o) {
      int elementComparison = bTreeElement.compareTo(o.getbTreeElement());
      if (elementComparison == 0) {
        return this.key.compareTo(o.key);
      }
      return elementComparison;
    }

    public String getKey() {
      return key;
    }

    public BTreeElement<V> getbTreeElement() {
      return bTreeElement;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }

      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Element<?> that = (Element<?>) o;
      return Objects.equals(key, that.key) && Objects.equals(bTreeElement, that.bTreeElement);
    }

    @Override
    public int hashCode() {
      return Objects.hash(key, bTreeElement);
    }
  }

  public static final class MissedKey implements Comparable<MissedKey> {
    private final String key;
    private final StatusCode statusCode;

    public MissedKey(String key, StatusCode statusCode) {
      if (key == null || statusCode == null) {
        throw new IllegalArgumentException("key or statusCode cannot be null");
      }
      this.key = key;
      this.statusCode = statusCode;
    }

    public String getKey() {
      return key;
    }

    public StatusCode getStatusCode() {
      return statusCode;
    }

    @Override
    public int compareTo(MissedKey o) {
      return this.key.compareTo(o.key);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }

      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      MissedKey missedKey = (MissedKey) o;
      return Objects.equals(key, missedKey.key) && statusCode == missedKey.statusCode;
    }

    @Override
    public int hashCode() {
      return Objects.hash(key, statusCode);
    }
  }

  public static final class TrimmedKey implements Comparable<TrimmedKey> {
    private final String key;
    private final BKey bKey;

    public TrimmedKey(String key, BKey bKey) {
      if (key == null || bKey == null) {
        throw new IllegalArgumentException("key or bKey cannot be null");
      }
      this.key = key;
      this.bKey = bKey;
    }

    public String getKey() {
      return key;
    }

    public BKey getBKey() {
      return bKey;
    }

    @Override
    public int compareTo(TrimmedKey o) {
      return this.key.compareTo(o.key);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }

      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      TrimmedKey that = (TrimmedKey) o;
      return Objects.equals(key, that.key) && Objects.equals(bKey, that.bKey);
    }

    @Override
    public int hashCode() {
      return Objects.hash(key, bKey);
    }
  }
}
