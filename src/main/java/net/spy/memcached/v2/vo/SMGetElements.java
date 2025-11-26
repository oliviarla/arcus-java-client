package net.spy.memcached.v2.vo;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

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
