package net.jodah.keyvaluestore.operations;

import java.io.Serializable;

public class EntryEvent<K, V> implements Serializable {
  public Object key;
  public Object oldValue;
  public Object newValue;

  public EntryEvent(Object key, Object oldValue, Object newValue) {
    this.key = key;
    this.oldValue = oldValue;
    this.newValue = newValue;
  }

  @Override
  public String toString() {
    return String.format("EntryEvent [key=%s, oldValue=%s, newValue=%s]", key, oldValue, newValue);
  }
}