package net.jodah.keyvaluestore;

import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import io.atomix.copycat.server.Commit;
import io.atomix.copycat.server.StateMachine;
import io.atomix.copycat.server.session.ServerSession;
import net.jodah.keyvaluestore.operations.Delete;
import net.jodah.keyvaluestore.operations.EntryEvent;
import net.jodah.keyvaluestore.operations.Get;
import net.jodah.keyvaluestore.operations.Listen;
import net.jodah.keyvaluestore.operations.Put;
import net.jodah.keyvaluestore.operations.PutWithTtl;

public class KeyValueStore extends StateMachine {
  private Map<Object, Commit> storage = new HashMap<>();
  private Set<Commit> listeners = new HashSet<>();

  private void publish(String event, Object key, Object oldValue, Object newValue) {
    listeners.forEach(c -> {
      c.session().publish(event, new EntryEvent(key, oldValue, newValue));
    });
  }

  public Object put(Commit<Put> commit) {
    Commit<Put> put = storage.put(commit.operation().key, commit);
    Object oldValue = put == null ? null : put.operation().value;
    publish("put", commit.operation().key, oldValue, commit.operation().value);
    return oldValue;
  }

  public void listen(Commit<Listen> commit) {
    listeners.add(commit);
  }

  public Object putWithTtl(Commit<PutWithTtl> commit) {
    Object result = storage.put(commit.operation().key, commit);
    executor.schedule(Duration.ofMillis(commit.operation().ttl), () -> {
      Commit<PutWithTtl> put = storage.remove(commit.operation().key);
      Object oldValue = put == null ? null : put.operation().value;
      publish("expire", commit.operation().key, oldValue, null);
      commit.release();
    });
    return result;
  }

  public Object get(Commit<Get> commit) {
    try {
      Commit<Put> put = storage.get(commit.operation().key);
      return put == null ? null : put.operation().value;
    } finally {
      commit.release();
    }
  }

  public Object delete(Commit<Delete> commit) {
    Commit<Put> put = null;
    try {
      put = storage.remove(commit.operation().key);
      Object oldValue = put == null ? null : put.operation().value;
      publish("delete", commit.operation().key, oldValue, null);
      return oldValue;
    } finally {
      if (put != null)
        put.release();
      commit.release();
    }
  }
}