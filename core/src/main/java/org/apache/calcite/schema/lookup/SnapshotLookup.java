/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.schema.lookup;

import org.apache.calcite.linq4j.function.Predicate1;
import org.apache.calcite.util.LazyReference;
import org.apache.calcite.util.NameMap;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

/**
 * This class can be used to make a snapshot of a lookups.
 *
 * <p>The name set is frozen the first time any method is called (so a query resolved against
 * this snapshot sees a consistent set of names for its whole lifetime, unaffected by a
 * concurrent change to the underlying schema) but each entity's value is resolved from the
 * delegate, and memoized, only the first time that specific name is actually asked for via
 * {@link #get} or {@link #getIgnoreCase} — not for every name the moment any one of them, or
 * {@link #getNames}, is requested. A caller that only needs to know what names exist (a FROM
 * -clause identifier resolution, for instance, via {@code getTableNames()}) does not force
 * every table in the schema to be constructed.
 *
 * <p>A snapshotted name whose resolution returns null (the delegate listed an entity it then
 * could not produce) is dropped from later {@link #getNames} results, so a caller that lists
 * and then resolves every name does not keep being handed one it cannot resolve.
 *
 * @param <T> Element Type
 */
public class SnapshotLookup<T> implements Lookup<T> {

  private final Lookup<T> delegate;
  private final LazyReference<NameMap<String>> namesRef = new LazyReference<>();
  private final ConcurrentMap<String, @NonNull T> resolved = new ConcurrentHashMap<>();
  private final Set<String> unresolvable = ConcurrentHashMap.newKeySet();
  private boolean enabled = true;

  public SnapshotLookup(Lookup<T> delegate) {
    this.delegate = delegate;
  }

  @Override public @Nullable T get(final String name) {
    if (!enabled) {
      return delegate.get(name);
    }
    Map.Entry<String, String> entry = names().range(name, true).firstEntry();
    if (entry == null) {
      return null;
    }
    return resolve(entry.getKey());
  }

  @Override public @Nullable Named<T> getIgnoreCase(final String name) {
    if (!enabled) {
      return delegate.getIgnoreCase(name);
    }
    Map.Entry<String, String> entry = names().range(name, false).firstEntry();
    if (entry == null) {
      return null;
    }
    String canonicalName = entry.getKey();
    T value = resolve(canonicalName);
    return value == null ? null : new Named<>(canonicalName, value);
  }

  @Override public Set<String> getNames(final LikePattern pattern) {
    if (!enabled) {
      return delegate.getNames(pattern);
    }
    final Predicate1<String> matcher = pattern.matcher();
    return names().map().keySet().stream()
        .filter(name -> !unresolvable.contains(name))
        .filter(matcher::apply)
        .collect(Collectors.toSet());
  }

  /**
   * Resolves and memoizes one name's value against the delegate, lazily. Called only for a name
   * already confirmed present in the frozen name set, so a miss here means the delegate's value
   * for a known name turned out null (e.g. a table that failed to load) rather than an absent
   * name — not memoized, so a later call can retry it.
   */
  private @Nullable T resolve(String name) {
    T cached = resolved.get(name);
    if (cached != null) {
      return cached;
    }
    T value = delegate.get(name);
    if (value == null) {
      unresolvable.add(name);
      return null;
    }
    T race = resolved.putIfAbsent(name, value);
    return race != null ? race : value;
  }

  private NameMap<String> names() {
    return namesRef.getOrCompute(this::loadNames);
  }

  private NameMap<String> loadNames() {
    NameMap<String> result = new NameMap<>();
    for (String name : delegate.getNames(LikePattern.any())) {
      result.put(name, name);
    }
    return result;
  }

  public void enable(boolean enabled) {
    if (!enabled) {
      namesRef.reset();
      resolved.clear();
      unresolvable.clear();
    }
    this.enabled = enabled;
  }

}
