package org.stianloader.paperpusher.search;

import java.util.Collections;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.function.Supplier;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

class COWMapHandle<K extends Comparable<K>, V> {
    @NotNull
    private volatile NavigableMap<K, V> readView;

    @SuppressWarnings("null")
    public COWMapHandle() {
        this.readView = Collections.unmodifiableNavigableMap(new TreeMap<>());
    }

    public COWMapHandle(COWMapHandle<K, V> map) {
        this.readView = map.readView;
    }

    @SuppressWarnings("null")
    public synchronized void compute(@NotNull K key, @NotNull Function<@Nullable V, @NotNull V> valueMapper) {
        V originValue = this.readView.get(key);
        V value = Objects.requireNonNull(valueMapper.apply(originValue), "mapper may not return null.");
        if (originValue == value) {
            return;
        }
        NavigableMap<K, V> writeMap = new TreeMap<>(this.readView);
        writeMap.put(key, value);
        this.readView = Collections.unmodifiableNavigableMap(writeMap);
    }

    @NotNull
    public NavigableMap<K, V> getReadView() {
        return this.readView;
    }

    @SuppressWarnings("null")
    public synchronized void put(@NotNull K key, @NotNull V value) {
        NavigableMap<K, V> writeMap = new TreeMap<>(this.readView);
        writeMap.put(key, value);
        this.readView = Collections.unmodifiableNavigableMap(writeMap);
    }

    @SuppressWarnings("null")
    public synchronized V putIfAbsent(@NotNull K key, @NotNull Supplier<@NotNull V> valueProvider) {
        V value = this.readView.get(key);
        if (value == null) {
            NavigableMap<K, V> writeMap = new TreeMap<>(this.readView);
            writeMap.put(key, (value = valueProvider.get()));
            this.readView = Collections.unmodifiableNavigableMap(writeMap);
        }

        return value;
    }
}
