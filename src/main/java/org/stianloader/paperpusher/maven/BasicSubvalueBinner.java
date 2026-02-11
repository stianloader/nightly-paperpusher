package org.stianloader.paperpusher.maven;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;

import org.jetbrains.annotations.NotNull;

public class BasicSubvalueBinner<@NotNull K, @NotNull T> implements Collector<T, Map<K, Set<T>>, Map<K, Set<T>>> {
    @NotNull
    private final Function<T, K> subvalueComputer;

    public BasicSubvalueBinner(@NotNull Function<T, K> subvalueComputer) {
        this.subvalueComputer = subvalueComputer;
    }

    @Override
    public Supplier<Map<K, Set<T>>> supplier() {
        return HashMap::new;
    }

    @Override
    public BiConsumer<Map<K, Set<T>>, T> accumulator() {
        return (map, accumValue) -> {
            map.compute(this.subvalueComputer.apply(accumValue), (_, value) -> {
                if (value == null) {
                    value = new HashSet<>();
                }
                value.add(accumValue);
                return value;
            });
        };
    }

    @Override
    public BinaryOperator<Map<K, Set<T>>> combiner() {
        return (a, b) -> {
            a.putAll(b);
            return a;
        };
    }

    @Override
    public Function<Map<K, Set<T>>, Map<K, Set<T>>> finisher() {
        return Function.identity();
    }

    @Override
    public Set<Characteristics> characteristics() {
        return Set.of(Characteristics.IDENTITY_FINISH, Characteristics.UNORDERED);
    }
}
