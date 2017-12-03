package com.yahoo.yqlplus.engine.internal.java.backends.java;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class StreamsSupport {
    public static <ROW> Function<Iterable<ROW>,Stream<ROW>> collectionFlattener() {
        return new Function<Iterable<ROW>, Stream<ROW>>() {
            @Override
            public Stream<ROW> apply(Iterable<ROW> rows) {
                return toStream(rows);
            }
        };
    }

    public static <ROW> Stream<ROW> toStream(Iterable<ROW> rows) {
        if (rows == null) {
            return Stream.empty();
        } else if(rows instanceof Collection) {
            return ((Collection<ROW>) rows).stream();
        } else {
            return StreamSupport.stream(rows.spliterator(), false);
        }
    }

    public static <ROW> Stream<ROW> skipNulls(Stream<ROW> input) {
        return input.filter(Objects::nonNull);
    }

    public static <IROW,OROW,KEY> Stream<OROW> groupBy(Stream<IROW> input, Function<IROW,KEY> keyFunction, BiFunction<KEY,List<IROW>,OROW> groupBy) {
        return input.collect(Collectors.groupingByConcurrent(keyFunction))
                .entrySet()
                .stream()
                .map((e) -> {
                    return groupBy.apply(e.getKey(), e.getValue());
                });
    }

    public static <LROW, RROW, OROW> Stream<OROW> cross(Stream<LROW> left, Iterable<RROW> right, BiFunction<LROW,RROW,OROW> crossFunction) {
        return left.flatMap(lrow -> {
            Stream.Builder<Object> out = Stream.builder();
            for(RROW rrow : right) {
                OROW x = crossFunction.apply(lrow, rrow);
                if(x instanceof Iterable) {
                    for(Object v : (Iterable)x) {
                        out.add(v);
                    }
                } else if(x instanceof Stream) {
                    ((Stream) x).forEachOrdered(out);
                } else {
                    out.add(x);
                }
            }
            return (Stream<OROW>)out.build();
        });
    }

    public static <KEY,ROW> Map<KEY, List<ROW>> computeJoinHash(Stream<ROW> input, Function<ROW,KEY> rowkeyFunction) {
        return input.collect(Collectors.groupingByConcurrent(rowkeyFunction));
    }

    public static <KEY, LROW, RROW, OROW> Stream<OROW> hashJoin(Stream<LROW> left, Stream<RROW> right, Function<LROW,KEY> leftKey, Function<RROW,KEY> rightKey, BiFunction<LROW,RROW,OROW> joinFunction) {
        final Map<KEY,List<RROW>> joinMap = computeJoinHash(right, rightKey);
        return left.flatMap(lrow -> {
            Stream.Builder<OROW> out = Stream.builder();
            for(RROW rrow : joinMap.getOrDefault(leftKey.apply(lrow), ImmutableList.of())) {
                out.add(joinFunction.apply(lrow, rrow));
            }
            return out.build();
        });
    }

    public static <KEY, LROW, RROW, OROW> Stream<OROW> outerHashJoin(Stream<LROW> left, Stream<RROW> right, Function<LROW,KEY> leftKey, Function<RROW,KEY> rightKey, BiFunction<LROW,RROW,OROW> joinFunction) {
        final Map<KEY,List<RROW>> joinMap = computeJoinHash(right, rightKey);
        return left.flatMap(lrow -> {
            Stream.Builder<OROW> out = Stream.builder();
            List<RROW> emptyJoin = Lists.newArrayList();
            emptyJoin.add(null);
            for(RROW rrow : joinMap.getOrDefault(leftKey.apply(lrow), emptyJoin)) {
                out.add(joinFunction.apply(lrow, rrow));
            }
            return out.build();
        });
    }
}
