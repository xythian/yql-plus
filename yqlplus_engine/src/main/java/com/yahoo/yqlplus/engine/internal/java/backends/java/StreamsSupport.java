package com.yahoo.yqlplus.engine.internal.java.backends.java;

import java.util.Collection;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Predicate;
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
        if(rows instanceof Collection) {
            return ((Collection<ROW>) rows).stream();
        } else {
            return StreamSupport.stream(rows.spliterator(), false);
        }
    }

    public static <ROW> Predicate<ROW> skipNulls() {
        return Objects::nonNull;
    }
}
