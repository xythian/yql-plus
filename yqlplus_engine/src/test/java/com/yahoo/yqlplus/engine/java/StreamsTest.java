package com.yahoo.yqlplus.engine.java;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.yahoo.yqlplus.api.Source;
import com.yahoo.yqlplus.api.annotations.Query;
import com.yahoo.yqlplus.engine.api.Record;
import com.yahoo.yqlplus.engine.internal.bytecode.CompilingTestBase;
import org.testng.Assert;
import org.testng.annotations.Test;
import org.testng.collections.Lists;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class StreamsTest extends CompilingTestBase {
    public static class TestRecord implements Record {
        private Map<String,Object> data;

        public TestRecord(Map<String, Object> data) {
            this.data = data;
        }


        public TestRecord(String id) {
            this(ImmutableMap.of("id", id));
        }

        @Override
        public Iterable<String> getFieldNames() {
            return data.keySet();
        }

        @Override
        public Object get(String field) {
            return data.get(field);
        }
    }

    public static class TestSource implements Source {
        @Query
        public Stream<TestRecord> scan() {
            return ImmutableList.of(new TestRecord("1")).stream();
        }
    }

    @Test
    public void requireSource() throws Exception {
        defineSource("test", TestSource.class);
        List<Record> result = runQueryProgram("SELECT id FROM test WHERE id = '2'");
        Assert.assertEquals(result.size(), 1);
    }

    @Test
    public void exploreStreamAPis() throws Exception {
        List<String> items = ImmutableList.of("a", "b", "c", "d");
        Stream<String> x = items.stream()
                .parallel()
                .flatMap(z -> ImmutableList.of(z, z).stream());
        List<String>  target = x.collect(Collectors.toList());
        System.err.println(target);
    }

    @Test
    public void exploreStreamApis2() throws Exception {
        List<String> items = ImmutableList.of("a", "b", "c", "d");
        Stream<String> x = items.stream()
                .distinct()
                .parallel()
                .flatMap(z -> ImmutableList.of(z, z).stream());
        List<String>  target = x.collect(Collectors.toList());
        System.err.println(target);
    }

}
