/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.plan.types.base;

import com.google.common.collect.ImmutableList;
import com.yahoo.yqlplus.engine.internal.java.backends.java.StreamsSupport;
import com.yahoo.yqlplus.engine.internal.plan.types.BytecodeExpression;
import com.yahoo.yqlplus.engine.internal.plan.types.StreamAdapter;
import com.yahoo.yqlplus.engine.internal.plan.types.TypeWidget;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

import java.util.Comparator;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class JavaStreamAdapter implements StreamAdapter {
    private final TypeWidget valueType;

    public JavaStreamAdapter(TypeWidget valueType) {
        this.valueType = valueType;
    }

    @Override
    public TypeWidget getValue() {
        return valueType;
    }

    @Override
    public BytecodeExpression first(BytecodeExpression target) {
        return new StreamFirstSequence(target, valueType);
    }

    @Override
    public BytecodeExpression collectList(BytecodeExpression streamInput) {
        return new InvokeExpression(Stream.class,
                Opcodes.INVOKEINTERFACE,
                "collect",
                Type.getMethodDescriptor(Type.getType(Object.class), Type.getType(Collector.class)),
                new ListTypeWidget(valueType),
                streamInput,
                ImmutableList.of(
                        new InvokeExpression(Collectors.class,
                                Opcodes.INVOKESTATIC,
                                "toList",
                                Type.getMethodDescriptor(Type.getType(Collector.class)),
                                AnyTypeWidget.getInstance(),
                                null,
                                ImmutableList.of()
                        )
                ));
    }

    @Override
    public BytecodeExpression streamInto(BytecodeExpression streamInput, BytecodeExpression targetExpression) {
        return new InvokeExpression(Stream.class,
                Opcodes.INVOKEINTERFACE,
                "forEachOrdered",
                Type.getMethodDescriptor(Type.VOID_TYPE, Type.getType(Consumer.class)),
                new ListTypeWidget(valueType),
                streamInput,
                ImmutableList.of(
                        targetExpression
                ));
    }

    @Override
    public BytecodeExpression flatten(BytecodeExpression streamInput) {
        if (!valueType.isIterable()) {
            throw new UnsupportedOperationException("Cannot flatten a Stream of non-iterable values");
        }
        return new InvokeExpression(Stream.class,
                Opcodes.INVOKEINTERFACE,
                "flatMap",
                Type.getMethodDescriptor(Type.getType(Stream.class), Type.getType(Function.class)),
                new StreamTypeWidget(valueType),
                streamInput,
                ImmutableList.of(
                        valueType.getIterableAdapter().streamFlattener()
                ));
    }

    @Override
    public BytecodeExpression offset(BytecodeExpression streamInput, BytecodeExpression offsetExpression) {
        return new InvokeExpression(Stream.class,
                Opcodes.INVOKEINTERFACE,
                "skip",
                Type.getMethodDescriptor(Type.getType(Stream.class), Type.getType(long.class)),
                new StreamTypeWidget(valueType),
                streamInput,
                ImmutableList.of(
                        new BytecodeCastExpression(BaseTypeAdapter.INT64, offsetExpression)
                ));
    }

    @Override
    public BytecodeExpression limit(BytecodeExpression streamInput, BytecodeExpression limitExpression) {
        return new InvokeExpression(Stream.class,
                Opcodes.INVOKEINTERFACE,
                "limit",
                Type.getMethodDescriptor(Type.getType(Stream.class), Type.getType(long.class)),
                new StreamTypeWidget(valueType),
                streamInput,
                ImmutableList.of(
                        new BytecodeCastExpression(BaseTypeAdapter.INT64, limitExpression)
                ));
    }

    @Override
    public BytecodeExpression distinct(BytecodeExpression streamInput) {
        return new InvokeExpression(Stream.class,
                Opcodes.INVOKEINTERFACE,
                "distinct",
                Type.getMethodDescriptor(Type.getType(Stream.class), Type.getType(long.class)),
                new StreamTypeWidget(valueType),
                streamInput,
                ImmutableList.of());
    }

    @Override
    public BytecodeExpression skipNulls(BytecodeExpression streamInput) {
        if(valueType.isNullable()) {
            return new InvokeExpression(StreamsSupport.class,
                                Opcodes.INVOKESTATIC,
                                "skipNulls",
                                Type.getMethodDescriptor(Type.getType(Stream.class), Type.getType(Stream.class)),
                                new StreamTypeWidget(NotNullableTypeWidget.create(valueType)),
                                null,
                            ImmutableList.of(streamInput));
        }
        return streamInput;
    }

    @Override
    public BytecodeExpression filter(BytecodeExpression streamInput, BytecodeExpression predicate) {
        return new InvokeExpression(Stream.class,
                Opcodes.INVOKEINTERFACE,
                "filter",
                Type.getMethodDescriptor(Type.getType(Stream.class), Type.getType(Predicate.class)),
                new StreamTypeWidget(valueType),
                streamInput,
                ImmutableList.of(
                        predicate
                ));
    }

    @Override
    public BytecodeExpression transform(BytecodeExpression streamInput, BytecodeExpression function, TypeWidget resultValueType) {
        return new InvokeExpression(Stream.class,
                Opcodes.INVOKEINTERFACE,
                "map",
                Type.getMethodDescriptor(Type.getType(Stream.class), Type.getType(Function.class)),
                new StreamTypeWidget(resultValueType),
                streamInput,
                ImmutableList.of(
                        function
                ));
    }

    @Override
    public BytecodeExpression scatter(BytecodeExpression streamInput, BytecodeExpression function, TypeWidget resultValueType) {
        BytecodeExpression parallel = new InvokeExpression(Stream.class,
                Opcodes.INVOKEINTERFACE,
                "parallel",
                Type.getMethodDescriptor(Type.getType(Stream.class)),
                new StreamTypeWidget(valueType),
                streamInput,
                ImmutableList.of(
                ));
        return transform(parallel, function, resultValueType);
    }

    @Override
    public BytecodeExpression sorted(BytecodeExpression streamInput, BytecodeExpression comparator) {
        return new InvokeExpression(Stream.class,
                Opcodes.INVOKEINTERFACE,
                "sorted",
                Type.getMethodDescriptor(Type.getType(Stream.class), Type.getType(Comparator.class)),
                new StreamTypeWidget(valueType),
                streamInput,
                ImmutableList.of(
                        comparator
                ));
    }

    @Override
    public BytecodeExpression groupBy(BytecodeExpression streamInput, BytecodeExpression keyFunction, BytecodeExpression groupFunction, TypeWidget resultType) {
        return new InvokeExpression(StreamsSupport.class,
                Opcodes.INVOKESTATIC,
                "groupBy",
                Type.getMethodDescriptor(Type.getType(Stream.class), Type.getType(Stream.class), Type.getType(Function.class), Type.getType(BiFunction.class)),
                new StreamTypeWidget(resultType),
                null,
                ImmutableList.of(
                        streamInput,
                        keyFunction,
                        groupFunction
                ));
    }

    @Override
    public BytecodeExpression cross(BytecodeExpression streamInput, BytecodeExpression rightIterable, BytecodeExpression crossFunction, TypeWidget resultType) {
        return new InvokeExpression(StreamsSupport.class,
                Opcodes.INVOKESTATIC,
                "cross",
                Type.getMethodDescriptor(Type.getType(Stream.class), Type.getType(Stream.class), Type.getType(Iterable.class), Type.getType(BiFunction.class)),
                new StreamTypeWidget(resultType),
                null,
                ImmutableList.of(
                        streamInput,
                        rightIterable,
                        crossFunction
                ));
    }

    @Override
    public BytecodeExpression hashJoin(BytecodeExpression streamInput, boolean outer, BytecodeExpression rightStream, BytecodeExpression leftKeyFunction, BytecodeExpression rightKeyFunction, BytecodeExpression joinFunction, TypeWidget resultType) {
        return new InvokeExpression(StreamsSupport.class,
                Opcodes.INVOKESTATIC,
                outer ? "outerHashJoin" : "hashJoin",
                Type.getMethodDescriptor(Type.getType(Stream.class), Type.getType(Stream.class), Type.getType(Stream.class), Type.getType(Function.class), Type.getType(Function.class), Type.getType(BiFunction.class)),
                new StreamTypeWidget(resultType),
                null,
                ImmutableList.of(
                        streamInput,
                        rightStream,
                        leftKeyFunction,
                        rightKeyFunction,
                        joinFunction
                ));
    }
}
