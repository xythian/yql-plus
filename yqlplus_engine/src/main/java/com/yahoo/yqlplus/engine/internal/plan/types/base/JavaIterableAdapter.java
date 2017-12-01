/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.plan.types.base;

import com.google.common.collect.ImmutableList;
import com.yahoo.yqlplus.engine.internal.java.backends.java.StreamsSupport;
import com.yahoo.yqlplus.engine.internal.plan.types.*;
import org.objectweb.asm.Type;
import org.objectweb.asm.Opcodes;

import java.util.function.Function;
import java.util.stream.Stream;

public class JavaIterableAdapter implements IterateAdapter {
    private final TypeWidget valueType;

    public JavaIterableAdapter(TypeWidget valueType) {
        this.valueType = valueType;
    }

    @Override
    public TypeWidget getValue() {
        return valueType;
    }

    @Override
    public BytecodeSequence iterate(final BytecodeExpression target, final IterateLoop loop) {
        return new IterateSequence(target, valueType, loop);
    }

    @Override
    public BytecodeSequence iterate(BytecodeExpression target, AssignableValue item, IterateLoop loop) {
        return new IterateSequence(target, valueType, item, loop);
    }

    @Override
    public BytecodeExpression first(BytecodeExpression target) {
        return new IterateFirstSequence(target, valueType);
    }

    @Override
    public BytecodeExpression streamFlattener() {
        return new InvokeExpression(StreamsSupport.class,
                Opcodes.INVOKESTATIC,
                "collectionFlattener",
                Type.getMethodDescriptor(Type.getType(Function.class)),
                new FunctionTypeWidget(),
                null,
                ImmutableList.of());
    }

    @Override
    public BytecodeExpression toStream(BytecodeExpression target) {
        return new InvokeExpression(StreamsSupport.class,
                Opcodes.INVOKESTATIC,
                "toStream",
                Type.getMethodDescriptor(Type.getType(Stream.class), Type.getType(Iterable.class)),
                new StreamTypeWidget(valueType),
                null,
                ImmutableList.of(target));
    }
}
