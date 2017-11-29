/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.plan.types.base;

import com.yahoo.yqlplus.engine.internal.compiler.CodeEmitter;
import com.yahoo.yqlplus.engine.internal.plan.types.AssignableValue;
import com.yahoo.yqlplus.engine.internal.plan.types.BytecodeExpression;
import com.yahoo.yqlplus.engine.internal.plan.types.TypeWidget;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

import java.util.Iterator;
import java.util.stream.Stream;

class StreamFirstSequence implements BytecodeExpression {
    private final BytecodeExpression target;
    private final TypeWidget valueType;
    private final TypeWidget optionalType;

    public StreamFirstSequence(BytecodeExpression target, TypeWidget valueType) {
        this.target = target;
        this.valueType = NullableTypeWidget.create(valueType.boxed());
        this.optionalType = OptionalTypeWidget.create(this.valueType);
    }

    @Override
    public TypeWidget getType() {
        return valueType;
    }

    @Override
    public void generate(CodeEmitter start) {
        CodeEmitter code = start.createScope();
        Label done = new Label();
        Label isNull = new Label();
        MethodVisitor mv = code.getMethodVisitor();
        BytecodeExpression tgt = code.evaluateOnce(target);
        tgt.generate(code);
        code.emitInstanceCheck(tgt.getType(), Stream.class, isNull);
        mv.visitMethodInsn(Opcodes.INVOKEINTERFACE, Type.getInternalName(Stream.class), "findFirst", Type.getMethodDescriptor(Type.getType(Stream.class)), true);
        AssignableValue opt = code.allocate(valueType);

        code.exec(iterator.write(code.adapt(Iterator.class)));
        code.exec(iterator.read());
        mv.visitMethodInsn(Opcodes.INVOKEINTERFACE, Type.getInternalName(Iterator.class), "hasNext", Type.getMethodDescriptor(Type.BOOLEAN_TYPE), true);
        mv.visitJumpInsn(Opcodes.IFEQ, isNull);
        code.exec(iterator.read());
        mv.visitMethodInsn(Opcodes.INVOKEINTERFACE, Type.getInternalName(Iterator.class), "next", Type.getMethodDescriptor(Type.getType(Object.class)), true);
        code.cast(valueType, AnyTypeWidget.getInstance(), isNull);
        mv.visitJumpInsn(Opcodes.GOTO, done);
        mv.visitLabel(isNull);
        mv.visitInsn(Opcodes.ACONST_NULL);
        mv.visitLabel(done);
    }
}
