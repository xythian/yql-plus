/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.plan.types.base;

import com.yahoo.yqlplus.engine.internal.compiler.CodeEmitter;
import com.yahoo.yqlplus.engine.internal.plan.types.BytecodeExpression;
import com.yahoo.yqlplus.engine.internal.plan.types.TypeWidget;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

import java.util.stream.Stream;

class StreamFirstSequence implements BytecodeExpression {
    private final BytecodeExpression target;
    private final TypeWidget optionalType;

    public StreamFirstSequence(BytecodeExpression target, TypeWidget valueType) {
        this.target = target;
        this.optionalType = OptionalTypeWidget.create(valueType);
    }

    @Override
    public TypeWidget getType() {
        return optionalType;
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
        mv.visitJumpInsn(Opcodes.GOTO, done);
        mv.visitLabel(isNull);
        code.exec(new OptionalTypeWidget.EmptyOptionalExpression(optionalType));
        mv.visitInsn(Opcodes.ACONST_NULL);
        mv.visitLabel(done);
        code.endScope();
    }
}
