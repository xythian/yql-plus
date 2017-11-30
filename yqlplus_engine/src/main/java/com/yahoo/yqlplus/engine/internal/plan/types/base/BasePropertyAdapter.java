/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.plan.types.base;

import com.yahoo.yqlplus.engine.internal.bytecode.types.gambit.ConstructStructExpression;
import com.yahoo.yqlplus.engine.internal.bytecode.types.gambit.PropertyOperation;
import com.yahoo.yqlplus.engine.internal.compiler.CodeEmitter;
import com.yahoo.yqlplus.engine.internal.plan.types.BytecodeExpression;
import com.yahoo.yqlplus.engine.internal.plan.types.BytecodeSequence;
import com.yahoo.yqlplus.engine.internal.plan.types.TypeWidget;
import org.objectweb.asm.Label;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

import java.util.List;

public abstract class BasePropertyAdapter implements PropertyAdapter {
    protected TypeWidget type;

    public BasePropertyAdapter(TypeWidget type) {
        this.type = type;
    }

    @Override
    public final BytecodeExpression construct(final List<PropertyOperation> fields) {
        return new ConstructStructExpression(type, fields);
    }

    @Override
    public final BytecodeSequence mergeIntoFieldWriter(BytecodeExpression target, final BytecodeExpression fieldWriter) {
        return visitProperties(target, new PropertyVisit() {
            @Override
            public void item(CodeEmitter code, BytecodeExpression propertyName, BytecodeExpression propertyValue, Label abortLoop, Label nextItem) {
                code.exec(fieldWriter);
                code.exec(propertyName);
                code.exec(propertyValue);
                code.box(propertyValue.getType());
                code.getMethodVisitor()
                        .visitMethodInsn(Opcodes.INVOKEINTERFACE,
                                Type.getInternalName(FieldWriter.class),
                                "put",
                                Type.getMethodDescriptor(Type.VOID_TYPE, Type.getType(String.class), Type.getType(Object.class)),
                                true);
            }
        });
    }



}
