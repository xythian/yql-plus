/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.bytecode.types;

import com.yahoo.yqlplus.api.types.YQLCoreType;
import com.yahoo.yqlplus.engine.api.NativeEncoding;
import com.yahoo.yqlplus.engine.internal.compiler.CodeEmitter;
import com.yahoo.yqlplus.engine.internal.plan.types.BytecodeExpression;
import com.yahoo.yqlplus.engine.internal.plan.types.IndexAdapter;
import com.yahoo.yqlplus.engine.internal.plan.types.IterateAdapter;
import com.yahoo.yqlplus.engine.internal.plan.types.SerializationAdapter;
import com.yahoo.yqlplus.engine.internal.plan.types.TypeWidget;
import com.yahoo.yqlplus.engine.internal.plan.types.base.ArrayIndexAdapter;
import com.yahoo.yqlplus.engine.internal.plan.types.base.BaseTypeWidget;
import com.yahoo.yqlplus.engine.internal.plan.types.base.ComparisonAdapter;
import com.yahoo.yqlplus.engine.internal.plan.types.base.IteratingSerializing;
import org.objectweb.asm.Label;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

public class ArrayTypeWidget extends BaseTypeWidget {
    private final TypeWidget valueType;

    public ArrayTypeWidget(Class<?> arrayType, TypeWidget valueType) {
        super(Type.getType(arrayType));
        this.valueType = valueType;
    }

    public ArrayTypeWidget(Type type, TypeWidget valueType) {
        super(type);
        this.valueType = valueType;
    }


    @Override
    public YQLCoreType getValueCoreType() {
        return YQLCoreType.ARRAY;
    }

    @Override
    public BytecodeExpression construct(BytecodeExpression... arguments) {
        throw new UnsupportedOperationException();
    }

    @Override
    public IndexAdapter getIndexAdapter() {
        return new ArrayIndexAdapter(this, valueType);
    }

    @Override
    public boolean isIndexable() {
        return true;
    }

    @Override
    public IterateAdapter getIterableAdapter() {
        return new ArrayIndexAdapter(this, valueType);
    }

    @Override
    public boolean isIterable() {
        return true;
    }

    @Override
    public SerializationAdapter getSerializationAdapter(NativeEncoding encoding) {
        return new IteratingSerializing(getIterableAdapter(), encoding);
    }

    @Override
    public ComparisonAdapter getComparisionAdapter() {
        return new ComparisonAdapter() {
            @Override
            public void coerceBoolean(CodeEmitter scope, Label isTrue, Label isFalse, Label isNull) {
                // null or true
                if (isNullable()) {
                    scope.getMethodVisitor().visitJumpInsn(Opcodes.IFNULL, isNull);
                } else {
                    scope.pop(ArrayTypeWidget.this);
                }
                scope.getMethodVisitor().visitJumpInsn(Opcodes.GOTO, isTrue);
            }
        };
    }

    @Override
    public boolean isAssignableFrom(TypeWidget type) {
        return type.getValueCoreType() == YQLCoreType.ANY || getJVMType().getDescriptor().equals(type.getJVMType().getDescriptor());
    }
}
