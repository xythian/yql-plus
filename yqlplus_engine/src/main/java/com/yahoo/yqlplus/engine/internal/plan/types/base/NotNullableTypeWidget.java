/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.plan.types.base;

import com.yahoo.yqlplus.api.types.YQLCoreType;
import com.yahoo.yqlplus.engine.api.NativeEncoding;
import com.yahoo.yqlplus.engine.internal.bytecode.types.gambit.ResultAdapter;
import com.yahoo.yqlplus.engine.internal.bytecode.types.gambit.ScopedBuilder;
import com.yahoo.yqlplus.engine.internal.compiler.CodeEmitter;
import com.yahoo.yqlplus.engine.internal.plan.types.BytecodeExpression;
import com.yahoo.yqlplus.engine.internal.plan.types.ExpressionTemplate;
import com.yahoo.yqlplus.engine.internal.plan.types.IndexAdapter;
import com.yahoo.yqlplus.engine.internal.plan.types.IterateAdapter;
import com.yahoo.yqlplus.engine.internal.plan.types.OptionalAdapter;
import com.yahoo.yqlplus.engine.internal.plan.types.ProgramValueTypeAdapter;
import com.yahoo.yqlplus.engine.internal.plan.types.PromiseAdapter;
import com.yahoo.yqlplus.engine.internal.plan.types.SerializationAdapter;
import com.yahoo.yqlplus.engine.internal.plan.types.StreamAdapter;
import com.yahoo.yqlplus.engine.internal.plan.types.TypeWidget;
import com.yahoo.yqlplus.engine.internal.plan.types.ValueSequence;
import org.objectweb.asm.Type;

import java.util.List;

public final class NotNullableTypeWidget implements TypeWidget {
    public static TypeWidget create(TypeWidget input) {
        if (input.isNullable()) {
            if (input instanceof NullableTypeWidget) {
                return ((NullableTypeWidget) input).getTarget();
            }
            return new NotNullableTypeWidget(input);
        } else {
            return input;
        }
    }

    private final TypeWidget target;

    public TypeWidget getTarget() {
        return target;
    }

    public NotNullableTypeWidget(TypeWidget target) {
        this.target = target;
    }

    @Override
    public YQLCoreType getValueCoreType() {
        return target.getValueCoreType();
    }

    @Override
    public Type getJVMType() {
        return target.getJVMType();
    }

    @Override
    public boolean isPrimitive() {
        return target.isPrimitive();
    }

    @Override
    public boolean isNullable() {
        return false;
    }

    @Override
    public BytecodeExpression construct(BytecodeExpression... arguments) {
        return target.construct(arguments);
    }

    @Override
    public Coercion coerceTo(BytecodeExpression source, TypeWidget target) {
        return this.target.coerceTo(source, target);
    }

    @Override
    public TypeWidget boxed() {
        return NotNullableTypeWidget.create(target.boxed());
    }

    @Override
    public TypeWidget unboxed() {
        return target.unboxed();
    }

    @Override
    public BytecodeExpression invoke(BytecodeExpression target, String methodName, List<BytecodeExpression> arguments) {
        return this.target.invoke(target, methodName, arguments);
    }

    @Override
    public BytecodeExpression invoke(BytecodeExpression target, TypeWidget outputType, String methodName, List<BytecodeExpression> arguments) {
        return this.target.invoke(target, outputType, methodName, arguments);
    }

    @Override
    public PropertyAdapter getPropertyAdapter() {
        return target.getPropertyAdapter();
    }

    @Override
    public boolean hasProperties() {
        return target.hasProperties();
    }

    @Override
    public IndexAdapter getIndexAdapter() {
        return target.getIndexAdapter();
    }

    @Override
    public boolean isIndexable() {
        return target.isIndexable();
    }

    @Override
    public IterateAdapter getIterableAdapter() {
        return target.getIterableAdapter();
    }

    @Override
    public boolean isIterable() {
        return target.isIterable();
    }

    @Override
    public SerializationAdapter getSerializationAdapter(NativeEncoding encoding) {
        return target.getSerializationAdapter(encoding);
    }

    @Override
    public ComparisonAdapter getComparisionAdapter() {
        return target.getComparisionAdapter();
    }

    @Override
    public boolean isPromise() {
        return target.isPromise();
    }

    @Override
    public PromiseAdapter getPromiseAdapter() {
        return target.getPromiseAdapter();
    }

    @Override
    public ResultAdapter getResultAdapter() {
        return target.getResultAdapter();
    }

    @Override
    public boolean isResult() {
        return target.isResult();
    }

    @Override
    public String getTypeName() {
        return target.getTypeName();
    }

    @Override
    public boolean isAssignableFrom(TypeWidget type) {
        return target.isAssignableFrom(type);
    }

    @Override
    public boolean hasUnificationAdapter() {
        return target.hasUnificationAdapter();
    }

    @Override
    public UnificationAdapter getUnificationAdapter(ProgramValueTypeAdapter typeAdapter) {
        return target.getUnificationAdapter(typeAdapter);
    }

    @Override
    public boolean isStream() {
        return target.isStream();
    }

    @Override
    public StreamAdapter getStreamAdapter() {
        return target.getStreamAdapter();
    }

    @Override
    public OptionalAdapter getOptionalAdapter() {
        return new OptionalAdapter() {
            @Override
            public TypeWidget getResultType() {
                return target;
            }

            @Override
            public BytecodeExpression resolve(ScopedBuilder scope, BytecodeExpression target, ExpressionTemplate available, ExpressionTemplate missing) {
                return available.compute(target);
            }

            @Override
            public void generate(CodeEmitter code, BytecodeExpression target, ValueSequence available, ValueSequence missing) {
                available.generate(code, target);
            }
        };
    }
}
