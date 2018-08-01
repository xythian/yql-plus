/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.compiler.code;

import com.yahoo.yqlplus.engine.api.PropertyNotFoundException;
import org.objectweb.asm.Label;

public abstract class OpenPropertyAdapter extends BasePropertyAdapter {
    public OpenPropertyAdapter(TypeWidget type) {
        super(type);
    }

    @Override
    public AssignableValue property(BytecodeExpression target, String propertyName) {
        return index(target, new StringConstantExpression(propertyName));
    }

    @Override
    public final Iterable<Property> getProperties() {
        throw new UnsupportedOperationException();
    }

    @Override
    public final boolean isClosed() {
        return false;
    }

    @Override
    public TypeWidget getPropertyType(String propertyName) throws PropertyNotFoundException {
        return NullableTypeWidget.create(AnyTypeWidget.getInstance());
    }

    @Override
    public abstract AssignableValue index(final BytecodeExpression target, final BytecodeExpression propertyName);

    @Override
    public BytecodeExpression property(BytecodeExpression target, String propertyName, BytecodeExpression defaultValue) {
        return index(target, new StringConstantExpression(propertyName), defaultValue);
    }

    @Override
    public BytecodeExpression index(BytecodeExpression target, BytecodeExpression propertyName, BytecodeExpression defaultValue) {
        BytecodeExpression sourceExpression = index(target, propertyName);
        return new CoalesceExpression(AnyTypeWidget.getInstance(), sourceExpression, defaultValue);
    }


    @Override
    public BytecodeSequence visitProperties(final BytecodeExpression target, final PropertyVisit loop) {
        BytecodeExpression s = getPropertyNameIterable(target);
        IterateAdapter it = s.getType().getIterableAdapter();
        return it.iterate(s, new IterateAdapter.IterateLoop() {
            @Override
            public void item(CodeEmitter code, BytecodeExpression propertyName, Label abortLoop, Label nextItem) {
                BytecodeExpression propertyValue = new BytecodeCastExpression(AnyTypeWidget.getInstance(), index(target, propertyName));
                propertyValue = code.evaluateOnce(propertyValue);
                code.gotoIfNull(propertyValue, nextItem);
                loop.item(code, propertyName, new NullCheckedEvaluatedExpression(propertyValue), abortLoop, nextItem);
            }
        });
    }
}
