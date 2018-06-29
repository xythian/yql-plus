/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.compiler.types;

import com.yahoo.yqlplus.compiler.code.AssignableValue;
import com.yahoo.yqlplus.compiler.code.BytecodeExpression;
import com.yahoo.yqlplus.compiler.code.BytecodeSequence;
import com.yahoo.yqlplus.compiler.code.IterateAdapter;
import com.yahoo.yqlplus.compiler.code.TypeWidget;

public class DynamicIterateAdapter implements IterateAdapter {

    @Override
    public TypeWidget getValue() {
        return AnyTypeWidget.getInstance();
    }

    @Override
    public BytecodeSequence iterate(final BytecodeExpression target, final IterateLoop loop) {
        return new IterateSequence(target, AnyTypeWidget.getInstance(), loop);
    }

    @Override
    public BytecodeSequence iterate(BytecodeExpression target, AssignableValue item, IterateLoop loop) {
        return new IterateSequence(target, AnyTypeWidget.getInstance(), item, loop);
    }

    @Override
    public BytecodeExpression first(BytecodeExpression target) {
        return new IterateFirstSequence(target, AnyTypeWidget.getInstance());
    }
}