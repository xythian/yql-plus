/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.plan.types.base;

import com.yahoo.yqlplus.engine.internal.plan.types.BytecodeExpression;
import com.yahoo.yqlplus.engine.internal.plan.types.StreamAdapter;
import com.yahoo.yqlplus.engine.internal.plan.types.TypeWidget;

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
}
