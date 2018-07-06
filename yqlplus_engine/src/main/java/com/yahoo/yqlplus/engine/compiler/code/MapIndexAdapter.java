/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.compiler.code;

public class MapIndexAdapter implements IndexAdapter {
    private final TypeWidget ownerType;
    private final TypeWidget keyType;
    private final TypeWidget valueType;

    public MapIndexAdapter(TypeWidget ownerType, TypeWidget keyType, TypeWidget valueType) {
        this.ownerType = ownerType;
        this.keyType = keyType;
        this.valueType = valueType;
    }

    @Override
    public TypeWidget getKey() {
        return keyType;
    }

    @Override
    public TypeWidget getValue() {
        return valueType;
    }

    @Override
    public AssignableValue index(BytecodeExpression target, BytecodeExpression indexExpression) {
        return new MapAssignableValue(getValue(), target, indexExpression);
    }

    @Override
    public BytecodeExpression length(BytecodeExpression inputExpr) {
        return new CollectionSizeExpression(inputExpr);
    }
}
