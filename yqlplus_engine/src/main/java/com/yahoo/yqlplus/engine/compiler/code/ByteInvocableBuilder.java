/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.compiler.code;

public class ByteInvocableBuilder extends ExpressionHandler implements InvocableBuilder {
    private MethodGenerator generator;


    public ByteInvocableBuilder(ASMClassSource source) {
        super(source);
        this.generator = source.createInvocableMethod("invoke");
        // this isn't going to work when child expressions use things from outer scopes
        body = generator.block();
    }

    @Override
    public BytecodeExpression addArgument(String name, TypeWidget type) {
        return generator.addArgument(name, type).read();
    }

    @Override
    public Invocable complete(BytecodeExpression result) {
        BytecodeExpression expr = result;
        generator.setReturnType(expr.getType());
        body.add(new BytecodeCastExpression(generator.getReturnType(), expr));
        body.add(new ReturnCode(generator.getReturnType()));
        return generator.createInvocable();
    }
}
