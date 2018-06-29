/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.compiler.exprs;

import com.yahoo.yqlplus.compiler.code.CodeEmitter;
import com.yahoo.yqlplus.compiler.code.LocalValueExpression;
import com.yahoo.yqlplus.compiler.code.TypeWidget;

public class LocalVarExpr extends LocalValueExpression {
    private String name;

    public LocalVarExpr(TypeWidget type, String name) {
        super(type);
        this.name = name;
    }

    @Override
    public void generate(CodeEmitter code) {
        code.getLocal(name).read().generate(code);
    }
}