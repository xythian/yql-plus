/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.plan.types;

import com.yahoo.yqlplus.engine.internal.bytecode.types.gambit.ExpressionTypeTemplate;
import com.yahoo.yqlplus.engine.internal.bytecode.types.gambit.ScopedBuilder;
import com.yahoo.yqlplus.engine.internal.compiler.CodeEmitter;

public interface OptionalAdapter {
    TypeWidget getResultType();

    BytecodeExpression resolve(ScopedBuilder scope, BytecodeExpression target, ExpressionTemplate available,  ExpressionTypeTemplate missing);
    void generate(CodeEmitter code, BytecodeExpression target, ValueSequence available, ValueSequence missing);
}

