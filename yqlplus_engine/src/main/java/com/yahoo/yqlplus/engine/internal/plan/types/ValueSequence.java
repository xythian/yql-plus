package com.yahoo.yqlplus.engine.internal.plan.types;

import com.yahoo.yqlplus.engine.internal.compiler.CodeEmitter;

public interface ValueSequence {
    void generate(CodeEmitter code, BytecodeExpression value);
}
