package com.yahoo.yqlplus.engine.internal.plan.types;

public interface StreamAdapter {
    TypeWidget getValue();
    BytecodeExpression first(BytecodeExpression target);

    BytecodeExpression collectList(BytecodeExpression streamInput);

    BytecodeExpression streamInto(BytecodeExpression streamInput, BytecodeExpression targetExpression);
}
