package com.yahoo.yqlplus.engine.internal.plan.types;

@FunctionalInterface
public interface ExpressionTemplate {
    BytecodeExpression compute(BytecodeExpression input);
}
