package com.yahoo.yqlplus.engine.internal.plan.types;

public interface StreamAdapter {
    TypeWidget getValue();
    BytecodeExpression first(BytecodeExpression target);

    BytecodeExpression collectList(BytecodeExpression streamInput);

    BytecodeExpression streamInto(BytecodeExpression streamInput, BytecodeExpression targetExpression);

    BytecodeExpression flatten(BytecodeExpression streamInput);

    BytecodeExpression offset(BytecodeExpression streamInput, BytecodeExpression offsetExpression);

    BytecodeExpression limit(BytecodeExpression streamInput, BytecodeExpression limitExpression);

    BytecodeExpression distinct(BytecodeExpression streamInput);

    BytecodeExpression skipNulls(BytecodeExpression streamInput);

    BytecodeExpression filter(BytecodeExpression streamInput, BytecodeExpression predicate);

    BytecodeExpression transform(BytecodeExpression streamInput, BytecodeExpression function, TypeWidget valueType);

    BytecodeExpression scatter(BytecodeExpression streamInput, BytecodeExpression function, TypeWidget valueType);

    BytecodeExpression sorted(BytecodeExpression streamInput, BytecodeExpression comparator);
}
