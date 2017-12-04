package com.yahoo.yqlplus.engine.internal.bytecode.types.gambit;

import com.yahoo.yqlplus.engine.internal.plan.types.BytecodeExpression;
import com.yahoo.yqlplus.engine.internal.plan.types.TypeWidget;

public interface ExpressionTypeTemplate {
   BytecodeExpression compute(TypeWidget availableResultType);
}
