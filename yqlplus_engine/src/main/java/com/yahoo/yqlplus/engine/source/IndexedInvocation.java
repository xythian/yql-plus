package com.yahoo.yqlplus.engine.source;

import com.yahoo.yqlplus.language.operator.OperatorNode;
import com.yahoo.yqlplus.operator.PhysicalExprOperator;

public interface IndexedInvocation {
    OperatorNode<PhysicalExprOperator> invoke();
}
