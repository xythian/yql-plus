package com.yahoo.yqlplus.engine.internal.plan;

import com.yahoo.yqlplus.engine.internal.plan.ast.FunctionOperator;
import com.yahoo.yqlplus.language.operator.OperatorNode;

import java.util.List;

public class KeyJoinDescriptor {
    private final boolean outer;
    private final List<String> keyColumns;
    // function(inputRow) -> keyRecord | Stream<keyRecord>
    private final OperatorNode<FunctionOperator> extractKey;
    // function(leftRow, rightRow) -> outputRow
    private final OperatorNode<FunctionOperator> combineOutput;

    public KeyJoinDescriptor(boolean outer, List<String> keyColumns, OperatorNode<FunctionOperator> extractKey, OperatorNode<FunctionOperator> combineOutput) {
        this.outer = outer;
        this.keyColumns = keyColumns;
        this.extractKey = extractKey;
        this.combineOutput = combineOutput;
    }

    public boolean isOuter() {
        return outer;
    }

    public List<String> getKeyColumns() {
        return keyColumns;
    }

    public OperatorNode<FunctionOperator> getExtractKey() {
        return extractKey;
    }

    public OperatorNode<FunctionOperator> getCombineOutput() {
        return combineOutput;
    }
}
