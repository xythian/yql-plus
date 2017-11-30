package com.yahoo.yqlplus.engine.internal.bytecode.types.gambit;

import com.yahoo.yqlplus.engine.internal.plan.types.BytecodeExpression;
import com.yahoo.yqlplus.language.parser.Location;

public class PropertyOperation {
    final Location loc;
    final String fieldName;
    final BytecodeExpression value;

    public PropertyOperation(Location loc, String fieldName, BytecodeExpression value) {
        this.loc = loc;
        this.fieldName = fieldName;
        this.value = value;
    }

    public PropertyOperation(String fieldName, BytecodeExpression value) {
        this.loc = Location.NONE;
        this.fieldName = fieldName;
        this.value = value;
    }
}
