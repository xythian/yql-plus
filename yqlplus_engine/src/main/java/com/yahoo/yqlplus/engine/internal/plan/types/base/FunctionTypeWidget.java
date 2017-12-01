package com.yahoo.yqlplus.engine.internal.plan.types.base;

import com.yahoo.yqlplus.api.types.YQLCoreType;
import org.objectweb.asm.Type;

import java.util.function.Function;

public class FunctionTypeWidget extends BaseTypeWidget {
    public FunctionTypeWidget() {
        super(Type.getType(Function.class));
    }

    @Override
    public YQLCoreType getValueCoreType() {
        return YQLCoreType.OBJECT;
    }
}
