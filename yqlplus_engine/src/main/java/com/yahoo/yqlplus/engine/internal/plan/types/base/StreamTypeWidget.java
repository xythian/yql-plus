/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.plan.types.base;


import com.yahoo.yqlplus.api.types.YQLCoreType;
import com.yahoo.yqlplus.engine.api.NativeEncoding;
import com.yahoo.yqlplus.engine.internal.bytecode.IterableTypeWidget;
import com.yahoo.yqlplus.engine.internal.plan.types.BytecodeExpression;
import com.yahoo.yqlplus.engine.internal.plan.types.IndexAdapter;
import com.yahoo.yqlplus.engine.internal.plan.types.IterateAdapter;
import com.yahoo.yqlplus.engine.internal.plan.types.ProgramValueTypeAdapter;
import com.yahoo.yqlplus.engine.internal.plan.types.SerializationAdapter;
import com.yahoo.yqlplus.engine.internal.plan.types.StreamAdapter;
import com.yahoo.yqlplus.engine.internal.plan.types.TypeWidget;
import org.objectweb.asm.Type;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

public final class StreamTypeWidget extends BaseTypeWidget {
    protected final TypeWidget valueType;

    public StreamTypeWidget(TypeWidget valueType) {
        this(Type.getType(Stream.class), valueType);
    }

    public StreamTypeWidget(Type jvmType, TypeWidget valueType) {
        super(jvmType);
        this.valueType = valueType;
    }

    public StreamTypeWidget(Class<?> jvmType, TypeWidget valueType) {
        super(Type.getType(jvmType));
        this.valueType = valueType;
    }

    @Override
    public YQLCoreType getValueCoreType() {
        return YQLCoreType.ARRAY;
    }

    @Override
    public boolean hasUnificationAdapter() {
        return true;
    }

    @Override
    public UnificationAdapter getUnificationAdapter(final ProgramValueTypeAdapter typeAdapter) {
        return other -> {
            // we *know* other matches our JVM type
            return new StreamTypeWidget(typeAdapter.unifyTypes(StreamTypeWidget.this.valueType, other.getStreamAdapter().getValue()));
        };
    }

    @Override
    public boolean isStream() {
        return true;
    }

    @Override
    public StreamAdapter getStreamAdapter() {
        return new JavaStreamAdapter(valueType);
    }
}
