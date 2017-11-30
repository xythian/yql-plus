/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.plan.types.base;

import com.fasterxml.jackson.core.JsonGenerator;
import com.yahoo.yqlplus.engine.api.NativeEncoding;
import com.yahoo.yqlplus.engine.internal.plan.types.BytecodeExpression;
import com.yahoo.yqlplus.engine.internal.plan.types.BytecodeSequence;
import com.yahoo.yqlplus.engine.internal.plan.types.SerializationAdapter;
import com.yahoo.yqlplus.engine.internal.plan.types.TypeWidget;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;


public class OptionalTestingSerializationAdapter implements SerializationAdapter {
    private final TypeWidget optionalTarget;
    private final NativeEncoding encoding;
    private final SerializationAdapter targetAdapter;

    public OptionalTestingSerializationAdapter(TypeWidget target, NativeEncoding encoding) {
        this(OptionalTypeWidget.create(target), target, encoding);
    }

    public OptionalTestingSerializationAdapter(TypeWidget optionalTarget, TypeWidget target, NativeEncoding encoding) {
        this.optionalTarget = optionalTarget;
        this.encoding = encoding;
        this.targetAdapter = target.getSerializationAdapter(encoding);
    }


    @Override
    public BytecodeSequence serializeTo(final BytecodeExpression input, final BytecodeExpression generator) {
        return code -> optionalTarget.getOptionalAdapter().generate(code,
                input,
                (availCode, availValue) -> {
                    availCode.exec(targetAdapter.serializeTo(availValue, generator));
                },
                (unavailCode, unavailValue) -> {
                    switch(encoding) {
                        case JSON:
                            code.exec(generator);
                            unavailCode.getMethodVisitor().visitMethodInsn(Opcodes.INVOKEVIRTUAL,
                                    Type.getInternalName(JsonGenerator.class),
                                    "writeNull",
                                    Type.getMethodDescriptor(Type.VOID_TYPE),
                                    false);
                            break;
                        default:
                            throw new UnsupportedOperationException("unknown NativeEncoding: " + encoding);
                    }

                });
    }

    @Override
    public BytecodeExpression deserializeFrom(BytecodeExpression parser) {
        return targetAdapter.deserializeFrom(parser);
    }
}
