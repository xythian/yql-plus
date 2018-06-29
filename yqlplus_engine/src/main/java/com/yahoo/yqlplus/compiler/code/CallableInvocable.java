/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.compiler.code;

import java.util.List;

public interface CallableInvocable extends GambitCreator.Invocable {
    @Override
    CallableInvocable prefix(BytecodeExpression... arguments);

    @Override
    CallableInvocable prefix(List<BytecodeExpression> arguments);

    TypeWidget getResultType();
}