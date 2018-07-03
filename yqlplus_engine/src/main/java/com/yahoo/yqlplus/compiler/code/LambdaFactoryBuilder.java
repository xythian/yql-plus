/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.compiler.code;

import java.lang.invoke.MethodHandle;

public interface LambdaFactoryBuilder extends InvocableBuilder {
    MethodHandle getFactory() throws Throwable;

    BytecodeExpression addLambdaArgument(String name, TypeWidget type);

    @Override
    LambdaInvocable complete(BytecodeExpression result);

    LambdaInvocable exit();
}
