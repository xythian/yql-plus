/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.compiler.code;

public interface LambdaInvocable extends GambitCreator.Invocable {
    TypeWidget getResultType();
    FunctionalInterfaceContract getContract();
}
