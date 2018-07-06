/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine;

import com.google.common.util.concurrent.ListenableFuture;
import com.yahoo.yqlplus.api.trace.Tracer;

public interface ProgramResult {
    Iterable<String> getResultNames();

    ListenableFuture<YQLResultSet> getResult(String name);

    ListenableFuture<Tracer> getEnd();

}
