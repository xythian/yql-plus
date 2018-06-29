/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.compiler.generate;

import com.yahoo.yqlplus.compiler.code.CodeEmitter;
import com.yahoo.yqlplus.compiler.code.TypeWidget;

class BoxedLDCConstant extends LDCConstant {

    BoxedLDCConstant(TypeWidget type, Object constant) {
        super(type, constant);
    }

    @Override
    public void generate(CodeEmitter environment) {
        super.generate(environment);
        environment.box(getType().unboxed());
    }
}