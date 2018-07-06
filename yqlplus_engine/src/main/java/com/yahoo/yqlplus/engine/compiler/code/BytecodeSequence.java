/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.compiler.code;


public interface BytecodeSequence {
    BytecodeSequence NOOP = new BytecodeSequence() {
        @Override
        public void generate(CodeEmitter code) {

        }
    };

    void generate(CodeEmitter code);

}
