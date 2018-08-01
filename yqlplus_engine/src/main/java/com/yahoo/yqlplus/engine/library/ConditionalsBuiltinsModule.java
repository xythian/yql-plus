/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.library;

import com.yahoo.yqlplus.engine.CompileContext;
import com.yahoo.yqlplus.engine.ModuleType;
import com.yahoo.yqlplus.engine.StreamValue;
import com.yahoo.yqlplus.language.logical.ExpressionOperator;
import com.yahoo.yqlplus.language.operator.OperatorNode;
import com.yahoo.yqlplus.language.parser.Location;
import com.yahoo.yqlplus.language.parser.ProgramCompileException;
import com.yahoo.yqlplus.operator.PhysicalExprOperator;

import java.util.List;

public class ConditionalsBuiltinsModule implements ModuleType {
    @Override
    public OperatorNode<PhysicalExprOperator> call(Location location, CompileContext context, String name, List<OperatorNode<ExpressionOperator>> arguments) {
        return callInRowContext(location, context, name, arguments, null);
    }

    @Override
    public OperatorNode<PhysicalExprOperator> callInRowContext(Location location, CompileContext context, String name, List<OperatorNode<ExpressionOperator>> arguments, OperatorNode<PhysicalExprOperator> row) {
        if ("coalesce".equals(name)) {
            List<OperatorNode<PhysicalExprOperator>> args = context.evaluateAllInRowContext(arguments, row);
            return OperatorNode.create(location, PhysicalExprOperator.COALESCE, args);
        } else if ("case".equals(name)) {
            List<OperatorNode<PhysicalExprOperator>> args = context.evaluateAllInRowContext(arguments, row);
            if (arguments.size() % 2 != 1) {
                throw new ProgramCompileException(location, "case(condition-1, value-1, condition-2, value-2, ..., condition-n, value-n, default-value): arguments to CASE must be odd in number");
            }
            OperatorNode<PhysicalExprOperator> ifFalse = args.get(args.size() - 1);
            // 0 1 2 3 4
            //     ^
            // ^
            for (int i = args.size() - 2; i > 0; i -= 2) {
                ifFalse = OperatorNode.create(location, PhysicalExprOperator.IF, args.get(i - 1), args.get(i), ifFalse);
            }
            return ifFalse;
        }
        throw new ProgramCompileException(location, "Unknown conditionals function '%s'", name);
    }

    @Override
    public OperatorNode<PhysicalExprOperator> property(Location location, CompileContext context, String name) {
        return null;
    }

    @Override
    public StreamValue pipe(Location location, CompileContext context, String name, StreamValue input, List<OperatorNode<ExpressionOperator>> arguments) {
        return null;
    }
}
