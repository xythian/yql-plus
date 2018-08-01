/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.library;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.yahoo.yqlplus.engine.CompileContext;
import com.yahoo.yqlplus.engine.ModuleType;
import com.yahoo.yqlplus.engine.StreamValue;
import com.yahoo.yqlplus.engine.internal.plan.ConstantExpressionEvaluator;
import com.yahoo.yqlplus.engine.internal.plan.NotConstantExpressionException;
import com.yahoo.yqlplus.language.logical.ExpressionOperator;
import com.yahoo.yqlplus.language.operator.OperatorNode;
import com.yahoo.yqlplus.language.parser.Location;
import com.yahoo.yqlplus.language.parser.ProgramCompileException;
import com.yahoo.yqlplus.operator.FunctionOperator;
import com.yahoo.yqlplus.operator.PhysicalExprOperator;
import com.yahoo.yqlplus.operator.StreamOperator;

import java.util.List;

public class SequenceBuiltinsModule implements ModuleType {
    @Override
    public OperatorNode<PhysicalExprOperator> call(Location location, CompileContext context, String name, List<OperatorNode<ExpressionOperator>> arguments) {
        return null;
    }

    @Override
    public OperatorNode<PhysicalExprOperator> callInRowContext(Location location, CompileContext context, String name, List<OperatorNode<ExpressionOperator>> arguments, OperatorNode<PhysicalExprOperator> row) {
        return null;
    }

    @Override
    public OperatorNode<PhysicalExprOperator> property(Location location, CompileContext context, String name) {
        if (name.equals("row")) {
            return OperatorNode.create(location, PhysicalExprOperator.LOCAL, "$$row");
        } else if (name.equals("key")) {
            return OperatorNode.create(location, PhysicalExprOperator.LOCAL, "$$key");
        } else if (name.equals("rows")) {
            return OperatorNode.create(location, PhysicalExprOperator.LOCAL, "$$rows");
        }
        return null;
    }

    private StreamValue dynamicGroupby(Location location, CompileContext context, StreamValue input, List<OperatorNode<ExpressionOperator>> arguments) {
        List<OperatorNode<PhysicalExprOperator>> args = context.evaluateAll(arguments);
        input.add(location, StreamOperator.GROUPBY,
                OperatorNode.create(FunctionOperator.FUNCTION, ImmutableList.of("$$row"), args.get(0)),
                OperatorNode.create(FunctionOperator.FUNCTION, ImmutableList.of("$$key", "$$rows"), args.get(1))
        );
        return input;
    }

    @Override
    public StreamValue pipe(Location location, CompileContext context, String name, StreamValue input, List<OperatorNode<ExpressionOperator>> arguments) {
        if ("groupby".equals(name)) {
            if (arguments.size() == 2) {
                return dynamicGroupby(location, context, input, arguments);
            } else if (arguments.size() != 3) {
                throw new ProgramCompileException(location, "groupby(group-field, output-group-field, output-group-rows): argument count mismatch");
            }
            ConstantExpressionEvaluator eval = new ConstantExpressionEvaluator();
            List<String> args = Lists.newArrayList();
            for (OperatorNode<ExpressionOperator> arg : arguments) {
                try {
                    Object val = eval.apply(arg);
                    if (!(val instanceof String)) {
                        throw new ProgramCompileException(location, "arguments to groupby must be string: (not string: %s)", arg);
                    }
                    args.add((String) val);
                } catch (NotConstantExpressionException e) {
                    throw new ProgramCompileException(location, "arguments to groupby must be constant: (not constant: %s)", arg);
                }
            }
            input.add(location, StreamOperator.GROUPBY,
                    OperatorNode.create(FunctionOperator.FUNCTION, ImmutableList.of("$row"), OperatorNode.create(PhysicalExprOperator.PROPREF, OperatorNode.create(PhysicalExprOperator.LOCAL, "$row"), args.get(0))),
                    OperatorNode.create(FunctionOperator.FUNCTION, ImmutableList.of("$key", "$rows"), OperatorNode.create(PhysicalExprOperator.RECORD,
                            ImmutableList.of(args.get(1), args.get(2)),
                            ImmutableList.of(OperatorNode.create(PhysicalExprOperator.LOCAL, "$key"), OperatorNode.create(PhysicalExprOperator.LOCAL, "$rows"))))
            );
            return input;
        } else if ("distinct".equals(name)) {
            if (arguments.size() != 0) {
                throw new ProgramCompileException(location, "distinct(): argument count mismatch");
            }
            input.add(location, StreamOperator.GROUPBY,
                    OperatorNode.create(FunctionOperator.FUNCTION, ImmutableList.of("$row"), OperatorNode.create(PhysicalExprOperator.LOCAL, "$row")),
                    OperatorNode.create(FunctionOperator.FUNCTION, ImmutableList.of("$key", "$rows"), OperatorNode.create(PhysicalExprOperator.LOCAL, "$key"))
            );
            return input;
        } else if ("flatten".equals(name)) {
            if (arguments.size() != 0) {
                throw new ProgramCompileException(location, "flatten(): argument count mismatch");
            }
            input.add(location, StreamOperator.FLATTEN);
            return input;
        } else if ("transform".equals(name)) {
            if (arguments.size() != 1) {
                throw new ProgramCompileException(location, "transform(<expr>): argument count mismatch");
            }
            OperatorNode<PhysicalExprOperator> arg = context.evaluate(arguments.get(0));
            input.add(location, StreamOperator.TRANSFORM,
                    OperatorNode.create(FunctionOperator.FUNCTION, ImmutableList.of("$$row"), arg));
            return input;
        } else if ("scatter".equals(name)) {
            if (arguments.size() != 1) {
                throw new ProgramCompileException(location, "scatter(<expr>): argument count mismatch");
            }
            OperatorNode<PhysicalExprOperator> arg = context.evaluate(arguments.get(0));
            input.add(location, StreamOperator.SCATTER,
                    OperatorNode.create(FunctionOperator.FUNCTION, ImmutableList.of("$$row"), arg));
            return input;
        } else if ("filter".equals(name)) {
            if (arguments.size() != 1) {
                throw new ProgramCompileException(location, "filter(<expr>): argument count mismatch");
            }
            OperatorNode<PhysicalExprOperator> arg = context.evaluate(arguments.get(0));
            input.add(location, StreamOperator.FILTER,
                    OperatorNode.create(FunctionOperator.FUNCTION, ImmutableList.of("$$row"), arg));
            return input;
        }
        throw new ProgramCompileException(location, "Unknown sequences function '%s'", name);
    }
}
