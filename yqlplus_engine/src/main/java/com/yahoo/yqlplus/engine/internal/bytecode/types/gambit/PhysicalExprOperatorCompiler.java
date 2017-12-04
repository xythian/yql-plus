/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.bytecode.types.gambit;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.inject.Injector;
import com.yahoo.cloud.metrics.api.MetricDimension;
import com.yahoo.yqlplus.engine.TaskContext;
import com.yahoo.yqlplus.engine.api.Record;
import com.yahoo.yqlplus.engine.internal.bytecode.IterableTypeWidget;
import com.yahoo.yqlplus.engine.internal.bytecode.KeyCursorTypeWidget;
import com.yahoo.yqlplus.engine.internal.bytecode.types.ArrayTypeWidget;
import com.yahoo.yqlplus.engine.internal.compiler.CodeEmitter;
import com.yahoo.yqlplus.engine.internal.generate.ProgramInvocation;
import com.yahoo.yqlplus.engine.internal.java.backends.java.KeyAccumulator;
import com.yahoo.yqlplus.engine.internal.java.backends.java.RecordAccumulator;
import com.yahoo.yqlplus.engine.internal.operations.ArithmeticOperation;
import com.yahoo.yqlplus.engine.internal.operations.BinaryComparison;
import com.yahoo.yqlplus.engine.internal.plan.ast.FunctionOperator;
import com.yahoo.yqlplus.engine.internal.plan.ast.OperatorValue;
import com.yahoo.yqlplus.engine.internal.plan.ast.PhysicalExprOperator;
import com.yahoo.yqlplus.engine.internal.plan.ast.PhysicalProjectOperator;
import com.yahoo.yqlplus.engine.internal.plan.streams.SinkOperator;
import com.yahoo.yqlplus.engine.internal.plan.streams.StreamOperator;
import com.yahoo.yqlplus.engine.internal.plan.types.BytecodeExpression;
import com.yahoo.yqlplus.engine.internal.plan.types.StreamAdapter;
import com.yahoo.yqlplus.engine.internal.plan.types.TypeWidget;
import com.yahoo.yqlplus.engine.internal.plan.types.base.AnyTypeWidget;
import com.yahoo.yqlplus.engine.internal.plan.types.base.BaseTypeAdapter;
import com.yahoo.yqlplus.engine.internal.plan.types.base.BaseTypeExpression;
import com.yahoo.yqlplus.engine.internal.plan.types.base.ListTypeWidget;
import com.yahoo.yqlplus.engine.internal.plan.types.base.NotNullableTypeWidget;
import com.yahoo.yqlplus.engine.internal.plan.types.base.PropertyAdapter;
import com.yahoo.yqlplus.engine.internal.plan.types.base.StreamTypeWidget;
import com.yahoo.yqlplus.language.operator.OperatorNode;
import com.yahoo.yqlplus.language.parser.Location;
import com.yahoo.yqlplus.language.parser.ProgramCompileException;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

import java.util.Comparator;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;

public class PhysicalExprOperatorCompiler {
    public static final MetricDimension EMPTY_DIMENSION = new MetricDimension();
    private ScopedBuilder scope;

    public PhysicalExprOperatorCompiler(ScopedBuilder scope) {
        this.scope = scope;
    }


    public BytecodeExpression evaluateExpression(final BytecodeExpression program, final BytecodeExpression context, final OperatorNode<PhysicalExprOperator> expr) {
        switch (expr.getOperator()) {
            case ASYNC_INVOKE:
            case INVOKE: {
                GambitCreator.Invocable invocable = expr.getArgument(0);
                List<OperatorNode<PhysicalExprOperator>> args = expr.getArgument(1);
                List<BytecodeExpression> arguments = evaluateExpressions(program, context, args);
                BytecodeExpression result = scope.invoke(expr.getLocation(), invocable, arguments);
                if (expr.getOperator() == PhysicalExprOperator.ASYNC_INVOKE) {
                    return scope.resolve(expr.getLocation(), getTimeout(context, expr.getLocation()), result);
                }
                return result;
            }
            case CALL: {
                TypeWidget outputType = expr.getArgument(0);
                String name = expr.getArgument(1);
                List<OperatorNode<PhysicalExprOperator>> arguments = expr.getArgument(2);
                List<BytecodeExpression> argumentExprs = evaluateExpressions(program, context, arguments);
                return scope.call(expr.getLocation(), outputType, name, argumentExprs);
            }
            case CONSTANT: {
                TypeWidget t = expr.getArgument(0);
                Object cval = expr.getArgument(1);
                return scope.constant(t, cval);
            }
            case ROOT_CONTEXT: {
                return scope.propertyValue(expr.getLocation(), program, "rootContext");
            }
            case CURRENT_CONTEXT:
                return context;
            case TRACE_CONTEXT: {
                OperatorNode<PhysicalExprOperator> attrs = expr.getArgument(0);
                final BytecodeExpression metricDimension = asMetricDimension(program, context, attrs);
                return scope.invokeExact(expr.getLocation(), "start", TaskContext.class, context.getType(), context, metricDimension);
            }
            case TIMEOUT_MAX: {
                final BytecodeExpression timeout = scope.cast(BaseTypeAdapter.INT64, evaluateExpression(program, context, expr.<OperatorNode<PhysicalExprOperator>>getArgument(0)));
                final BytecodeExpression units = evaluateExpression(program, context, expr.<OperatorNode<PhysicalExprOperator>>getArgument(1));
                return scope.invokeExact(expr.getLocation(), "timeout", TaskContext.class, context.getType(), context, timeout, units);
            }
            case TIMEOUT_GUARD: {
                final BytecodeExpression min = scope.cast(BaseTypeAdapter.INT64, evaluateExpression(program, context, expr.<OperatorNode<PhysicalExprOperator>>getArgument(0)));
                final BytecodeExpression minUnits = evaluateExpression(program, context, expr.<OperatorNode<PhysicalExprOperator>>getArgument(1));
                final BytecodeExpression max = scope.cast(BaseTypeAdapter.INT64, evaluateExpression(program, context, expr.<OperatorNode<PhysicalExprOperator>>getArgument(2)));
                final BytecodeExpression maxUnits = evaluateExpression(program, context, expr.<OperatorNode<PhysicalExprOperator>>getArgument(3));
                return scope.invokeExact(expr.getLocation(), "timeout", TaskContext.class, context.getType(), context, min, minUnits, max, maxUnits);
            }
            case END_CONTEXT: {
                final BytecodeExpression output = evaluateExpression(program, context, expr.<OperatorNode<PhysicalExprOperator>>getArgument(0));
                GambitCreator.ScopeBuilder child = scope.scope();
                BytecodeExpression result = child.evaluateInto(output);
                child.exec(child.invokeExact(expr.getLocation(), "end", TaskContext.class, BaseTypeAdapter.VOID, context));
                return child.complete(result);
            }
            case VALUE: {
                OperatorValue value = expr.getArgument(0);
                return resolveValue(expr.getLocation(), program, context, value);
            }
            case CAST: {
                final TypeWidget outputType = expr.getArgument(0);
                OperatorNode<PhysicalExprOperator> input = expr.getArgument(1);
                final BytecodeExpression output = evaluateExpression(program, context, input);
                return scope.cast(expr.getLocation(), outputType, output);
            }
            case FOREACH: {
                BytecodeExpression inputExpr = evaluateExpression(program, context, expr.<OperatorNode<PhysicalExprOperator>>getArgument(0));
                OperatorNode<FunctionOperator> function = expr.getArgument(1);
                TypeWidget itemType = inputExpr.getType().getIterableAdapter().getValue();
                GambitCreator.Invocable functionImpl = compileFunction(program.getType(), context.getType(), ImmutableList.of(itemType), function);
                return scope.transform(expr.getLocation(), inputExpr, functionImpl.prefix(program, context));
            }
            case FIRST: {
                BytecodeExpression inputExpr = evaluateExpression(program, context, expr.<OperatorNode<PhysicalExprOperator>>getArgument(0));
                return scope.first(expr.getLocation(), inputExpr);
            }
            case SINGLETON: {
                OperatorNode<PhysicalExprOperator> value = expr.getArgument(0);
                BytecodeExpression targetExpression = evaluateExpression(program, context, value);
                return scope.invokeExact(expr.getLocation(), "singleton", ProgramInvocation.class, new ListTypeWidget(targetExpression.getType()), program, scope.cast(AnyTypeWidget.getInstance(), targetExpression));
            }
            case LENGTH: {
                BytecodeExpression inputExpr = evaluateExpression(program, context, expr.<OperatorNode<PhysicalExprOperator>>getArgument(0));
                return scope.length(expr.getLocation(), inputExpr);
            }
            case PROPREF: {
                OperatorNode<PhysicalExprOperator> target = expr.getArgument(0);
                String propertyName = expr.getArgument(1);
                BytecodeExpression targetExpr = evaluateExpression(program, context, target);
                return scope.propertyValue(expr.getLocation(), targetExpr, propertyName);
            }
            case INDEX: {
                OperatorNode<PhysicalExprOperator> target = expr.getArgument(0);
                OperatorNode<PhysicalExprOperator> index = expr.getArgument(1);
                BytecodeExpression targetExpr = evaluateExpression(program, context, target);
                BytecodeExpression indexExpr = evaluateExpression(program, context, index);
                return scope.indexValue(expr.getLocation(), targetExpr, indexExpr);
            }
            case WITH_CONTEXT: {
                GambitCreator.ScopeBuilder contextScope = scope.scope();
                PhysicalExprOperatorCompiler compiler = new PhysicalExprOperatorCompiler(contextScope);
                BytecodeExpression ctxExpr = contextScope.evaluateInto(compiler.evaluateExpression(program, context, expr.<OperatorNode<PhysicalExprOperator>>getArgument(0)));
                OperatorNode<PhysicalExprOperator> exprTarget = expr.getArgument(1);
                BytecodeExpression resultExpr = compiler.evaluateExpression(program, ctxExpr, OperatorNode.create(PhysicalExprOperator.END_CONTEXT, exprTarget));
                return contextScope.complete(resultExpr);
            }
            case CONCAT: {
                List<OperatorNode<PhysicalExprOperator>> iterables = expr.getArgument(0);
                List<BytecodeExpression> exprs = evaluateExpressions(program, context, iterables);
                List<TypeWidget> types = Lists.newArrayList();
                for (BytecodeExpression e : exprs) {
                    types.add(e.getType().getIterableAdapter().getValue());
                }

                return scope.cast(expr.getLocation(), new IterableTypeWidget(scope.unify(types)),
                        ExactInvocation.boundInvoke(Opcodes.INVOKESTATIC, "concat", scope.adapt(Iterables.class, false),
                                scope.adapt(Iterable.class, false),
                                scope.cast(scope.adapt(Iterable.class, false),
                                        scope.list(expr.getLocation(), exprs))).invoke(expr.getLocation()));
            }
            case ENFORCE_TIMEOUT: {
                List<BytecodeExpression> localExprs = Lists.newArrayList();
                List<TypeWidget> types = Lists.newArrayList();
                List<String> localNames = expr.getOperator().localsFor(expr);
                localExprs.add(program);
                localExprs.add(context);
                for (String local : localNames) {
                    BytecodeExpression arg = scope.local(expr.getLocation(), local);
                    localExprs.add(arg);
                    types.add(arg.getType());
                }
                CallableInvocable invocation = compileCallable(program.getType(), context.getType(), types,
                        OperatorNode.create(FunctionOperator.FUNCTION, localNames, expr.getArgument(0)));
                final BytecodeExpression timeout = getTimeout(context, expr.getLocation());
                return scope.resolve(expr.getLocation(), timeout, scope.fork(expr.getLocation(), getRuntime(scope, program, context), invocation, localExprs));
            }
            case LOCAL: {
                String localName = expr.getArgument(0);
                return scope.local(expr.getLocation(), localName);
            }
            case EQ:
            case NEQ: {
                OperatorNode<PhysicalExprOperator> left = expr.getArgument(0);
                OperatorNode<PhysicalExprOperator> right = expr.getArgument(1);
                final BytecodeExpression leftExpr = evaluateExpression(program, context, left);
                final BytecodeExpression rightExpr = evaluateExpression(program, context, right);
                return expr.getOperator() == PhysicalExprOperator.EQ ? scope.eq(expr.getLocation(), leftExpr, rightExpr)
                        : scope.neq(expr.getLocation(), leftExpr, rightExpr);
            }
            case BOOLEAN_COMPARE: {
                final BinaryComparison booleanComparison = expr.getArgument(0);
                OperatorNode<PhysicalExprOperator> left = expr.getArgument(1);
                OperatorNode<PhysicalExprOperator> right = expr.getArgument(2);
                final BytecodeExpression leftExpr = evaluateExpression(program, context, left);
                final BytecodeExpression rightExpr = evaluateExpression(program, context, right);
                return scope.compare(expr.getLocation(), booleanComparison, leftExpr, rightExpr);
            }

            case BINARY_MATH: {
                final ArithmeticOperation arithmeticOperation = expr.getArgument(0);
                OperatorNode<PhysicalExprOperator> left = expr.getArgument(1);
                OperatorNode<PhysicalExprOperator> right = expr.getArgument(2);
                final BytecodeExpression leftExpr = evaluateExpression(program, context, left);
                final BytecodeExpression rightExpr = evaluateExpression(program, context, right);
                return scope.arithmetic(expr.getLocation(), arithmeticOperation, leftExpr, rightExpr);
            }
            case COMPARE: {
                OperatorNode<PhysicalExprOperator> left = expr.getArgument(0);
                OperatorNode<PhysicalExprOperator> right = expr.getArgument(1);
                final BytecodeExpression leftExpr = evaluateExpression(program, context, left);
                final BytecodeExpression rightExpr = evaluateExpression(program, context, right);
                return scope.compare(expr.getLocation(), leftExpr, rightExpr);
            }
            case MULTICOMPARE: {
                List<OperatorNode<PhysicalExprOperator>> exprs = expr.getArgument(0);
                List<BytecodeExpression> expressions = evaluateExpressions(program, context, exprs);
                return scope.composeCompare(expressions);
            }
            case COALESCE: {
                List<OperatorNode<PhysicalExprOperator>> exprs = expr.getArgument(0);
                List<BytecodeExpression> expressions = evaluateExpressions(program, context, exprs);
                return scope.coalesce(expr.getLocation(), expressions);
            }
            case IF: {
                OperatorNode<PhysicalExprOperator> test = expr.getArgument(0);
                OperatorNode<PhysicalExprOperator> ifTrue = expr.getArgument(1);
                OperatorNode<PhysicalExprOperator> ifFalse = expr.getArgument(2);
                return handleIfTail(program, context, scope.createCase(), test, ifTrue, ifFalse);
            }
            case STREAM_EXECUTE: {
                OperatorNode<PhysicalExprOperator> input = expr.getArgument(0);
                OperatorNode<StreamOperator> stream = expr.getArgument(1);
                return streamExecute(program, context, input, stream);
            }
            case STREAM_CREATE: {
                OperatorNode<StreamOperator> t = expr.getArgument(0);
                return compileStreamCreate(program, context, t);
            }
            case STREAM_COMPLETE: {
                OperatorNode<PhysicalExprOperator> streamExpression = expr.getArgument(0);
                BytecodeExpression streamExpr = evaluateExpression(program, context, streamExpression);
                return scope.invoke(expr.getLocation(),
                        ExactInvocation.boundInvoke(Opcodes.INVOKEVIRTUAL, "complete", streamExpr.getType(),
                                // ideally this would unify the types of the input streams
                                new ListTypeWidget(AnyTypeWidget.getInstance()), streamExpr));
            }
            case RECORD: {
                List<String> names = expr.getArgument(0);
                List<OperatorNode<PhysicalExprOperator>> exprs = expr.getArgument(1);
                List<BytecodeExpression> evaluated = evaluateExpressions(program, context, exprs);
                GambitCreator.RecordBuilder recordBuilder = scope.record();
                for (int i = 0; i < names.size(); ++i) {
                    recordBuilder.add(expr.getLocation(), names.get(i), evaluated.get(i));
                }
                return recordBuilder.build();
            }
            case RECORD_AS: {
                TypeWidget recordType = expr.getArgument(0);
                List<String> names = expr.getArgument(1);
                List<OperatorNode<PhysicalExprOperator>> exprs = expr.getArgument(2);
                List<BytecodeExpression> evaluated = evaluateExpressions(program, context, exprs);
                List<PropertyOperation> sets = Lists.newArrayList();
                for (int i = 0; i < names.size(); ++i) {
                    sets.add(new PropertyOperation(expr.getLocation(), names.get(i), evaluated.get(i)));
                }
                if (!recordType.hasProperties()) {
                    throw new ProgramCompileException(expr.getLocation(), "Type passed to RECORD_AS has no properties", recordType.getTypeName());
                }
                return recordType.getPropertyAdapter().construct(sets);
            }
            case PROJECT: {
                List<OperatorNode<PhysicalProjectOperator>> operations = expr.getArgument(0);
                GambitCreator.RecordBuilder recordBuilder;
                if("map".equals(expr.getAnnotation("project:type"))) {
                    recordBuilder = scope.dynamicRecord();
                } else {
                    recordBuilder = scope.record();
                }
                for(OperatorNode<PhysicalProjectOperator> op : operations) {
                    switch(op.getOperator()) {
                        case FIELD: {
                            OperatorNode<PhysicalExprOperator> fieldValue = op.getArgument(0);
                            String fieldName = op.getArgument(1);
                            recordBuilder.add(expr.getLocation(), fieldName, evaluateExpression(program, context, fieldValue));
                            break;
                        }
                        case MERGE: {
                            OperatorNode<PhysicalExprOperator> fieldValue = op.getArgument(0);
                            recordBuilder.merge(expr.getLocation(), evaluateExpression(program, context, fieldValue));
                            break;
                        }
                        default:
                            throw new UnsupportedOperationException("Unknown PhysicalProjectOperator: " + op.getOperator());
                    }
                }
                return recordBuilder.build();

            }
            case NULL: {
                TypeWidget type = expr.getArgument(0);
                return scope.nullFor(type);
            }
            case GENERATE_KEYS: {
                List<String> names = expr.getArgument(0);
                List<OperatorNode<PhysicalExprOperator>> valueLists = expr.getArgument(1);
                List<BytecodeExpression> insns = Lists.newArrayListWithCapacity(names.size() * 2);
                for (int i = 0; i < names.size(); ++i) {
                    insns.add(scope.constant(names.get(i)));
                    BytecodeExpression keyExpr = evaluateExpression(program, context, valueLists.get(i));
                    insns.add(scope.cast(AnyTypeWidget.getInstance(), keyExpr));
                }

                BytecodeExpression arr = scope.array(expr.getLocation(), BaseTypeAdapter.ANY, insns);
                GambitCreator.Invocable factory = scope.constructor(keyCursorFor(scope, names), arr.getType());
                return scope.invoke(expr.getLocation(), factory, arr);
            }
            case OR: {
                List<OperatorNode<PhysicalExprOperator>> args = expr.getArgument(0);
                return scope.or(expr.getLocation(), evaluateExpressions(program, context, args));
            }
            case AND: {
                List<OperatorNode<PhysicalExprOperator>> args = expr.getArgument(0);
                return scope.and(expr.getLocation(), evaluateExpressions(program, context, args));
            }
            case IN: {
                OperatorNode<PhysicalExprOperator> left = expr.getArgument(0);
                OperatorNode<PhysicalExprOperator> right = expr.getArgument(1);
                return scope.in(expr.getLocation(),
                        evaluateExpression(program, context, left),
                        evaluateExpression(program, context, right));
            }
            case CONTAINS: {
                OperatorNode<PhysicalExprOperator> left = expr.getArgument(0);
                OperatorNode<PhysicalExprOperator> right = expr.getArgument(1);
                return scope.contains(expr.getLocation(),
                        evaluateExpression(program, context, left),
                        evaluateExpression(program, context, right));
            }
            case MATCHES: {
                OperatorNode<PhysicalExprOperator> left = expr.getArgument(0);
                OperatorNode<PhysicalExprOperator> right = expr.getArgument(1);
                return scope.matches(expr.getLocation(),
                        evaluateExpression(program, context, left),
                        evaluateExpression(program, context, right));
            }
            case NOT: {
                OperatorNode<PhysicalExprOperator> target = expr.getArgument(0);
                return scope.not(expr.getLocation(), evaluateExpression(program, context, target));
            }
            case NEGATE: {
                OperatorNode<PhysicalExprOperator> target = expr.getArgument(0);
                return scope.negate(expr.getLocation(), evaluateExpression(program, context, target));
            }
            case IS_NULL: {
                OperatorNode<PhysicalExprOperator> target = expr.getArgument(0);
                return scope.isNull(expr.getLocation(), evaluateExpression(program, context, target));
            }
            case BOOL: {
                OperatorNode<PhysicalExprOperator> target = expr.getArgument(0);
                return scope.bool(expr.getLocation(), evaluateExpression(program, context, target));
            }
            case ARRAY: {
                List<OperatorNode<PhysicalExprOperator>> args = expr.getArgument(0);
                return scope.list(expr.getLocation(), evaluateExpressions(program, context, args));
            }
            case CATCH: {
                OperatorNode<PhysicalExprOperator> primary = expr.getArgument(0);
                OperatorNode<PhysicalExprOperator> fallback = expr.getArgument(1);
                return scope.fallback(expr.getLocation(),
                        evaluateExpression(program, context, primary),
                        evaluateExpression(program, context, fallback));
            }
            case NEW: {
                TypeWidget type = expr.getArgument(0);
                List<OperatorNode<PhysicalExprOperator>> valueLists = expr.getArgument(1);
                List<BytecodeExpression> exprs = evaluateExpressions(program, context, valueLists);
                List<TypeWidget> types = Lists.newArrayList();
                for (BytecodeExpression e : exprs) {
                    types.add(e.getType());
                }
                return scope.invoke(expr.getLocation(), scope.constructor(type, types), exprs);
            }
            case INJECT_MEMBERS: {
                OperatorNode<PhysicalExprOperator> value = expr.getArgument(0);
                final BytecodeExpression result = evaluateExpression(program, context, value);
                final BytecodeExpression injector = getInjector(program, context);
                return new BaseTypeExpression(result.getType()) {
                    @Override
                    public void generate(CodeEmitter code) {
                        code.exec(result);
                        code.dup(result.getType());
                        code.exec(injector);
                        code.swap(injector.getType(), result.getType());
                        code.getMethodVisitor().visitMethodInsn(Opcodes.INVOKEINTERFACE, Type.getInternalName(Injector.class),
                                "injectMembers", Type.getMethodDescriptor(Type.VOID_TYPE, Type.getType(Object.class)), true);
                    }
                };


            }
            default:
                throw new ProgramCompileException("Unimplemented PhysicalExprOperator: " + expr.toString());
        }
    }

    private BytecodeExpression handleIfTail(BytecodeExpression program, BytecodeExpression context, GambitCreator.CaseBuilder caseBuilder, OperatorNode<PhysicalExprOperator> test, OperatorNode<PhysicalExprOperator> ifTrue, OperatorNode<PhysicalExprOperator> ifFalse) {
        caseBuilder.when(evaluateExpression(program, context, test), evaluateExpression(program, context, ifTrue));
        if(ifFalse.getOperator() == PhysicalExprOperator.IF) {
            OperatorNode<PhysicalExprOperator> nextTest = ifFalse.getArgument(0);
            OperatorNode<PhysicalExprOperator> nextTruth = ifFalse.getArgument(1);
            OperatorNode<PhysicalExprOperator> nextFalse = ifFalse.getArgument(2);
            return handleIfTail(program, context, caseBuilder, nextTest, nextTruth, nextFalse);
        }
        return caseBuilder.exit(evaluateExpression(program, context, ifFalse));
    }


    private BytecodeExpression getTimeout(BytecodeExpression context, Location loc) {
        return scope.propertyValue(loc, context, "timeout");
    }

    private BytecodeExpression compileStreamCreate(BytecodeExpression program, BytecodeExpression context, OperatorNode<StreamOperator> node) {
        ObjectBuilder stream = scope.createObject(RecordAccumulator.class);
        stream.addParameter("$program", program.getType());
        stream.addParameter("$context", context.getType());
        ObjectBuilder.MethodBuilder finish = stream.method("finish");
        BytecodeExpression input = finish.addArgument("input", new StreamTypeWidget(AnyTypeWidget.getInstance()));
        GambitCreator.ScopeBuilder scope = finish.scope();
        BytecodeExpression streamResult = compileStreamExpression(scope, scope.local("$program"), scope.local("$context"), node, input);
        finish.exit(scope.complete(streamResult));
        return scope.invoke(node.getLocation(), scope.constructor(stream.type(), program.getType(), context.getType()).prefix(program, context));
    }

    public BytecodeExpression resolveValue(Location loc, BytecodeExpression program, BytecodeExpression ctx, OperatorValue value) {
        String name = value.getName();
        if (name == null) {
            throw new ProgramCompileException("Unnamed OperatorValue");
        }
        BytecodeExpression valueExpr = scope.propertyValue(loc, program, name);
        return scope.resolve(loc, getTimeout(ctx, loc), valueExpr);
    }

    private GambitCreator.Invocable compileFunction(TypeWidget programType, TypeWidget contextType, List<TypeWidget> argumentTypes, OperatorNode<FunctionOperator> function) {
        List<String> argumentNames = function.getArgument(0);
        OperatorNode<PhysicalExprOperator> functionBody = function.getArgument(1);
        InvocableBuilder out = this.scope.createInvocable();
        out.addArgument("$program", programType);
        out.addArgument("$context", contextType);
        for (int i = 0; i < argumentNames.size(); ++i) {
            out.addArgument(argumentNames.get(i), argumentTypes.get(i));
        }
        PhysicalExprOperatorCompiler compiler = new PhysicalExprOperatorCompiler(out);
        BytecodeExpression result = compiler.evaluateExpression(out.local("$program"), out.local("$context"), functionBody);
        return out.complete(result);
    }

    private CallableInvocable compileCallable(TypeWidget programType, TypeWidget contextType, List<TypeWidget> argumentTypes, OperatorNode<FunctionOperator> function) {
        List<String> argumentNames = function.getArgument(0);
        OperatorNode<PhysicalExprOperator> functionBody = function.getArgument(1);
        CallableInvocableBuilder builder = this.scope.createInvocableCallable();
        builder.addArgument("$program", programType);
        builder.addArgument("$context", contextType);
        for (int i = 0; i < argumentNames.size(); ++i) {
            builder.addArgument(argumentNames.get(i), argumentTypes.get(i));
        }
        PhysicalExprOperatorCompiler compiler = new PhysicalExprOperatorCompiler(builder);
        BytecodeExpression result = compiler.evaluateExpression(builder.local("$program"), builder.local("$context"), functionBody);
        return builder.complete(result);
    }


    static class LambdaCallable {
        private final TypeWidget lambda;
        private final TypeWidget resultType;

        public LambdaCallable(TypeWidget lambda, TypeWidget resultType) {
            this.lambda = lambda;
            this.resultType = resultType;
        }

        public TypeWidget getLambda() {
            return lambda;
        }

        public BytecodeExpression create(BytecodeExpression program, BytecodeExpression contextExpr) {
            return lambda.construct(program, contextExpr);
        }

        public TypeWidget getResultType() {
            return resultType;
        }
    }

    private LambdaCallable compilePredicate(TypeWidget programType, TypeWidget contextType, TypeWidget itemType, OperatorNode<FunctionOperator> function) {
        List<String> argumentNames = function.getArgument(0);
        OperatorNode<PhysicalExprOperator> functionBody = function.getArgument(1);
        ObjectBuilder builder = this.scope.createObject();
        builder.implement(Predicate.class);
        builder.addParameter("$program", programType);
        builder.addParameter("$context", contextType);
        ObjectBuilder.MethodBuilder apply = builder.method("test");
        BytecodeExpression leftExpr = apply.addArgument("$item$", AnyTypeWidget.getInstance());
        apply.evaluateInto(argumentNames.get(0),
                apply.cast(function.getLocation(), itemType, leftExpr));
        PhysicalExprOperatorCompiler compiler = new PhysicalExprOperatorCompiler(apply);
        BytecodeExpression result = compiler.evaluateExpression(apply.local("$program"), apply.local("$context"), functionBody);
        apply.exit(result);
        return new LambdaCallable(builder.type(), result.getType());
    }

    private LambdaCallable compileFunctionLambda(TypeWidget programType, TypeWidget contextType, TypeWidget itemType, OperatorNode<FunctionOperator> function) {
        List<String> argumentNames = function.getArgument(0);
        OperatorNode<PhysicalExprOperator> functionBody = function.getArgument(1);
        ObjectBuilder builder = this.scope.createObject();
        builder.implement(Function.class);
        builder.addParameter("$program", programType);
        builder.addParameter("$context", contextType);
        ObjectBuilder.MethodBuilder apply = builder.method("apply");
        BytecodeExpression leftExpr = apply.addArgument("$item$", AnyTypeWidget.getInstance());
        apply.evaluateInto(argumentNames.get(0),
                apply.cast(function.getLocation(), itemType, leftExpr));
        PhysicalExprOperatorCompiler compiler = new PhysicalExprOperatorCompiler(apply);
        BytecodeExpression result = compiler.evaluateExpression(apply.local("$program"), apply.local("$context"), functionBody);
        apply.exit(apply.cast(Location.NONE, AnyTypeWidget.getInstance(), result));
        return new LambdaCallable(builder.type(), result.getType());
    }

    private LambdaCallable compileBiFunctionLambda(TypeWidget programType, TypeWidget contextType, TypeWidget arg1Type, TypeWidget arg2Type, OperatorNode<FunctionOperator> function) {
        List<String> argumentNames = function.getArgument(0);
        OperatorNode<PhysicalExprOperator> functionBody = function.getArgument(1);
        ObjectBuilder builder = this.scope.createObject();
        builder.implement(BiFunction.class);
        builder.addParameter("$program", programType);
        builder.addParameter("$context", contextType);
        ObjectBuilder.MethodBuilder apply = builder.method("apply");
        BytecodeExpression arg1Expr = apply.addArgument("$arg1", AnyTypeWidget.getInstance());
        BytecodeExpression arg2Expr = apply.addArgument("$arg2", AnyTypeWidget.getInstance());
        apply.evaluateInto(argumentNames.get(0),
                apply.cast(function.getLocation(), arg1Type, arg1Expr));
        apply.evaluateInto(argumentNames.get(1),
                apply.cast(function.getLocation(), arg2Type, arg2Expr));
        PhysicalExprOperatorCompiler compiler = new PhysicalExprOperatorCompiler(apply);
        BytecodeExpression result = compiler.evaluateExpression(apply.local("$program"), apply.local("$context"), functionBody);
        apply.exit(apply.cast(Location.NONE, AnyTypeWidget.getInstance(), result));
        return new LambdaCallable(builder.type(), result.getType());
    }

    private LambdaCallable compileComparator(TypeWidget programType, TypeWidget contextType, TypeWidget itemType, OperatorNode<FunctionOperator> function) {
        // TODO Comparator.class - int compare(Object left, Object right);
        List<String> argumentNames = function.getArgument(0);
        OperatorNode<PhysicalExprOperator> functionBody = function.getArgument(1);
        ObjectBuilder builder = this.scope.createObject();
        builder.implement(Comparator.class);
        builder.addParameter("$program", programType);
        builder.addParameter("$context", contextType);
        ObjectBuilder.MethodBuilder compareMethod = builder.method("compare");
        BytecodeExpression leftExpr = compareMethod.addArgument("left", AnyTypeWidget.getInstance());
        BytecodeExpression rightExpr = compareMethod.addArgument("right", AnyTypeWidget.getInstance());
        compareMethod.evaluateInto(argumentNames.get(0),
                compareMethod.cast(function.getLocation(), itemType, leftExpr));
        compareMethod.evaluateInto(argumentNames.get(1),
                compareMethod.cast(function.getLocation(), itemType, rightExpr));
        PhysicalExprOperatorCompiler compiler = new PhysicalExprOperatorCompiler(compareMethod);
        BytecodeExpression result = compiler.evaluateExpression(compareMethod.local("$program"), compareMethod.local("$context"), functionBody);
        compareMethod.exit(result);
        return new LambdaCallable(builder.type(), result.getType());
    }


    private BytecodeExpression asMetricDimension(BytecodeExpression program, BytecodeExpression context, OperatorNode<PhysicalExprOperator> attrs) {
        List<String> keys = attrs.getArgument(0);
        List<OperatorNode<PhysicalExprOperator>> vals = attrs.getArgument(1);
        MetricDimension dims = EMPTY_DIMENSION;
        for (int i = 0; i < keys.size(); ++i) {
            String key = keys.get(i);
            OperatorNode<PhysicalExprOperator> expr = vals.get(i);
            if (expr.getOperator() == PhysicalExprOperator.CONSTANT) {
                String val = expr.getArgument(1).toString();
                dims = dims.with(key, val);
            } else {
                return dynamicMetricDimension(program, context, attrs);
            }
        }
        return scope.constant(dims);
    }

    private BytecodeExpression dynamicMetricDimension(BytecodeExpression program, BytecodeExpression context, OperatorNode<PhysicalExprOperator> attrs) {
        List<String> keys = attrs.getArgument(0);
        List<OperatorNode<PhysicalExprOperator>> vals = attrs.getArgument(1);
        final List<BytecodeExpression> valueExprs = evaluateExpressions(program, context, vals);
        BytecodeExpression metric = scope.constant(EMPTY_DIMENSION);
        for (int i = 0; i < keys.size(); ++i) {
            BytecodeExpression key = scope.constant(keys.get(i));
            BytecodeExpression value = scope.cast(vals.get(i).getLocation(), BaseTypeAdapter.STRING, valueExprs.get(i));
            metric = scope.invokeExact(attrs.getLocation(), "with", MetricDimension.class, scope.adapt(MetricDimension.class, false), metric, key, value);
        }
        return metric;
    }

    private List<BytecodeExpression> evaluateExpressions(BytecodeExpression program, BytecodeExpression context, List<OperatorNode<PhysicalExprOperator>> vals) {
        List<BytecodeExpression> output = Lists.newArrayListWithExpectedSize(vals.size());
        for (OperatorNode<PhysicalExprOperator> expr : vals) {
            output.add(evaluateExpression(program, context, expr));
        }
        return output;
    }

    private static BytecodeExpression getRuntime(ScopedBuilder scope, BytecodeExpression program, BytecodeExpression context) {
        return ExactInvocation.boundInvoke(Opcodes.INVOKEVIRTUAL, "getRuntime", scope.adapt(ProgramInvocation.class, false), scope.adapt(GambitRuntime.class, false), program, context).invoke(Location.NONE);
    }

    private BytecodeExpression getInjector(BytecodeExpression program, BytecodeExpression context) {
        return ExactInvocation.boundInvoke(Opcodes.INVOKEVIRTUAL, "getInjector", scope.adapt(ProgramInvocation.class, false), scope.adapt(Injector.class, false), program).invoke(Location.NONE);
    }

    private BytecodeExpression streamExecute(final BytecodeExpression program, final BytecodeExpression ctxExpr, OperatorNode<PhysicalExprOperator> input, OperatorNode<StreamOperator> stream) {
        BytecodeExpression streamInput = evaluateExpression(program, ctxExpr, input);
        GambitCreator.ScopeBuilder scope = this.scope.scope();
        final BytecodeExpression timeout = getTimeout(ctxExpr, input.getLocation());
        streamInput = scope.resolve(input.getLocation(), timeout, streamInput);
        if(streamInput.getType().isIterable()) {
            streamInput = streamInput.getType().getIterableAdapter().toStream(streamInput);
            return compileStreamExpression(scope, program, ctxExpr, stream, streamInput);
        } else if (streamInput.getType().isStream()) {
            return compileStreamExpression(scope, program, ctxExpr, stream, streamInput);
         } else {
            throw new UnsupportedOperationException("streamExecute argument must be iterable; type is " + streamInput.getType());

        }
    }

    private BytecodeExpression compileStreamExpression(GambitCreator.ScopeBuilder scope, BytecodeExpression program, BytecodeExpression ctxExpr, OperatorNode<StreamOperator> streamOperator, BytecodeExpression streamInput) {
        if(!streamInput.getType().isStream()) {
            if(streamInput.getType().isIterable()) {
                streamInput = streamInput.getType().getIterableAdapter().toStream(streamInput);
            }
        }
        StreamAdapter adapter = streamInput.getType().getStreamAdapter();
        if (streamOperator.getOperator() == StreamOperator.SINK) {
            OperatorNode<SinkOperator> sink = streamOperator.getArgument(0);
            switch (sink.getOperator()) {
                case ACCUMULATE:
                    return adapter.collectList(streamInput);
                case STREAM: {
                    OperatorNode<PhysicalExprOperator> target = sink.getArgument(0);
                    BytecodeExpression targetExpression = evaluateExpression(program, ctxExpr, target);
                    return adapter.streamInto(streamInput, targetExpression);
                }
                default:
                    throw new UnsupportedOperationException("Unknown SINK operator: " + sink);
            }
        }
        OperatorNode<StreamOperator> nextStream = streamOperator.getArgument(0);
        switch (streamOperator.getOperator()) {
            case TRANSFORM: {
                OperatorNode<FunctionOperator> function = streamOperator.getArgument(1);
                LambdaCallable functionType = compileFunctionLambda(program.getType(), ctxExpr.getType(), adapter.getValue(), function);
                BytecodeExpression filtered = adapter.transform(streamInput, functionType.create(program, ctxExpr), functionType.resultType);
                return compileStreamExpression(scope, program, ctxExpr, nextStream, filtered);
            }
            case SCATTER: {
                OperatorNode<FunctionOperator> function = streamOperator.getArgument(1);
                LambdaCallable functionType = compileFunctionLambda(program.getType(), ctxExpr.getType(), adapter.getValue(), function);
                BytecodeExpression filtered = adapter.scatter(streamInput, functionType.create(program, ctxExpr), functionType.resultType);
                return compileStreamExpression(scope, program, ctxExpr, nextStream, filtered);
            }
            case DISTINCT: {
                return compileStreamExpression(scope, program, ctxExpr, nextStream, adapter.distinct(streamInput));
            }
            case FLATTEN: {
                BytecodeExpression flattened = adapter.flatten(streamInput);
                return compileStreamExpression(scope, program, ctxExpr, nextStream, flattened);
            }
            case FILTER: {
                OperatorNode<FunctionOperator> function = streamOperator.getArgument(1);
                LambdaCallable predicateType = compilePredicate(program.getType(), ctxExpr.getType(), adapter.getValue(), function);
                BytecodeExpression filtered = adapter.filter(streamInput, predicateType.create(program, ctxExpr));
                return compileStreamExpression(scope, program, ctxExpr, nextStream, filtered);
            }
            case OFFSET: {
                OperatorNode<PhysicalExprOperator> offset = streamOperator.getArgument(1);
                BytecodeExpression offsetExpression = evaluateExpression(program, ctxExpr, offset);
                BytecodeExpression offsetStream = adapter.offset(streamInput, offsetExpression);
                return compileStreamExpression(scope, program, ctxExpr, nextStream, offsetStream);
            }
            case LIMIT: {
                OperatorNode<PhysicalExprOperator> limit = streamOperator.getArgument(1);
                BytecodeExpression limitExpression = evaluateExpression(program, ctxExpr, limit);
                BytecodeExpression limitStream = adapter.limit(streamInput, limitExpression);
                return compileStreamExpression(scope, program, ctxExpr, nextStream, limitStream);
            }
            case SLICE: {
                OperatorNode<PhysicalExprOperator> offset = streamOperator.getArgument(1);
                OperatorNode<PhysicalExprOperator> limit = streamOperator.getArgument(2);
                BytecodeExpression offsetExpression = evaluateExpression(program, ctxExpr, offset);
                BytecodeExpression limitExpression = evaluateExpression(program, ctxExpr, limit);
                BytecodeExpression offsetStream = adapter.offset(streamInput, offsetExpression);
                return compileStreamExpression(scope, program, ctxExpr, nextStream, offsetStream.getType().getStreamAdapter().limit(offsetStream, limitExpression));
            }
            case ORDERBY: {
                OperatorNode<FunctionOperator> comparator = streamOperator.getArgument(1);
                LambdaCallable functionType = compileComparator(program.getType(), ctxExpr.getType(), adapter.getValue(), comparator);
                BytecodeExpression sorted = adapter.sorted(streamInput, functionType.create(program, ctxExpr));
                return compileStreamExpression(scope, program, ctxExpr, nextStream, sorted);
            }
            case GROUPBY: {
                OperatorNode<FunctionOperator> key = streamOperator.getArgument(1);
                OperatorNode<FunctionOperator> output = streamOperator.getArgument(2);
                LambdaCallable keyFunction = compileFunctionLambda(program.getType(), ctxExpr.getType(), adapter.getValue(), key);
                LambdaCallable outputFunction = compileBiFunctionLambda(program.getType(), ctxExpr.getType(), keyFunction.resultType, new ListTypeWidget(adapter.getValue()), output);
                BytecodeExpression grouped = adapter.groupBy(streamInput, keyFunction.create(program, ctxExpr), outputFunction.create(program, ctxExpr), outputFunction.resultType);
                return compileStreamExpression(scope, program, ctxExpr, nextStream, grouped);
            }
            case CROSS: {
                OperatorNode<PhysicalExprOperator> right = streamOperator.getArgument(1);
                BytecodeExpression rightExpr = evaluateExpression(program, ctxExpr, right);
                OperatorNode<FunctionOperator> output = streamOperator.getArgument(2);
                LambdaCallable outputFunction = compileBiFunctionLambda(program.getType(), ctxExpr.getType(), adapter.getValue(), rightExpr.getType().getIterableAdapter().getValue(), output);
                TypeWidget outputType = outputFunction.resultType;
                if (outputType.isIterable()) {
                    outputType = outputType.getIterableAdapter().getValue();
                } else if(outputType.isStream()) {
                    outputType = outputType.getStreamAdapter().getValue();
                }
                BytecodeExpression crossed = adapter.cross(streamInput, rightExpr, outputFunction.create(program, ctxExpr), outputType);
                return compileStreamExpression(scope, program, ctxExpr, nextStream, crossed);
            }
            case PIPE: {
                // PIPE(stream, function(stream) -> stream)
                // PIPE(StreamOperator.class, FunctionOperator.class),
                OperatorNode<FunctionOperator> function = streamOperator.getArgument(1);
                GambitCreator.Invocable functionType = compileFunction(program.getType(), ctxExpr.getType(), ImmutableList.of(streamInput.getType()), function);
                BytecodeExpression resultStream = functionType.invoke(streamOperator.getLocation(), program, ctxExpr, streamInput);
                return compileStreamExpression(scope, program, ctxExpr, nextStream, resultStream);
            }
            case FLATTEN_TRANSFORM: {
                // FLATTEN_TRANSFORM(stream, (row) -> *rows)
                // FLATTEN_TRANSFORM(StreamOperator.class, FunctionOperator.class),
                OperatorNode<FunctionOperator> function = streamOperator.getArgument(1);
                LambdaCallable functionType = compileFunctionLambda(program.getType(), ctxExpr.getType(), adapter.getValue(), function);
                BytecodeExpression filtered = adapter.flatTransform(streamInput, functionType.create(program, ctxExpr), functionType.resultType);
                return compileStreamExpression(scope, program, ctxExpr, nextStream, filtered);
            }
            case FLATTEN_SCATTER: {
                // FLATTEN_SCATTER(stream, (row) -> *rows)
                // FLATTEN_SCATTER(StreamOperator.class, FunctionOperator.class),
                OperatorNode<FunctionOperator> function = streamOperator.getArgument(1);
                LambdaCallable functionType = compileFunctionLambda(program.getType(), ctxExpr.getType(), adapter.getValue(), function);
                BytecodeExpression filtered = adapter.flatScatter(streamInput, functionType.create(program, ctxExpr), functionType.resultType);
                return compileStreamExpression(scope, program, ctxExpr, nextStream, filtered);
            }
            case OUTER_HASH_JOIN:
            case HASH_JOIN: {
                // TODO: we should support a stream operator type that is (FLATMAP, FUNCTION<LROW> -> OROWS)
                //       or perhaps (XJOIN, LKEY, FUNCTION<LKEY> -> Stream<RROW>, FUNCTION<LROW, RROW>)
                OperatorNode<PhysicalExprOperator> right = streamOperator.getArgument(1);
                BytecodeExpression rightExpr = evaluateExpression(program, ctxExpr, right);
                OperatorNode<FunctionOperator> leftKey = streamOperator.getArgument(2);
                OperatorNode<FunctionOperator> rightKey = streamOperator.getArgument(3);
                LambdaCallable leftKeyFunction = compileFunctionLambda(program.getType(), ctxExpr.getType(), adapter.getValue(), leftKey);
                LambdaCallable rightKeyFunction = compileFunctionLambda(program.getType(), ctxExpr.getType(), rightExpr.getType().getIterableAdapter().getValue(), rightKey);
                OperatorNode<FunctionOperator> join = streamOperator.getArgument(4);
                LambdaCallable joinFunction = compileBiFunctionLambda(program.getType(), ctxExpr.getType(), adapter.getValue(), rightExpr.getType().getIterableAdapter().getValue(), join);
                BytecodeExpression joined = adapter.hashJoin(streamInput,
                        streamOperator.getOperator() == StreamOperator.OUTER_HASH_JOIN,
                        rightExpr.getType().getIterableAdapter().toStream(rightExpr),
                        leftKeyFunction.create(program, ctxExpr),
                        rightKeyFunction.create(program, ctxExpr),
                        joinFunction.create(program, ctxExpr),
                        joinFunction.resultType);
                return compileStreamExpression(scope, program, ctxExpr, nextStream, joined);
            }
            default:
                throw new UnsupportedOperationException("Unexpected transform StreamOperator: " + streamOperator.toString());
        }
    }

    public TypeWidget keyCursorFor(GambitTypes types, List<String> names) {
        ObjectBuilder builder = types.createObject(KeyAccumulator.class);
        ObjectBuilder.ConstructorBuilder bld = builder.getConstructor();
        bld.invokeSpecial(KeyAccumulator.class, bld.addArgument("$lst", new ArrayTypeWidget(Type.getType("[Ljava/lang/Object;"), AnyTypeWidget.getInstance())));
        ObjectBuilder.MethodBuilder adapter = builder.method("createKey");
        //     protected abstract RECORD createKey(List<KEY> columns);
        BytecodeExpression lst = adapter.addArgument("$list", NotNullableTypeWidget.create(new ListTypeWidget(AnyTypeWidget.getInstance())));
        TypeWidget keyType = BaseTypeAdapter.STRUCT;
        PropertyAdapter propertyAdapter = keyType.getPropertyAdapter();
        List<PropertyOperation> makeFields = Lists.newArrayList();
        int i = 0;
        List<TypeWidget> keyTypes = Lists.newArrayList();
        for (String name : names) {
            TypeWidget propertyType = propertyAdapter.getPropertyType(name);
            keyTypes.add(propertyType);
            makeFields.add(new PropertyOperation(name, adapter.cast(Location.NONE, propertyType, adapter.indexValue(Location.NONE, lst, adapter.constant(i++)))));
        }
        adapter.exit(adapter.cast(Location.NONE, types.adapt(Record.class, false), propertyAdapter.construct(makeFields)));
        builder.setTypeWidget(new KeyCursorTypeWidget(builder.getJVMType(), names, keyType));
        return builder.type();
    }
}

