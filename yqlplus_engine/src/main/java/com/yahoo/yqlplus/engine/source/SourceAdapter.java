package com.yahoo.yqlplus.engine.source;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.yahoo.cloud.metrics.api.MetricEmitter;
import com.yahoo.cloud.metrics.api.TaskMetricEmitter;
import com.yahoo.yqlplus.api.annotations.*;
import com.yahoo.yqlplus.api.trace.Tracer;
import com.yahoo.yqlplus.api.types.YQLTypeException;
import com.yahoo.yqlplus.engine.compiler.code.TypeWidget;
import com.yahoo.yqlplus.engine.internal.plan.ContextPlanner;
import com.yahoo.yqlplus.engine.internal.plan.SourceType;
import com.yahoo.yqlplus.language.logical.ExpressionOperator;
import com.yahoo.yqlplus.language.logical.SequenceOperator;
import com.yahoo.yqlplus.language.operator.OperatorNode;
import com.yahoo.yqlplus.language.parser.Location;
import com.yahoo.yqlplus.operator.*;
import org.objectweb.asm.Type;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static com.yahoo.yqlplus.engine.source.SourceApiGenerator.isFreeArgument;

public class SourceAdapter implements SourceType {
    private final String moduleName;
    private final Class<?> clazz;
    private final Supplier<?> supplier;
    private OperatorNode<PhysicalExprOperator> module;

    enum MethodType {
        SELECT,
        INSERT,
        UPDATE,
        DELETE,
        NONE
    }

    private MethodType classify(Method method) {
        Query select = method.getAnnotation(Query.class);
        Insert insert = method.getAnnotation(Insert.class);
        Update update = method.getAnnotation(Update.class);
        Delete delete = method.getAnnotation(Delete.class);
        if(!Modifier.isPublic(method.getModifiers())) {
            return MethodType.NONE;
        } else if (select != null) {
            return MethodType.SELECT;
        } else if (insert != null) {
            return MethodType.INSERT;
        } else if (update != null) {
            return MethodType.UPDATE;
        } else if (delete != null) {
            return MethodType.DELETE;
        } else {
            return MethodType.NONE;
        }
    }

    interface Parameter {
        OperatorNode<PhysicalExprOperator> create(Location loc, ContextPlanner planner);
    }

    private Parameter parameter(OperatorNode<PhysicalExprOperator> p) {
        return (loc, planner) -> p;
    }

    public SourceAdapter(String moduleName, Class<?> clazz, Supplier<?> module) {
        this.moduleName = moduleName;
        this.clazz = clazz;
        this.supplier = module;
        // loop over all methods and build adapters for them all
        methods:
        for (Method method : clazz.getMethods()) {
            MethodType type = classify(method);
            if (type == MethodType.NONE) {
                continue;
            }

            ExprScope scoper = new ExprScope();
            int args = 0;
            List<Parameter> invokeArguments = Lists.newArrayList();
            PhysicalExprOperator callOperator = PhysicalExprOperator.INVOKEVIRTUAL;
            if (Modifier.isStatic(method.getModifiers())) {
                callOperator = PhysicalExprOperator.INVOKESTATIC;
            } else if(clazz.isInterface()) {
                callOperator = PhysicalExprOperator.INVOKEINTERFACE;
                invokeArguments.add(this::getModule);
            } else {
                invokeArguments.add(this::getModule);
            }
            Class<?>[] argumentTypes = method.getParameterTypes();
            Annotation[][] annotations = method.getParameterAnnotations();
            for (int i = 0; i < argumentTypes.length; ++i) {
                Class<?> parameterType = argumentTypes[i];
                if (isFreeArgument(argumentTypes[i], annotations[i])) {
                    invokeArguments.add(parameter(scoper.addArgument("arg" + args)));
                    args++;
                } else {
                    for (Annotation annotate : annotations[i]) {
                        if (annotate instanceof Key) {
                            String keyName = key.value().toLowerCase();
                            boolean skipEmpty = key.skipEmptyOrZero();
                            boolean skipNull = key.skipNull();
                            if (keyArguments.containsKey(keyName)) {
                                reportMethodParameterException(methodType, method, "@Key column '%s' used multiple times", keyName);
                            } else if (List.class.isAssignableFrom(parameterType)) {
                                if (!batch && !isScan()) {
                                    reportMethodParameterException(methodType, method, "@Key column '%s' is a List (batch); a method must either be entirely-batch or entirely-not", keyName);
                                }
                                batch = true;
                                TypeWidget keyType = parameterWidget.getIterableAdapter().getValue();
                                verifyArgumentType(methodType, rowType, rowProperties, keyName, keyType, "Key", method);
                                addIndexKey(keyName, keyType, skipEmpty, skipNull);
                                addKeyParameter(body, parameterWidget, keyName);
                            } else if (batch) {
                                reportMethodParameterException(methodType, method, "@Key column '%s' is a single value but other parameters are batch; a method must either be entirely-batch or entirely-not", keyName);
                            } else {
                                verifyArgumentType(methodType, rowType, rowProperties, keyName, parameterWidget, "Key", method);
                                addIndexKey(keyName, parameterWidget, skipEmpty, skipNull);
                                addKeyParameter(body, parameterWidget, keyName);
                            }
                            return keyArguments.get(keyName);
                        } else if (annotate instanceof Set || annotate instanceof DefaultValue) {
                            continue methods;
                        } else if (annotate instanceof TimeoutMilliseconds) {
                            if (!Long.TYPE.isAssignableFrom(parameterType)) {
                                reportMethodParameterException("TimeoutMilliseconds", method, "@TimeoutMilliseconds argument type must be a primitive long");
                            }
                            invokeArguments.add(parameter(OperatorNode.create(PhysicalExprOperator.TIMEOUT_REMAINING, TimeUnit.MILLISECONDS)));
                        } else if (annotate instanceof Emitter) {
                            if (MetricEmitter.class.isAssignableFrom(parameterType) || TaskMetricEmitter.class.isAssignableFrom(parameterType)) {
                                invokeArguments.add(parameter(OperatorNode.create(PhysicalExprOperator.PROPREF, OperatorNode.create(PhysicalExprOperator.CURRENT_CONTEXT), "metricEmitter")));
                            } else if (Tracer.class.isAssignableFrom(parameterType)) {
                                invokeArguments.add(parameter(OperatorNode.create(PhysicalExprOperator.PROPREF, OperatorNode.create(PhysicalExprOperator.CURRENT_CONTEXT), "tracer")));
                            } else {
                                reportMethodParameterException("Trace", method, "@Emitter argument type must be a %s or %s", MetricEmitter.class.getName(), Tracer.class.getName());
                            }
                        }
                    }
                }
            }
            return OperatorNode.create(callOperator, method.getGenericReturnType(), Type.getType(method.getDeclaringClass()), method.getName(), Type.getMethodDescriptor(method), invokeArguments);
        }    }

    @Override
    public StreamValue plan(ContextPlanner planner, OperatorNode<SequenceOperator> query, OperatorNode<SequenceOperator> source) {
        List<OperatorNode<ExpressionOperator>> args = source.getArgument(1);
        int count = args.size();

        return null;
    }

    @Override
    public StreamValue join(ContextPlanner planner, OperatorNode<PhysicalExprOperator> leftSide, OperatorNode<ExpressionOperator> joinExpression, OperatorNode<SequenceOperator> right, OperatorNode<SequenceOperator> source) {
        List<OperatorNode<ExpressionOperator>> args = source.getArgument(1);
        int count = args.size();
        return null;
    }

    private OperatorNode<PhysicalExprOperator> getModule(Location location, ContextPlanner planner) {
        if (module == null) {
            if (supplier != null) {
                OperatorValue value = OperatorStep.create(planner.getValueTypeAdapter(), location, PhysicalOperator.EVALUATE,
                        OperatorNode.create(location, PhysicalExprOperator.CURRENT_CONTEXT),
                        OperatorNode.create(location, PhysicalExprOperator.INVOKEINTERFACE,
                                clazz, Type.getType(Supplier.class), "get", Type.getMethodDescriptor(Type.getType(Object.class)),
                                ImmutableList.of(OperatorNode.create(PhysicalExprOperator.CONSTANT_VALUE, Supplier.class, supplier))));
                module = OperatorNode.create(location, PhysicalExprOperator.VALUE, value);
            }  else {
                OperatorValue value = OperatorStep.create(planner.getValueTypeAdapter(), location, PhysicalOperator.EVALUATE,
                        OperatorNode.create(location, PhysicalExprOperator.CURRENT_CONTEXT),
                        OperatorNode.create(location, PhysicalExprOperator.INVOKENEW,
                                clazz,
                                ImmutableList.of()));
                module = OperatorNode.create(location, PhysicalExprOperator.VALUE, value);
            }
        }
        return module;
    }

    private void reportMethodParameterException(String type, Method method, String message, Object... args) {
        message = String.format(message, args);
        throw new YQLTypeException(String.format("@%s method error: %s.%s: %s", type, method.getDeclaringClass().getName(), method.getName(), message));
    }

}
