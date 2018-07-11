package com.yahoo.yqlplus.engine.source;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.yahoo.cloud.metrics.api.MetricEmitter;
import com.yahoo.cloud.metrics.api.TaskMetricEmitter;
import com.yahoo.yqlplus.api.annotations.DefaultValue;
import com.yahoo.yqlplus.api.annotations.Delete;
import com.yahoo.yqlplus.api.annotations.Emitter;
import com.yahoo.yqlplus.api.annotations.Insert;
import com.yahoo.yqlplus.api.annotations.Key;
import com.yahoo.yqlplus.api.annotations.Query;
import com.yahoo.yqlplus.api.annotations.Set;
import com.yahoo.yqlplus.api.annotations.TimeoutMilliseconds;
import com.yahoo.yqlplus.api.annotations.Update;
import com.yahoo.yqlplus.api.index.IndexDescriptor;
import com.yahoo.yqlplus.api.trace.Tracer;
import com.yahoo.yqlplus.api.types.YQLType;
import com.yahoo.yqlplus.api.types.YQLTypeException;
import com.yahoo.yqlplus.engine.api.PropertyNotFoundException;
import com.yahoo.yqlplus.engine.compiler.code.AssignableValue;
import com.yahoo.yqlplus.engine.compiler.code.BytecodeExpression;
import com.yahoo.yqlplus.engine.compiler.code.PropertyAdapter;
import com.yahoo.yqlplus.engine.compiler.code.ScopedBuilder;
import com.yahoo.yqlplus.engine.compiler.code.TypeWidget;
import com.yahoo.yqlplus.engine.internal.plan.ContextPlanner;
import com.yahoo.yqlplus.engine.internal.plan.DynamicExpressionEnvironment;
import com.yahoo.yqlplus.engine.internal.plan.SourceType;
import com.yahoo.yqlplus.language.logical.ExpressionOperator;
import com.yahoo.yqlplus.language.logical.SequenceOperator;
import com.yahoo.yqlplus.language.operator.OperatorNode;
import com.yahoo.yqlplus.language.parser.Location;
import com.yahoo.yqlplus.language.parser.ProgramCompileException;
import com.yahoo.yqlplus.operator.ExprScope;
import com.yahoo.yqlplus.operator.FunctionOperator;
import com.yahoo.yqlplus.operator.OperatorStep;
import com.yahoo.yqlplus.operator.OperatorValue;
import com.yahoo.yqlplus.operator.PhysicalExprOperator;
import com.yahoo.yqlplus.operator.PhysicalOperator;
import com.yahoo.yqlplus.operator.SinkOperator;
import com.yahoo.yqlplus.operator.StreamOperator;
import com.yahoo.yqlplus.operator.StreamValue;
import org.objectweb.asm.Type;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static com.yahoo.yqlplus.engine.source.SourceApiGenerator.isFreeArgument;

public class SourceAdapter implements SourceType {
    private final String moduleName;
    private final Class<?> clazz;
    private final Supplier<?> supplier;
    private OperatorNode<PhysicalExprOperator> source;

    public SourceAdapter(String moduleName, Class<?> clazz, Supplier<?> module) {
        this.moduleName = moduleName;
        this.clazz = clazz;
        this.supplier = module;
    }

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

    private Map<String, OperatorNode<PhysicalExprOperator>> decodeExpressionMap(ContextPlanner context, OperatorNode<ExpressionOperator> map) {
        Preconditions.checkArgument(map.getOperator() == ExpressionOperator.MAP);
        List<String> names = map.getArgument(0);
        List<OperatorNode<ExpressionOperator>> values = map.getArgument(1);
        List<OperatorNode<PhysicalExprOperator>> valueExprs = context.evaluateList(values);
        ImmutableMap.Builder<String, OperatorNode<PhysicalExprOperator>> b = ImmutableMap.builder();
        for(int i = 0; i < names.size(); i++) {
            b.put(names.get(i), valueExprs.get(i));
        }
        return b.build();
    }

    private List<Map<String, OperatorNode<PhysicalExprOperator>>> decodeInsertSequenceMaps(ContextPlanner context, OperatorNode<SequenceOperator> records) {
        if(records.getOperator() == SequenceOperator.EVALUATE) {
            OperatorNode<ExpressionOperator> expr = records.getArgument(0);
            if(expr.getOperator() == ExpressionOperator.ARRAY) {
                List<OperatorNode<ExpressionOperator>> maps = expr.getArgument(0);
                List<Map<String, OperatorNode<PhysicalExprOperator>>> output = Lists.newArrayListWithExpectedSize(maps.size());
                for(OperatorNode<ExpressionOperator> op : maps) {
                    if(op.getOperator() == ExpressionOperator.MAP) {
                        output.add(decodeExpressionMap(context, op));
                    } else {
                        return null;
                    }
                }
            }
        }
        return null;
    }

    private void findApplicableMethods(ContextPlanner context, OperatorNode<SequenceOperator> source) {
        Class<? extends Annotation> annotationClass;
        List<OperatorNode<ExpressionOperator>> arguments;
        OperatorNode<SequenceOperator> scan;
        // an INSERT source is a sequence
        //    it may be a sequence of (EVALUATE (ARRAY (MAP ...)) in the case of INSERT INTO (c1, ...)  .. VALUES (v1, ...)
        //    it may be an arbitrary sequence as in INSERT INTO foo SELECT ....
        // We know ahead of time sinc we have the operator right here, so let's figure it out
        // an UPDATE source is always a MAP.
        // when the input is a MAP, populate writeRecord
        //  otherwise populate inputRecords
        List<Map<String, OperatorNode<PhysicalExprOperator>>> writeRecords;
        StreamValue inputRecords;
        switch(source.getOperator()) {
            case SCAN:
                annotationClass = Query.class;
                scan = source;
                arguments = source.getArgument(1);
                break;
            case INSERT: {
                annotationClass = Insert.class;
                scan = source.getArgument(0);
                arguments = scan.getArgument(1);
                OperatorNode<SequenceOperator> records = source.getArgument(1);
                writeRecords = decodeInsertSequenceMaps(context, records);
                if (writeRecords == null) {
                    inputRecords = context.execute(records);
                }
                break;
            }
            case UPDATE:
            case UPDATE_ALL:
                annotationClass = Update.class;
                scan = source.getArgument(0);
                arguments = scan.getArgument(1);
                OperatorNode<ExpressionOperator> map = source.getArgument(1);
                writeRecords = ImmutableList.of(decodeExpressionMap(context, map));
                break;
            case DELETE:
            case DELETE_ALL:
                annotationClass = Delete.class;
                scan = source.getArgument(0);
                arguments = scan.getArgument(1);
                break;
            default:
                throw new ProgramCompileException(source.getLocation(), "Unknown source operator: " + source);
        }
        // may want a different path for writes, since we can (and will) likely resolve/plan the whole operation here.
        // for reads we can dispatch to multiple query methods, so planning is more complex and requires knowing all of the applicable @Query methods before starting
        // but for writes we must dispatch to exactly one (or zero, a failure) write method
        //     for writes the input data is available we can use that as part of the method matching, since we may only match one method
        //     our output of the matching process will be the final invocation.
        // we do not permit writes on the left side of a join, so we know a join will always be a read.
        // we should cleanly seperate read and write method matching, so we can use the two-pass approach for reads and the one-pass approach for writes.
        List<OperatorNode<PhysicalExprOperator>> inputArgs = context.evaluateList(arguments);
        methods:
        for(Method method : clazz.getMethods()) {
            if(!Modifier.isPublic(method.getModifiers()) || method.getAnnotation(annotationClass) == null) {
                continue;
            }
            Location location = scan.getLocation();
            Class<?>[] argumentTypes = method.getParameterTypes();
            java.lang.reflect.Type[] genericArgumentTypes = method.getGenericParameterTypes();
            Annotation[][] annotations = method.getParameterAnnotations();
            Iterator<OperatorNode<PhysicalExprOperator>> inputNext = inputArgs.iterator();
            TypeWidget outputType = context.getGambitScope().adapt(method.getGenericReturnType(), true);
            TypeWidget rowType = outputType;
            boolean singleton = true;
            if (outputType.isPromise()) {
                rowType = outputType.getPromiseAdapter().getResultType();
            }
            if (rowType.isIterable()) {
                singleton = false;
                rowType = rowType.getIterableAdapter().getValue();
            }
            QM m = new QM(annotationClass, method, getSource(location, context), singleton, rowType);

            if (!rowType.hasProperties()) {
                throw new YQLTypeException("Source method " + method + " does not return a STRUCT type: " + rowType);
            }
            for (int i = 0; i < argumentTypes.length; ++i) {
                Class<?> parameterType = argumentTypes[i];
                java.lang.reflect.Type genericType = genericArgumentTypes[i];
                if (isFreeArgument(argumentTypes[i], annotations[i])) {
                    if (!inputNext.hasNext()) {
                        continue methods;
                    }
                    m.invokeArguments.add(inputNext.next());
                } else {
                    for (Annotation annotate : annotations[i]) {
                        if (annotate instanceof Key) {
                            if(Insert.class.isAssignableFrom(annotationClass)) {
                                reportMethodParameterException("Insert", method, "@Key parameters are not permitted on @Insert methods");
                                throw new IllegalArgumentException();
                            }
                            m.addKeyArgument((Key) annotate, parameterType, context.getGambitScope().adapt(genericArgumentTypes[i], true));
                        } else if (annotate instanceof Set) {
                            Object defaultValue = null;
                            Set set = (Set) annotate;
                            TypeWidget setType = context.getGambitScope().adapt(genericArgumentTypes[i], true);
                            for (Annotation ann : annotations[i]) {
                                if (ann instanceof DefaultValue) {
                                    defaultValue = parseDefaultValue(method, set.value(), setType, ((DefaultValue) ann).value());
                                }
                            }
                            if(Insert.class.isAssignableFrom(annotationClass) || Update.class.isAssignableFrom(annotationClass)) {
                                reportMethodParameterException(annotationClass.getSimpleName(), method, "@Key parameters are only permitted on @Insert and @Update methods");
                                throw new IllegalArgumentException();
                            }
                            m.addSetArgument(set.value(), defaultValue, parameterType, setType);
                        } else if (annotate instanceof TimeoutMilliseconds) {
                            if (!Long.TYPE.isAssignableFrom(parameterType)) {
                                reportMethodParameterException("TimeoutMilliseconds", method, "@TimeoutMilliseconds argument type must be a primitive long");
                            }
                            m.invokeArguments.add(OperatorNode.create(PhysicalExprOperator.TIMEOUT_REMAINING, TimeUnit.MILLISECONDS));
                        } else if (annotate instanceof Emitter) {
                            if (MetricEmitter.class.isAssignableFrom(parameterType) || TaskMetricEmitter.class.isAssignableFrom(parameterType)) {
                                m.invokeArguments.add(OperatorNode.create(PhysicalExprOperator.PROPREF, OperatorNode.create(PhysicalExprOperator.CURRENT_CONTEXT), "metricEmitter"));
                            } else if (Tracer.class.isAssignableFrom(parameterType)) {
                                m.invokeArguments.add(OperatorNode.create(PhysicalExprOperator.PROPREF, OperatorNode.create(PhysicalExprOperator.CURRENT_CONTEXT), "tracer"));
                            } else {
                                reportMethodParameterException("Trace", method, "@Emitter argument type must be a %s or %s", MetricEmitter.class.getName(), Tracer.class.getName());
                            }
                        }
                    }
                }
            }
        }
    }

    class QM {
        Class<? extends Annotation> annotationClass;
        Method method;
        List<OperatorNode<PhysicalExprOperator>> invokeArguments;
        PhysicalExprOperator callOperator;
        final IndexDescriptor.Builder indexBuilder;
        final Map<String, AssignableValue> keyArguments;
        boolean singleton;
        boolean batch;
        TypeWidget rowType;
        PropertyAdapter rowProperties;
        String methodType;
        public QM(Class<? extends Annotation> annotationClass, Method method, OperatorNode<PhysicalExprOperator> source, boolean singleton, TypeWidget rowType) {
            this.annotationClass = annotationClass;
            this.methodType = annotationClass.getSimpleName();
            this.method = method;
            this.invokeArguments = Lists.newArrayList();
            this.callOperator = PhysicalExprOperator.INVOKEVIRTUAL;
            if (Modifier.isStatic(method.getModifiers())) {
                callOperator = PhysicalExprOperator.INVOKESTATIC;
            } else if(clazz.isInterface()) {
                callOperator = PhysicalExprOperator.INVOKEINTERFACE;
                invokeArguments.add(source);
            } else {
                invokeArguments.add(source);
            }
            indexBuilder = IndexDescriptor.builder();
            keyArguments = Maps.newLinkedHashMap();
            this.singleton = singleton;
            this.rowType = rowType;
            this.rowProperties = rowType.getPropertyAdapter();
            this.batch = false;
        }

        boolean isScan() {
            return keyArguments.isEmpty();
        }


        void addKeyArgument(Key key, Class<?> parameterType, TypeWidget parameterWidget) {
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
                invokeArguments.add(extractKey(OperatorNode.create(PhysicalExprOperator.LOCAL, "$keys"), keyName));
            } else if (batch) {
                reportMethodParameterException(methodType, method, "@Key column '%s' is a single value but other parameters are batch; a method must either be entirely-batch or entirely-not", keyName);
            } else {
                verifyArgumentType(methodType, rowType, rowProperties, keyName, parameterWidget, "Key", method);
                addIndexKey(keyName, parameterWidget, skipEmpty, skipNull);
                invokeArguments.add(OperatorNode.create(PhysicalExprOperator.PROPREF, OperatorNode.create(PhysicalExprOperator.LOCAL, "$key"), keyName));
            }
        }

        void addSetArgument(String keyName, Object defaultValue, Class<?> parameterType, TypeWidget setType) {
//            if (dataValues.containsKey(keyName)) {
//                reportMethodParameterException("Insert", method, "@Set('%s') used multiple times", keyName);
//                throw new IllegalArgumentException(); // unreachable, but satisfies javac reachability analyzer
//            }
//            dataValues.put(keyName, body.allocate(setType));
//            verifyArgumentType("Insert", rowType, rowProperties, keyName, setType, "Set", method);
//            YQLType type = gambitScope.createYQLType(setType);
//            argumentMap.addField(keyName, type, defaultValue != null);
//            if (defaultValue != null) {
//                defaultValues.put(keyName, parseDefaultValue(body, method, keyName, setType, defaultValue.value()));
//            }
//            return dataValues.get(keyName);
        }

        protected void addIndexKey(String keyName, TypeWidget keyType, boolean skipEmpty, boolean skipNull) {
            try {
                indexBuilder.addColumn(keyName, keyType.getValueCoreType(), skipEmpty, skipNull);
            } catch (IllegalArgumentException e) {
                reportMethodParameterException(methodType, method, "Key '%s' cannot be added to index: %s", keyName, e.getMessage());
            }
        }
    }

    private OperatorNode<StreamOperator> accumulate() {
        return OperatorNode.create(StreamOperator.SINK, OperatorNode.create(SinkOperator.ACCUMULATE));
    }


    private OperatorNode<PhysicalExprOperator> extractKey(OperatorNode<PhysicalExprOperator> keys, String keyName) {
        ExprScope scope = new ExprScope();
        OperatorNode<PhysicalExprOperator> key = OperatorNode.create(PhysicalExprOperator.PROPREF, scope.addArgument("$key"), keyName);
        OperatorNode<FunctionOperator> function = scope.createFunction(key);
        return OperatorNode.create(PhysicalExprOperator.STREAM_EXECUTE, keys, OperatorNode.create(StreamOperator.TRANSFORM, accumulate(), function));
    }

    private OperatorNode<PhysicalExprOperator> getSource(Location location, ContextPlanner planner) {
        if (source == null) {
            if (supplier != null) {
                OperatorValue value = OperatorStep.create(planner.getValueTypeAdapter(), location, PhysicalOperator.EVALUATE,
                        OperatorNode.create(location, PhysicalExprOperator.CURRENT_CONTEXT),
                        OperatorNode.create(location, PhysicalExprOperator.INVOKEINTERFACE,
                                clazz, Type.getType(Supplier.class), "get", Type.getMethodDescriptor(Type.getType(Object.class)),
                                ImmutableList.of(OperatorNode.create(PhysicalExprOperator.CONSTANT_VALUE, Supplier.class, supplier))));
                source = OperatorNode.create(location, PhysicalExprOperator.VALUE, value);
            }  else {
                OperatorValue value = OperatorStep.create(planner.getValueTypeAdapter(), location, PhysicalOperator.EVALUATE,
                        OperatorNode.create(location, PhysicalExprOperator.CURRENT_CONTEXT),
                        OperatorNode.create(location, PhysicalExprOperator.INVOKENEW,
                                clazz,
                                ImmutableList.of()));
                source = OperatorNode.create(location, PhysicalExprOperator.VALUE, value);
            }
        }
        return source;
    }

    protected void reportMethodParameterException(String type, Method method, String message, Object... args) {
        message = String.format(message, args);
        throw new YQLTypeException(String.format("@%s method error: %s.%s: %s", type, method.getDeclaringClass().getName(), method.getName(), message));
    }

    private void reportMethodException(Method method, String message, Object... args) {
        message = String.format(message, args);
        throw new YQLTypeException(String.format("method error: %s.%s: %s", clazz.getName(), method.getName(), message));
    }

    /**
     * Verifies that the given resultType has a property matching the given fieldName and fieldType.
     */
    private void verifyArgumentType(String methodTypeName, TypeWidget rowType, PropertyAdapter rowProperties, String propertyName, TypeWidget argumentType, String annotationName,
                                    Method method)
            throws ProgramCompileException {
        try {
            TypeWidget targetType = rowProperties.getPropertyType(propertyName);
            if (!targetType.isAssignableFrom(argumentType)) {
                reportMethodParameterException(methodTypeName, method, "class %s property %s is %s while @%s('%s') type is %s: @%s('%s') " +
                                "argument type %s cannot be coerced to property '%s' type %s in method %s.%s",
                        rowType.getTypeName(), propertyName, targetType.getTypeName(), annotationName,
                        propertyName, argumentType.getTypeName(),
                        annotationName, propertyName, argumentType.getTypeName(), propertyName,
                        targetType.getTypeName(), method.getDeclaringClass().getName(),
                        method.getName());
            }
        } catch (PropertyNotFoundException e) {
            reportMethodParameterException(methodTypeName, method, "Property @%s('%s') for method %s.%s does not exist on return type %s",
                    annotationName, propertyName, method.getDeclaringClass().getName(), method.getName(),
                    rowType.getTypeName());
        }
    }

    private Object parseDefaultValue(Method method, String keyName, TypeWidget setType, String defaultValue) {
        if (setType.isIterable()) {
            TypeWidget target = setType.getIterableAdapter().getValue();
            List<Object> expr = Lists.newArrayList();
            StringTokenizer tokenizer = new StringTokenizer(defaultValue, ",", false);
            while (tokenizer.hasMoreElements()) {
                expr.add(parseDefaultValue(method, keyName, target, tokenizer.nextToken().trim()));
            }
            return expr;
        } else {
            try {
                switch (setType.getValueCoreType()) {
                    case BOOLEAN:
                        return Boolean.valueOf(defaultValue);
                    case INT8:
                        return Byte.decode(defaultValue);
                    case INT16:
                        return Short.valueOf(defaultValue);
                    case INT32:
                        return Integer.valueOf(defaultValue);
                    case INT64:
                    case TIMESTAMP:
                        return Long.valueOf(defaultValue);
                    case FLOAT32:
                        return Float.valueOf(defaultValue);
                    case FLOAT64:
                        return Double.valueOf(defaultValue);
                    case STRING:
                        return defaultValue;
                    default:
                        reportMethodException(method, "Unable to match default value for @Set('%s') @DefaultValue('%s') to type %s", keyName, defaultValue, setType.getTypeName());
                        throw new IllegalArgumentException(); // reachability
                }
            } catch (NumberFormatException e) {
                reportMethodException(method, "Unable to parse default argument %s for @Set('%s'): %s", defaultValue, keyName, e.getMessage());
                throw new IllegalArgumentException(); // reachability
            }
        }
    }


}
