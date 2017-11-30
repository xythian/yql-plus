/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.plan.types.base;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.yahoo.yqlplus.api.types.YQLCoreType;
import com.yahoo.yqlplus.engine.api.NativeEncoding;
import com.yahoo.yqlplus.engine.api.PropertyNotFoundException;
import com.yahoo.yqlplus.engine.internal.bytecode.IterableTypeWidget;
import com.yahoo.yqlplus.engine.internal.bytecode.types.gambit.PropertyOperation;
import com.yahoo.yqlplus.engine.internal.bytecode.types.gambit.ResultAdapter;
import com.yahoo.yqlplus.engine.internal.compiler.CodeEmitter;
import com.yahoo.yqlplus.engine.internal.plan.types.AssignableValue;
import com.yahoo.yqlplus.engine.internal.plan.types.BytecodeExpression;
import com.yahoo.yqlplus.engine.internal.plan.types.BytecodeSequence;
import com.yahoo.yqlplus.engine.internal.plan.types.IndexAdapter;
import com.yahoo.yqlplus.engine.internal.plan.types.IterateAdapter;
import com.yahoo.yqlplus.engine.internal.plan.types.OptionalAdapter;
import com.yahoo.yqlplus.engine.internal.plan.types.ProgramValueTypeAdapter;
import com.yahoo.yqlplus.engine.internal.plan.types.PromiseAdapter;
import com.yahoo.yqlplus.engine.internal.plan.types.SerializationAdapter;
import com.yahoo.yqlplus.engine.internal.plan.types.TypeWidget;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class OptionalTypeWidget extends BaseTypeWidget {
    public static TypeWidget create(TypeWidget input) {
        if (input instanceof OptionalTypeWidget) {
            return input;
        } else if (input.isPrimitive()) {
            return create(input.boxed());
        } else if (!input.isNullable()) {
            return new OptionalTypeWidget(input);
        } else {
            return new OptionalTypeWidget(NotNullableTypeWidget.create(input));
        }
    }

    private final TypeWidget target;

    public TypeWidget getTarget() {
        return target;
    }

    private OptionalTypeWidget(TypeWidget target) {
        super(Type.getType(Optional.class));
        this.target = target;
    }



    @Override
    public boolean isPrimitive() {
        return false;
    }

    @Override
    public BytecodeExpression construct(BytecodeExpression... arguments) {
        return target.construct(arguments);
    }

    @Override
    public Coercion coerceTo(BytecodeExpression source, TypeWidget target) {
        return this.target.coerceTo(source, target);
    }

    @Override
    public TypeWidget boxed() {
        return this;
    }

    @Override
    public TypeWidget unboxed() {
        return this;
    }

    static class EmptyOptionalExpression extends BaseTypeExpression {
        public EmptyOptionalExpression(TypeWidget type) {
            super(create(type));
        }

        @Override
        public void generate(CodeEmitter code) {
            code.getMethodVisitor().visitMethodInsn(Opcodes.INVOKESTATIC,
                    Type.getInternalName(Optional.class),
                    "empty",
                    Type.getMethodDescriptor(Type.getType(Optional.class)),
                    false);
        }
    }

    static class WrapOptionalExpression extends BaseTypeExpression {
        private final BytecodeExpression input;

        public WrapOptionalExpression(TypeWidget type, BytecodeExpression input) {
            super(type);
            this.input = input;
        }

        public WrapOptionalExpression(BytecodeExpression input) {
            super(create(input.getType()));
            this.input = input;
        }

        @Override
        public void generate(CodeEmitter code) {
            code.exec(input);
            code.box(input.getType());
            if(input.getType().isNullable()) {
                code.getMethodVisitor().visitMethodInsn(Opcodes.INVOKESTATIC,
                        Type.getInternalName(Optional.class),
                        "ofNullable",
                        Type.getMethodDescriptor(Type.getType(Optional.class), Type.getType(Object.class)),
                        false);
            } else {
                code.getMethodVisitor().visitMethodInsn(Opcodes.INVOKESTATIC,
                        Type.getInternalName(Optional.class),
                        "of",
                        Type.getMethodDescriptor(Type.getType(Optional.class), Type.getType(Object.class)),
                        false);
            }
        }
    }


    private BytecodeExpression wrapInOptional(BytecodeExpression input) {
        return new WrapOptionalExpression(this, input);
    }

    @Override
    public PropertyAdapter getPropertyAdapter() {
        final PropertyAdapter targetAdapter = target.getPropertyAdapter();
        final OptionalAdapter optionalAdapter = getOptionalAdapter();
        return new OptionalPropertyAdapter(targetAdapter, optionalAdapter);
    }

    @Override
    public boolean hasProperties() {
        return target.hasProperties();
    }

    @Override
    public IndexAdapter getIndexAdapter() {
        return target.getIndexAdapter();
    }

    @Override
    public boolean isIndexable() {
        return target.isIndexable();
    }

    @Override
    public IterateAdapter getIterableAdapter() {
        return target.getIterableAdapter();
    }

    @Override
    public boolean isIterable() {
        return target.isIterable();
    }

    @Override
    public YQLCoreType getValueCoreType() {
        return target.getValueCoreType();
    }

    @Override
    public ComparisonAdapter getComparisionAdapter() {
        return new NullTestingComparisonAdapter(this, target);
    }

    @Override
    public PromiseAdapter getPromiseAdapter() {
        return target.getPromiseAdapter();
    }

    @Override
    public boolean isPromise() {
        return target.isPromise();
    }

    @Override
    public ResultAdapter getResultAdapter() {
        return target.getResultAdapter();
    }

    @Override
    public boolean isResult() {
        return target.isResult();
    }

    @Override
    public String getTypeName() {
        return target.getTypeName();
    }

    @Override
    public boolean isAssignableFrom(TypeWidget type) {
        return target.isAssignableFrom(type);
    }

    @Override
    public boolean hasUnificationAdapter() {
        return target.hasUnificationAdapter();
    }

    @Override
    public UnificationAdapter getUnificationAdapter(ProgramValueTypeAdapter typeAdapter) {
        UnificationAdapter adapter = target.getUnificationAdapter(typeAdapter);
        return new UnificationAdapter() {
            @Override
            public TypeWidget unify(TypeWidget other) {
                return create(adapter.unify(other));
            }
        };
    }

    @Override
    public SerializationAdapter getSerializationAdapter(NativeEncoding encoding) {
        return new OptionalTestingSerializationAdapter(this, target, encoding);
    }



    @Override
    public String toString() {
        return "OptionalType<" +
                target +
                '>';
    }

    private class OptionalPropertyAdapter implements PropertyAdapter {
        private final PropertyAdapter targetAdapter;
        private final OptionalAdapter optionalAdapter;

        public OptionalPropertyAdapter(PropertyAdapter targetAdapter, OptionalAdapter optionalAdapter) {
            this.targetAdapter = targetAdapter;
            this.optionalAdapter = optionalAdapter;
        }

        @Override
        public BytecodeExpression construct(List<PropertyOperation> fields) {
            return wrapInOptional(targetAdapter.construct(fields));
        }

        @Override
        public boolean isClosed() {
            return targetAdapter.isClosed();
        }

        @Override
        public TypeWidget getPropertyType(String propertyName) throws PropertyNotFoundException {
            return create(targetAdapter.getPropertyType(propertyName));
        }

        @Override
        public Iterable<Property> getProperties() {
            return Lists.newArrayList(targetAdapter.getProperties())
                    .stream()
                    .map((p) -> new Property(p.name, create(p.type)))
                    .collect(Collectors.toList());
        }

        @Override
        public AssignableValue property(BytecodeExpression target, String propertyName) {
            TypeWidget outputPropertyType = getPropertyType(propertyName);
            return new AssignableValue() {
                @Override
                public TypeWidget getType() {
                    return outputPropertyType;
                }

                @Override
                public BytecodeExpression read() {
                    return new BaseTypeExpression(outputPropertyType) {
                        @Override
                        public void generate(CodeEmitter code) {
                            optionalAdapter.generate(code,
                                    target,
                                    (availCode, availValue) -> {
                                        code.exec(new WrapOptionalExpression(outputPropertyType, targetAdapter.property(availValue, propertyName)));
                                    },
                                    (unavailCode, unavailValue) -> {
                                        unavailCode.exec(new EmptyOptionalExpression(outputPropertyType));
                                    }
                            );
                        }
                    };
                }

                @Override
                public BytecodeSequence write(BytecodeExpression value) {
                    return code -> getOptionalAdapter().generate(code,
                            target,
                            (availCode, availValue) -> {
                                code.exec(targetAdapter.property(availValue, propertyName).write(value));
                            },
                            (unavailCode, unavailValue) -> {
                                // do nothing
                            }
                    );
                }

                @Override
                public BytecodeSequence write(TypeWidget top) {
                    return code -> getOptionalAdapter().generate(code,
                            target,
                            (availCode, availValue) -> {
                                code.exec(targetAdapter.property(availValue, propertyName).write(top));
                            },
                            (unavailCode, unavailValue) -> {
                                code.pop(top);
                            }
                    );
                }

                @Override
                public void generate(CodeEmitter code) {
                    read().generate(code);
                }
            };
        }

        @Override
        public AssignableValue index(BytecodeExpression target, BytecodeExpression propertyName) {
            TypeWidget outputPropertyType = create(AnyTypeWidget.getInstance());
            return new AssignableValue() {
                @Override
                public TypeWidget getType() {
                    return outputPropertyType;
                }

                @Override
                public BytecodeExpression read() {
                    return new BaseTypeExpression(outputPropertyType) {
                        @Override
                        public void generate(CodeEmitter code) {
                            optionalAdapter.generate(code,
                                    target,
                                    (availCode, availValue) -> {
                                        code.exec(new WrapOptionalExpression(outputPropertyType, targetAdapter.index(availValue, propertyName)));
                                    },
                                    (unavailCode, unavailValue) -> {
                                        unavailCode.exec(new EmptyOptionalExpression(outputPropertyType));
                                    }
                            );
                        }
                    };
                }

                @Override
                public BytecodeSequence write(BytecodeExpression value) {
                    return code -> getOptionalAdapter().generate(code,
                            target,
                            (availCode, availValue) -> {
                                code.exec(targetAdapter.index(availValue, propertyName).write(value));
                            },
                            (unavailCode, unavailValue) -> {
                                // do nothing
                            }
                    );
                }

                @Override
                public BytecodeSequence write(TypeWidget top) {
                    return code -> getOptionalAdapter().generate(code,
                            target,
                            (availCode, availValue) -> {
                                code.exec(targetAdapter.index(availValue, propertyName).write(top));
                            },
                            (unavailCode, unavailValue) -> {
                                code.pop(top);
                            }
                    );
                }

                @Override
                public void generate(CodeEmitter code) {
                    read().generate(code);
                }
            };
        }

        @Override
        public BytecodeSequence mergeIntoFieldWriter(BytecodeExpression target, BytecodeExpression fieldWriter) {
            return code -> getOptionalAdapter().generate(code,
                    target,
                    (availCode, availValue) -> {
                        code.exec(targetAdapter.mergeIntoFieldWriter(availValue, fieldWriter));
                    },
                    (unavailCode, unavailValue) -> {

                    }
            );
        }

        @Override
        public BytecodeSequence visitProperties(BytecodeExpression target, PropertyVisit loop) {
            return code -> getOptionalAdapter().generate(code,
                    target,
                    (availCode, availValue) -> {
                        code.exec(targetAdapter.visitProperties(availValue, loop));
                    },
                    (unavailCode, unavailValue) -> {

                    }
            );
        }

        @Override
        public BytecodeExpression getPropertyNameIterable(BytecodeExpression target) {
            TypeWidget outputType = new IterableTypeWidget(BaseTypeAdapter.STRING);
            return new BaseTypeExpression(outputType) {
                @Override
                public void generate(CodeEmitter code) {
                    getOptionalAdapter().generate(code,
                            target,
                            (availCode, availValue) -> {
                                BytecodeExpression out = targetAdapter.getPropertyNameIterable(availValue);
                                code.exec(out);
                                code.cast(outputType, out.getType());
                            },
                            (unavailCode, unavailValue) -> {
                                code.getMethodVisitor().visitMethodInsn(Opcodes.INVOKESTATIC,
                                        Type.getInternalName(ImmutableList.class),
                                        "of",
                                        Type.getMethodDescriptor(Type.getType(ImmutableList.class)),
                                        false);
                            });
                }
            };
        }
    }
}
