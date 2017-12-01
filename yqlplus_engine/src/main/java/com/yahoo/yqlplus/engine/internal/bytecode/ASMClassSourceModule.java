/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.bytecode;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.AbstractModule;
import com.google.inject.Provider;
import com.google.inject.multibindings.Multibinder;
import com.yahoo.yqlplus.engine.internal.bytecode.types.JVMTypes;
import com.yahoo.yqlplus.engine.internal.bytecode.types.gambit.CompletableFutureResultType;
import com.yahoo.yqlplus.engine.internal.bytecode.types.gambit.FutureResultType;
import com.yahoo.yqlplus.engine.internal.bytecode.types.gambit.ListenableFutureResultType;
import com.yahoo.yqlplus.engine.internal.plan.types.ProgramValueTypeAdapter;
import com.yahoo.yqlplus.engine.internal.plan.types.TypeWidget;
import com.yahoo.yqlplus.engine.internal.plan.types.base.ListTypeWidget;
import com.yahoo.yqlplus.engine.internal.plan.types.base.MapTypeWidget;
import com.yahoo.yqlplus.engine.internal.plan.types.base.OptionalTypeWidget;
import com.yahoo.yqlplus.engine.internal.plan.types.base.ReflectiveJavaTypeWidget;
import com.yahoo.yqlplus.engine.internal.plan.types.base.ReflectiveTypeAdapter;
import com.yahoo.yqlplus.engine.internal.plan.types.base.StreamTypeWidget;
import com.yahoo.yqlplus.engine.internal.plan.types.base.TypeAdaptingWidget;

import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.stream.Stream;

public class ASMClassSourceModule extends AbstractModule {
    @Override
    protected void configure() {
        Multibinder<TypeAdaptingWidget> binder = Multibinder.newSetBinder(binder(), TypeAdaptingWidget.class);
        binder.addBinding().toInstance(new TypeFieldAdaptingWidget());
        binder.addBinding().toInstance(new TypeAdaptingWidget() {
            @Override
            public boolean supports(Class<?> clazzType) {
                return Enum.class.isAssignableFrom(clazzType) && !clazzType.isEnum();
            }

            @Override
            public TypeWidget adapt(ProgramValueTypeAdapter typeAdapter, Type type) {
                return typeAdapter.adaptInternal(JVMTypes.getRawType(type).getSuperclass());
            }
        });
        binder.addBinding().toInstance(new TypeAdaptingWidget() {
            @Override
            public boolean supports(Class<?> clazzType) {
                return clazzType.isEnum();
            }

            @Override
            public TypeWidget adapt(ProgramValueTypeAdapter typeAdapter, Type type) {
                return new EnumTypeAdapter(JVMTypes.getRawType(type));
            }
        });
        binder.addBinding().toInstance(new TypeAdaptingWidget() {
            @Override
            public boolean supports(Class<?> clazzType) {
                return Future.class.isAssignableFrom(clazzType);
            }

            @Override
            public TypeWidget adapt(ProgramValueTypeAdapter typeAdapter, Type type) {
                TypeWidget valueType = typeAdapter.adaptInternal(JVMTypes.getTypeArgument(type, 0));
                Class<?> rawType = JVMTypes.getRawType(type);
                if(ListenableFuture.class.isAssignableFrom(rawType)) {
                    return new ListenableFutureResultType(valueType);
                } else if(CompletableFuture.class.isAssignableFrom(rawType)) {
                    return new CompletableFutureResultType(valueType);
                }
                return new FutureResultType(valueType);
            }
        });
        binder.addBinding().toInstance(new TypeAdaptingWidget() {
            @Override
            public boolean supports(Class<?> clazzType) {
                return Stream.class.isAssignableFrom(clazzType);
            }

            @Override
            public TypeWidget adapt(ProgramValueTypeAdapter typeAdapter, Type type) {
                TypeWidget valueType = typeAdapter.adaptInternal(JVMTypes.getTypeArgument(type, 0));
                Class<?> rawType = JVMTypes.getRawType(type);
                return new StreamTypeWidget(rawType, valueType);
            }
        });
        binder.addBinding().toInstance(new RecordAdaptingWidget());
        binder.addBinding().toInstance(new TypeAdaptingWidget() {
            @Override
            public boolean supports(Class<?> clazzType) {
                return Map.class.isAssignableFrom(clazzType);
            }

            @Override
            public TypeWidget adapt(ProgramValueTypeAdapter typeAdapter, Type type) {
                return new MapTypeWidget(getPackageSafeRawType(type, Map.class), typeAdapter.adaptInternal(JVMTypes.getTypeArgument(type, 0)), typeAdapter.adaptInternal(JVMTypes.getTypeArgument(type, 1)));
            }
        });
        binder.addBinding().toInstance(new TypeAdaptingWidget() {
            @Override
            public boolean supports(Class<?> clazzType) {
                return List.class.isAssignableFrom(clazzType);
            }

            @Override
            public TypeWidget adapt(ProgramValueTypeAdapter typeAdapter, Type type) {
                return new ListTypeWidget(getPackageSafeRawType(type, List.class), typeAdapter.adaptInternal(JVMTypes.getTypeArgument(type, 0)));
            }
        });
        binder.addBinding().toInstance(new TypeAdaptingWidget() {
            @Override
            public boolean supports(Class<?> clazzType) {
                return Provider.class.isAssignableFrom(clazzType);
            }

            @Override
            public TypeWidget adapt(ProgramValueTypeAdapter typeAdapter, Type type) {
                return new ReflectiveJavaTypeWidget(typeAdapter, Provider.class);
            }
        });
        binder.addBinding().toInstance(new TypeAdaptingWidget() {
            @Override
            public boolean supports(Class<?> clazzType) {
                return Optional.class.isAssignableFrom(clazzType);
            }

            @Override
            public TypeWidget adapt(ProgramValueTypeAdapter typeAdapter, Type type) {
                return OptionalTypeWidget.create(typeAdapter.adaptInternal(JVMTypes.getTypeArgument(type, 0)));
            }
        });

        bind(TypeAdaptingWidget.class).to(ReflectiveTypeAdapter.class);
    }

    private org.objectweb.asm.Type getPackageSafeRawType(Type type, Class<?> acceptableType) {
        Class<?> clazz = JVMTypes.getRawType(type);
        if(!Modifier.isPublic(clazz.getModifiers())) {
            clazz = acceptableType;
        }
        return org.objectweb.asm.Type.getType(clazz);
    }
}
