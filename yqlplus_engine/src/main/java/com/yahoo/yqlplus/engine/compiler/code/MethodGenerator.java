/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.compiler.code;

import org.objectweb.asm.*;

import java.lang.reflect.Modifier;

public class MethodGenerator extends FunctionGenerator {
    protected String name;
    protected TypeWidget returnType = BaseTypeAdapter.VOID;

    public MethodGenerator(UnitGenerator unit, String name, boolean isStatic) {
        super(unit, isStatic);
        this.name = name;
    }

    private MethodVisitor createMethod(ClassVisitor cw) {
        final String methodDescriptor = createMethodDescriptor();
        MethodVisitor visitor = cw.visitMethod(modifiers, name, methodDescriptor, null, null);
        generateAnnotations(visitor);
        visitor.visitCode();
        return visitor;
    }

    @Override
    public void generate(ClassVisitor cw) {
        CodeEmitter out = new UnitCodeEmitter(unit, arguments, createMethod(cw));
        code.generate(out);
        try {
            out.getMethodVisitor().visitMaxs(0, 0);
        } catch (ArrayIndexOutOfBoundsException | NegativeArraySizeException e) {
            System.err.println("Frame mismatch generating " + unit.getType().getJVMType().getInternalName() + ":" + name);
            throw e;
        } catch (NullPointerException e) {
            System.err.println("NPE generating " + unit.getType().getJVMType().getInternalName() + ":" + name);
            throw e;
        }
        out.getMethodVisitor().visitEnd();
    }

    public TypeWidget getReturnType() {
        return returnType;
    }

    public void setReturnType(TypeWidget returnType) {
        this.returnType = returnType;
    }

    @Override
    protected Type getReturnJVMType() {
        return getReturnType().getJVMType();
    }

    public void invoke(CodeEmitter code) {
        code.getMethodVisitor().visitMethodInsn(Opcodes.INVOKEVIRTUAL, unit.getInternalName(), name, createMethodDescriptor(), false);
    }

    public void returnValue(BytecodeExpression result) {
        setReturnType(result.getType());
        add(result);
        add(new ReturnCode(result.getType().getJVMType().getOpcode(Opcodes.IRETURN)));
    }

    @Override
    public GambitCreator.Invocable createInvocable() {
        int op = Opcodes.INVOKEVIRTUAL;
        if (Modifier.isStatic(modifiers)) {
            op = Opcodes.INVOKESTATIC;
        }
        return ExactInvocation.exactInvoke(op, name, unit.getType(), getReturnType(), getArgumentTypes());
    }

    public Handle getHandle() {
        int h = Opcodes.H_INVOKEVIRTUAL;
        if(Modifier.isStatic(modifiers)) {
            h = Opcodes.H_INVOKESTATIC;
        }
        return new Handle(h, unit.internalName, name, createMethodDescriptor(), false);
    }
}
