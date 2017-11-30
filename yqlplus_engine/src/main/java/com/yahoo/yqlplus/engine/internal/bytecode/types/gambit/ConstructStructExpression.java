package com.yahoo.yqlplus.engine.internal.bytecode.types.gambit;

import com.yahoo.yqlplus.engine.internal.compiler.CodeEmitter;
import com.yahoo.yqlplus.engine.internal.plan.types.AssignableValue;
import com.yahoo.yqlplus.engine.internal.plan.types.BytecodeExpression;
import com.yahoo.yqlplus.engine.internal.plan.types.OptionalAdapter;
import com.yahoo.yqlplus.engine.internal.plan.types.TypeWidget;
import com.yahoo.yqlplus.engine.internal.plan.types.base.BaseTypeExpression;
import com.yahoo.yqlplus.engine.internal.plan.types.base.BytecodeCastExpression;
import com.yahoo.yqlplus.engine.internal.plan.types.base.NotNullableTypeWidget;
import com.yahoo.yqlplus.engine.internal.plan.types.base.PropertyAdapter;
import org.objectweb.asm.Label;

import java.util.List;

public class ConstructStructExpression extends BaseTypeExpression {
    private final List<PropertyOperation> operationNodes;

    public ConstructStructExpression(TypeWidget type, List<PropertyOperation> operationNodes) {
        super(NotNullableTypeWidget.create(type));
        this.operationNodes = operationNodes;
    }

    @Override
    public void generate(CodeEmitter top) {
        CodeEmitter code = top.createScope();
        BytecodeExpression input = code.evaluateOnce(getType().construct());
        PropertyAdapter inputAdapter = getType().getPropertyAdapter();
        for(PropertyOperation op : operationNodes) {
            if("".equals(op.fieldName)) {
                // merge
                BytecodeExpression source = op.value;
                OptionalAdapter adapt =  source.getType().getOptionalAdapter();
                adapt.generate(code, source,
                        (avail, value) -> {
                            PropertyAdapter adapter = value.getType().getPropertyAdapter();
                            if(adapter.isClosed()) {
                                for(PropertyAdapter.Property property : adapter.getProperties()) {
                                    AssignableValue out = inputAdapter.property(input, property.name);
                                    writePropertyValue(avail, adapter.property(value, property.name), out);
                                }
                            } else {
                                avail.exec(adapter.visitProperties(value,
                                        (availCode, propertyName, propertyValue, abortLoop, nextItem) -> {
                                            AssignableValue out = inputAdapter.index(input, propertyName);
                                            availCode.exec(out.write(propertyValue));
                                        }));
                            }

                        },
                        (gen, missingValue) -> {
                            // missing, do nothing re: merging properties
                        }
                );
            } else {
                // set
                AssignableValue out = inputAdapter.property(input, op.fieldName);
                writePropertyValue(code, op.value, out);

            }
        }
        code.exec(input);
        code.endScope();

    }

    private void writePropertyValue(CodeEmitter avail, BytecodeExpression value, AssignableValue out) {
        if(value.getType().isNullable()) {
            Label nextItem = new Label();
            value = avail.evaluateOnce(value);
            avail.gotoIfNull(value, nextItem);
            avail.exec(out.write(new BytecodeCastExpression(NotNullableTypeWidget.create(out.getType()), value)));
            avail.getMethodVisitor().visitLabel(nextItem);
        } else {
            avail.exec(out.write(value));
        }
    }
}
