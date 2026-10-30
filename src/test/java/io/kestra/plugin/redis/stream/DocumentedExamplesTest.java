package io.kestra.plugin.redis.stream;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.time.Duration;
import java.util.List;

import org.junit.jupiter.api.Test;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.flows.Flow;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.models.triggers.AbstractTrigger;
import io.kestra.core.models.validations.ModelValidator;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.YamlParser;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;

// A documented example that is not a valid flow is a broken copy-paste starting point, so every one is checked.
@KestraTest
class DocumentedExamplesTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private ModelValidator modelValidator;

    @Test
    void everyDocumentedExampleIsAValidTypedFlow() throws Exception {
        var checked = 0;
        for (var type : List.of(GroupCreate.class, ReadGroup.class, Ack.class, Claim.class, Trigger.class)) {
            for (var example : type.getAnnotation(Plugin.class).examples()) {
                if (!example.full()) {
                    continue;
                }

                // typed parsing fails on an unknown property or a wrong type of any task or trigger of the example
                var flow = YamlParser.parse(String.join("\n", example.code()), Flow.class);

                var violations = modelValidator.isValid(flow);
                assertThat(type.getSimpleName() + " example '" + flow.getId() + "': " + violations, violations.isPresent(), is(false));

                // a literal Property is converted and bounded only when it is rendered, so render each simple one
                for (var task : flow.allTasksWithChilds()) {
                    renderLiterals(flow.getId(), task);
                }
                if (flow.getTriggers() != null) {
                    for (var trigger : flow.getTriggers()) {
                        renderLiterals(flow.getId(), trigger);
                    }
                }
                checked++;
            }
        }
        assertThat(checked, greaterThan(0));
    }

    private void renderLiterals(String flowId, Object holder) throws Exception {
        var runContext = runContextFactory.of();
        for (Class<?> type = holder.getClass(); type != null && type != Object.class; type = type.getSuperclass()) {
            for (var field : type.getDeclaredFields()) {
                if (!Property.class.isAssignableFrom(field.getType()) || !(field.getGenericType() instanceof ParameterizedType generic)) {
                    continue;
                }

                var target = simpleTarget(generic.getActualTypeArguments()[0]);
                field.setAccessible(true);
                var property = (Property<?>) field.get(holder);
                if (target == null || property == null || String.valueOf(property).contains("{{")) {
                    continue;
                }

                try {
                    @SuppressWarnings({ "unchecked", "rawtypes" })
                    var rendered = runContext.render((Property) property).as((Class) target).orElseThrow();

                    // Kestra checks @Min and @Max of the type argument on a typed value only, not on a YAML literal,
                    // so the converted value is put back to let the validator apply the bounds
                    field.set(holder, Property.ofValue(rendered));
                    if (holder instanceof Task || holder instanceof AbstractTrigger) {
                        var violations = modelValidator.isValid(holder);
                        assertThat(String.valueOf(violations), violations.isPresent(), is(false));
                    }
                    field.set(holder, property);
                } catch (Exception e) {
                    throw new AssertionError(
                        "example '" + flowId + "': " + holder.getClass().getSimpleName() + "." + field.getName()
                            + " = " + property + " is not a valid " + target.getSimpleName() + ": " + e.getMessage(),
                        e
                    );
                }
            }
        }
    }

    // only types that a YAML literal can be converted to without help of the flow context
    private static Class<?> simpleTarget(Type argument) {
        if (argument instanceof Class<?> type
            && (type == Integer.class || type == Float.class || type == Boolean.class || type == Duration.class || type.isEnum())) {
            return type;
        }
        return null;
    }
}
