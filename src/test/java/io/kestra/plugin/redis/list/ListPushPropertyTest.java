package io.kestra.plugin.redis.list;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.plugin.redis.pubsub.Publish;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
class ListPushPropertyTest {

    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void shouldResolveStringPropertyInListPush() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of());
        Object result = ListPush.resolveFrom(Property.ofValue("kestra-storage:///data/items.ion"), runContext);
        assertThat(result, is("kestra-storage:///data/items.ion"));
    }

    @Test
    void shouldResolveListPropertyInListPush() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of());
        Object result = ListPush.resolveFrom(Property.ofValue(List.of("item1", "item2")), runContext);
        assertThat(result, is(List.of("item1", "item2")));
    }

    @Test
    void shouldResolveExpressionPropertyInListPush() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of("upstreamUri", "kestra-storage:///data/evaluated.ion"));
        Object result = ListPush.resolveFrom(Property.ofExpression("{{ upstreamUri }}"), runContext);
        assertThat(result, is("kestra-storage:///data/evaluated.ion"));
    }

    @Test
    void shouldRejectInvalidPropertyTypeInListPush() {
        RunContext runContext = runContextFactory.of(Map.of());
        assertThrows(IllegalVariableEvaluationException.class, () ->
            ListPush.resolveFrom(Property.ofValue(123), runContext)
        );
    }

    @Test
    void shouldResolveStringPropertyInPublish() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of());
        Object result = Publish.resolveFrom(Property.ofValue("kestra-storage:///data/items.ion"), runContext);
        assertThat(result, is("kestra-storage:///data/items.ion"));
    }

    @Test
    void shouldResolveListPropertyInPublish() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of());
        Object result = Publish.resolveFrom(Property.ofValue(List.of("item1", "item2")), runContext);
        assertThat(result, is(List.of("item1", "item2")));
    }

    @Test
    void shouldRejectInvalidPropertyTypeInPublish() {
        RunContext runContext = runContextFactory.of(Map.of());
        assertThrows(IllegalVariableEvaluationException.class, () ->
            Publish.resolveFrom(Property.ofValue(123), runContext)
        );
    }
}
