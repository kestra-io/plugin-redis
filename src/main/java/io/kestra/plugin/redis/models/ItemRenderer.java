package io.kestra.plugin.redis.models;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.runners.RunContext;

public final class ItemRenderer {
    private ItemRenderer() {
    }

    /**
     * Strings, maps and lists may hold expressions; other values (numbers, booleans...) go to {@link SerdeType} unchanged.
     */
    @SuppressWarnings("unchecked")
    public static Object render(RunContext runContext, Object item) throws IllegalVariableEvaluationException {
        if (item instanceof String itemString) {
            return runContext.render(itemString);
        }
        if (item instanceof Map<?, ?> itemMap) {
            return runContext.render((Map<String, Object>) itemMap);
        }
        if (item instanceof List<?> itemList) {
            List<Object> rendered = new ArrayList<>(itemList.size());
            for (Object element : itemList) {
                rendered.add(render(runContext, element));
            }
            return rendered;
        }
        return item;
    }
}
