package io.kestra.plugin.redis.json;

import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtensionContext;

public class RedisStackAvailableCondition implements ExecutionCondition {
    @Override
    public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {
        try {
            String uri = RedisStackTestSupport.redisUriOrNull();
            if (uri != null && !uri.isBlank()) {
                return ConditionEvaluationResult.enabled("Redis Stack is available");
            }

            return ConditionEvaluationResult.disabled("Redis Stack (RedisJSON) is not available");
        } catch (Throwable t) {
            return ConditionEvaluationResult.disabled("Redis Stack (RedisJSON) is not available: " + t.getMessage());
        }
    }
}
