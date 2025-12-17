package io.kestra.plugin.redis.cli;

import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtensionContext;

import org.testcontainers.DockerClientFactory;

/**
 * Prevents Docker-dependent tests from starting (and therefore prevents Micronaut/Kestra
 * test context initialization) when Docker is not reachable in the current environment.
 */
public final class DockerAvailableCondition implements ExecutionCondition {
    private static final ConditionEvaluationResult ENABLED =
        ConditionEvaluationResult.enabled("Docker is available");

    @Override
    public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {
        try {
            if (DockerClientFactory.instance().isDockerAvailable()) {
                return ENABLED;
            }

            return ConditionEvaluationResult.disabled("Docker is not available");
        } catch (Throwable t) {
            String message = t.getMessage();
            if (message == null || message.isBlank()) {
                message = t.getClass().getName();
            }
            return ConditionEvaluationResult.disabled("Docker is not available: " + message);
        }
    }
}
