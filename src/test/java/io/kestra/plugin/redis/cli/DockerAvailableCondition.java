package io.kestra.plugin.redis.cli;

import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.io.File;

/**
 * Prevents Docker-dependent tests from starting (and therefore prevents Micronaut/Kestra
 * test context initialization) when Docker is not reachable in the current environment.
 */
public final class DockerAvailableCondition implements ExecutionCondition {
    private static final ConditionEvaluationResult ENABLED =
        ConditionEvaluationResult.enabled("Docker is available");

    @Override
    public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {
        String dockerHost = System.getenv("DOCKER_HOST");
        if (dockerHost != null && !dockerHost.isBlank()) {
            return ENABLED;
        }

        File socket = new File("/var/run/docker.sock");
        if (socket.exists()) {
            return ENABLED;
        }

        return ConditionEvaluationResult.disabled(
            "Docker is not available (missing /var/run/docker.sock and DOCKER_HOST is not set)"
        );
    }
}
