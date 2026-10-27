package io.kestra.plugin.redis.cli;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.utils.IdUtils;
import io.kestra.plugin.scripts.runner.docker.Docker;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;

@KestraTest
class RedisCLIRunnerCopyTest {

    @Test
    void freshTaskRunnerReturnsAnIndependentCopy() throws Exception {
        var runner = Docker.builder()
            .type(Docker.class.getName())
            .networkMode("custom-net")
            .build();

        var task = RedisCLI.builder()
            .id(IdUtils.create())
            .type(RedisCLI.class.getName())
            .host(Property.ofValue("localhost"))
            .commands(Property.ofValue(List.of("PING")))
            .build();

        var copy = task.freshTaskRunner(runner);

        assertThat(copy, not(sameInstance(runner)));
        assertThat(copy, instanceOf(Docker.class));
        assertThat(((Docker) copy).getNetworkMode(), is("custom-net"));
    }
}
