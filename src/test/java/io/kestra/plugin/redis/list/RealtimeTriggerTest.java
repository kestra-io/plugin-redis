package io.kestra.plugin.redis.list;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

class RealtimeTriggerTest extends AbstractTriggerTest {
    @Override
    protected String getKey() {
        return "mytriggerkey_realtime";
    }

    @Test
    @Disabled("TODO: migrate to new RealtimeTrigger test pattern")
    void flow() throws Exception {
    }
}
