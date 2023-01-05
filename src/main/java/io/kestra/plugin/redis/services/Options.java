package io.kestra.plugin.redis.services;

import io.lettuce.core.SetArgs;
import lombok.Builder;
import lombok.extern.jackson.Jacksonized;

@Builder
@Jacksonized
public class Options {
    private Long ex;

    private Long exAt;

    private Long px;

    private Long pxAt;

    @Builder.Default
    private boolean nx = false;

    @Builder.Default
    private boolean xx = false;

    @Builder.Default
    private boolean keepttl = false;

    public SetArgs getRedisSetArgs() {
        SetArgs setArgs = new SetArgs();
        if (ex != null) {
            setArgs.ex(ex);
        }
        if (exAt != null) {
            setArgs.exAt(exAt);
        }
        if (px != null) {
            setArgs.px(px);
        }
        if (pxAt != null) {
            setArgs.pxAt(pxAt);
        }
        if (nx) {
            setArgs.nx();
        }
        if (xx) {
            setArgs.xx();
        }
        if (keepttl) {
            setArgs.keepttl();
        }
        return setArgs;
    }
}
