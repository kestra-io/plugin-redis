package io.kestra.plugin.redis.cli;

import java.util.List;

import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * Pure unit tests for the shell-quoting / tokenization logic that protects the
 * redis-cli invocation against shell injection (CWE-78 / OWASP A03:2021).
 * These run without Docker or a live Redis.
 */
class RedisCLIShellQuotingTest {

    // --- shellQuote (used for host, username and every command token) ---

    @Test
    void shellQuote_wrapsValueInSingleQuotes() {
        assertThat(RedisCLI.shellQuote("localhost"), is("'localhost'"));
    }

    @Test
    void shellQuote_escapesEmbeddedSingleQuote() {
        // ' -> '\'' so the value stays a single literal argument
        assertThat(RedisCLI.shellQuote("it's"), is("'it'\\''s'"));
    }

    @Test
    void shellQuote_neutralizesCommandSubstitutionInHost() {
        // finding #1: host must not be able to break out via $(...)
        String quoted = RedisCLI.shellQuote("x$(touch /tmp/pwned)");
        assertThat(quoted, is("'x$(touch /tmp/pwned)'"));
        // the metacharacters are inside single quotes -> inert to /bin/sh
        assertThat(quoted, startsWith("'"));
        assertThat(quoted, endsWith("'"));
    }

    // --- tokenize ---

    @Test
    void tokenize_splitsOnWhitespace() {
        assertThat(RedisCLI.tokenize("GET mykey"), contains("GET", "mykey"));
    }

    @Test
    void tokenize_honorsDoubleQuotedGroup() {
        assertThat(
            RedisCLI.tokenize("SET mykey \"Hello World\""),
            contains("SET", "mykey", "Hello World")
        );
    }

    @Test
    void tokenize_honorsSingleQuotedGroup() {
        assertThat(
            RedisCLI.tokenize("SET k 'value with spaces'"),
            contains("SET", "k", "value with spaces")
        );
    }

    @Test
    void tokenize_honorsBackslashEscapedWhitespace() {
        // finding #3: "a\ b" is a single argument "a b", matching the shell
        // word-splitting the /bin/sh wrapper previously performed.
        assertThat(RedisCLI.tokenize("SET key a\\ b"), contains("SET", "key", "a b"));
        assertThat(RedisCLI.tokenize("GET a\\ b\\ c"), contains("GET", "a b c"));
    }

    @Test
    void tokenize_keepsInjectionMetacharactersAsPlainTokens() {
        // ";" and command names are just tokens, never separators
        assertThat(
            RedisCLI.tokenize("GET foo; touch /tmp/pwned"),
            contains("GET", "foo;", "touch", "/tmp/pwned")
        );
    }

    // --- shellQuoteRedisCommand (composition of tokenize + shellQuote) ---

    @Test
    void shellQuoteRedisCommand_preservesMultiArgumentSemantics() {
        assertThat(
            RedisCLI.shellQuoteRedisCommand("SET mykey \"Hello World\""),
            is("'SET' 'mykey' 'Hello World'")
        );
    }

    @Test
    void shellQuoteRedisCommand_preservesBackslashEscapedArgument() {
        // finding #3 end-to-end: single value "a b" survives as one quoted arg
        assertThat(
            RedisCLI.shellQuoteRedisCommand("SET key a\\ b"),
            is("'SET' 'key' 'a b'")
        );
    }

    @Test
    void shellQuoteRedisCommand_neutralizesCommandInjection() {
        // finding #5: every metacharacter ends up inside single quotes,
        // so /bin/sh -c cannot execute the injected `touch`.
        String quoted = RedisCLI.shellQuoteRedisCommand("GET foo; touch /tmp/pwned");
        assertThat(quoted, is("'GET' 'foo;' 'touch' '/tmp/pwned'"));
        assertThat(quoted, not(containsString("; touch")));
    }

    @Test
    void shellQuoteRedisCommand_neutralizesCommandSubstitution() {
        String quoted = RedisCLI.shellQuoteRedisCommand("GET $(whoami)");
        assertThat(quoted, is("'GET' '$(whoami)'"));
    }

    @Test
    void shellQuoteRedisCommand_handlesEmptyInput() {
        assertThat(RedisCLI.shellQuoteRedisCommand(""), is(""));
        assertThat(RedisCLI.tokenize(""), is(List.of()));
    }
}
