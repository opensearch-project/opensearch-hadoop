/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.hadoop.rest.commonshttp.auth.gcp;

import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;
import org.opensearch.hadoop.OpenSearchHadoopIllegalStateException;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class GcpOidcCredentialsProviderTest {

    private static final int REFRESH_WINDOW = 300;
    private static final Instant NOW = Instant.ofEpochSecond(1673626117); // 2023-01-13 16:08:37 +0000

    /**
     * A token source that hands out a distinct token value per call and records how often it
     * was invoked, so caching and refresh behavior can be asserted without live credentials.
     */
    private static class CountingTokenSource implements GcpTokenSource {
        private final AtomicInteger calls = new AtomicInteger(0);
        private final Instant expiresAt;

        CountingTokenSource(Instant expiresAt) {
            this.expiresAt = expiresAt;
        }

        @Override
        public GcpToken fetchToken() {
            int call = calls.incrementAndGet();
            return new GcpToken("token-" + call, expiresAt);
        }

        int getCalls() {
            return calls.get();
        }
    }

    private static Clock fixedClock(Instant instant) {
        return Clock.fixed(instant, ZoneOffset.UTC);
    }

    @Test
    public void testTokenIsCachedWhileValid() {
        CountingTokenSource source = new CountingTokenSource(NOW.plusSeconds(3600));
        GcpOidcCredentialsProvider provider =
                new GcpOidcCredentialsProvider(source, REFRESH_WINDOW, fixedClock(NOW));

        assertThat(provider.getTokenValue(), is("token-1"));
        assertThat(provider.getTokenValue(), is("token-1"));
        assertThat(provider.getTokenValue(), is("token-1"));
        assertThat(source.getCalls(), is(1));
    }

    @Test
    public void testTokenIsRefreshedOnceInsideRefreshWindow() {
        // Expires in 60s, inside the 300s refresh window, so every call must refresh.
        CountingTokenSource source = new CountingTokenSource(NOW.plusSeconds(60));
        GcpOidcCredentialsProvider provider =
                new GcpOidcCredentialsProvider(source, REFRESH_WINDOW, fixedClock(NOW));

        assertThat(provider.getTokenValue(), is("token-1"));
        assertThat(provider.getTokenValue(), is("token-2"));
        assertThat(source.getCalls(), is(2));
    }

    @Test
    public void testTokenExactlyAtRefreshWindowBoundaryIsRefreshed() {
        // Expiry exactly at now + window is treated as already expired.
        CountingTokenSource source = new CountingTokenSource(NOW.plusSeconds(REFRESH_WINDOW));
        GcpOidcCredentialsProvider provider =
                new GcpOidcCredentialsProvider(source, REFRESH_WINDOW, fixedClock(NOW));

        assertThat(provider.getTokenValue(), is("token-1"));
        assertThat(provider.getTokenValue(), is("token-2"));
    }

    @Test
    public void testTokenWithoutExpiryIsAlwaysRefreshed() {
        CountingTokenSource source = new CountingTokenSource(null);
        GcpOidcCredentialsProvider provider =
                new GcpOidcCredentialsProvider(source, REFRESH_WINDOW, fixedClock(NOW));

        assertThat(provider.getTokenValue(), is("token-1"));
        assertThat(provider.getTokenValue(), is("token-2"));
        assertThat(source.getCalls(), is(2));
    }

    @Test
    public void testExpiredTokenIsRefreshedAsClockAdvances() {
        final Instant expiry = NOW.plusSeconds(3600);
        GcpTokenSource source = new CountingTokenSource(expiry);
        // Mutable clock: starts before the refresh window, then advances past it.
        final Instant[] current = new Instant[] { NOW };
        Clock advancing = new Clock() {
            @Override
            public ZoneOffset getZone() {
                return ZoneOffset.UTC;
            }

            @Override
            public Clock withZone(java.time.ZoneId zone) {
                return this;
            }

            @Override
            public Instant instant() {
                return current[0];
            }
        };

        GcpOidcCredentialsProvider provider =
                new GcpOidcCredentialsProvider(source, REFRESH_WINDOW, advancing);

        assertThat(provider.getTokenValue(), is("token-1"));
        // Advance to within the refresh window of expiry.
        current[0] = expiry.minusSeconds(REFRESH_WINDOW - 1);
        assertThat(provider.getTokenValue(), is("token-2"));
    }

    @Test
    public void testIOExceptionIsWrapped() {
        GcpTokenSource failing = new GcpTokenSource() {
            @Override
            public GcpToken fetchToken() throws IOException {
                throw new IOException("no credentials available");
            }
        };
        GcpOidcCredentialsProvider provider =
                new GcpOidcCredentialsProvider(failing, REFRESH_WINDOW, fixedClock(NOW));

        try {
            provider.getTokenValue();
            fail("Expected OpenSearchHadoopIllegalStateException");
        } catch (OpenSearchHadoopIllegalStateException e) {
            assertThat(e.getCause() instanceof IOException, is(true));
        }
    }

    @Test(expected = OpenSearchHadoopIllegalStateException.class)
    public void testNullTokenIsRejected() {
        GcpTokenSource nullSource = new GcpTokenSource() {
            @Override
            public GcpToken fetchToken() {
                return null;
            }
        };
        new GcpOidcCredentialsProvider(nullSource, REFRESH_WINDOW, fixedClock(NOW)).getTokenValue();
    }

    @Test(expected = OpenSearchHadoopIllegalStateException.class)
    public void testEmptyTokenValueIsRejected() {
        GcpTokenSource emptySource = new GcpTokenSource() {
            @Override
            public GcpToken fetchToken() {
                return new GcpToken("", NOW.plusSeconds(3600));
            }
        };
        new GcpOidcCredentialsProvider(emptySource, REFRESH_WINDOW, fixedClock(NOW)).getTokenValue();
    }
}
