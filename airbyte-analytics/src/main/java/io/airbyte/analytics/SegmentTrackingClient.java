/*
 * MIT License
 *
 * Copyright (c) 2020 Airbyte
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package io.airbyte.analytics;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.segment.analytics.Analytics;
import com.segment.analytics.messages.IdentifyMessage;
import com.segment.analytics.messages.TrackMessage;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.logging.log4j.util.Strings;

public class SegmentTrackingClient implements TrackingClient {

  private static final String SEGMENT_WRITE_KEY = "7UDdp5K55CyiGgsauOr2pNNujGvmhaeu";
  private static final String AIRBYTE_VERSION_KEY = "airbyte_version";
  private static final String AIRBYTE_ROLE = "airbyte_role";

  // Analytics is threadsafe.
  private final Analytics analytics;
  private final Supplier<TrackingIdentity> identitySupplier;
  private final Supplier<String> roleSupplier;

  @VisibleForTesting
  SegmentTrackingClient(final Supplier<TrackingIdentity> identitySupplier,
                        final Analytics analytics,
                        final Supplier<String> roleSupplier) {
    this.identitySupplier = identitySupplier;
    this.analytics = analytics;
    this.roleSupplier = roleSupplier;
  }

  public SegmentTrackingClient(Supplier<TrackingIdentity> identitySupplier) {
    this(identitySupplier, Analytics.builder(SEGMENT_WRITE_KEY).build(), () -> System.getenv(AIRBYTE_ROLE.toUpperCase()));
  }

  @Override
  public void identify() {
    final TrackingIdentity trackingIdentity = identitySupplier.get();
    final ImmutableMap.Builder<String, Object> identityMetadataBuilder = ImmutableMap.<String, Object>builder()
        .put(AIRBYTE_VERSION_KEY, trackingIdentity.getAirbyteVersion())
        .put("anonymized", trackingIdentity.isAnonymousDataCollection())
        .put("subscribed_newsletter", trackingIdentity.isNews())
        .put("subscribed_security", trackingIdentity.isSecurityUpdates());

    final String airbyteRole = roleSupplier.get();
    if (!Strings.isEmpty(airbyteRole)) {
      identityMetadataBuilder.put(AIRBYTE_ROLE, airbyteRole);
    }

    trackingIdentity.getEmail().ifPresent(email -> identityMetadataBuilder.put("email", email));

    analytics.enqueue(IdentifyMessage.builder()
        .userId(trackingIdentity.getCustomerId().toString())
        .traits(identityMetadataBuilder.build()));
  }

  @Override
  public void track(String action) {
    track(action, Collections.emptyMap());
  }

  @Override
  public void track(String action, Map<String, Object> metadata) {
    final Map<String, Object> mapCopy = new HashMap<>(metadata);
    final TrackingIdentity trackingIdentity = identitySupplier.get();
    mapCopy.put(AIRBYTE_VERSION_KEY, trackingIdentity.getAirbyteVersion());
    analytics.enqueue(TrackMessage.builder(action)
        .userId(trackingIdentity.getCustomerId().toString())
        .properties(mapCopy));
  }

}
