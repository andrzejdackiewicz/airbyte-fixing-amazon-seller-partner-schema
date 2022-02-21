/*
 * Copyright (c) 2021 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.metrics.lib;

/**
 * Interface representing an Airbyte Application to collect metrics for. This interface is present
 * as Java doesn't support enum inheritance as of Java 17, so we use a shared interface so this
 * interface can be used by the {@link AirbyteMetric} class in the {@link AirbyteMetricsRegistry}
 * enum.
 */
public interface AirbyteApplication {

  String getApplicationName();

}
