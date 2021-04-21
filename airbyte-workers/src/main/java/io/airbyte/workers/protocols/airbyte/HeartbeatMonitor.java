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

package io.airbyte.workers.protocols.airbyte;

import com.google.common.annotations.VisibleForTesting;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

public class HeartbeatMonitor {

  public static final int TIME_MAGNITUDE = 30;
  public static final TemporalUnit TIME_UNIT = ChronoUnit.SECONDS;

  private final Supplier<Instant> instantSupplier;
  private final AtomicReference<Instant> lastBeat;

  public HeartbeatMonitor() {
    this(Instant::now);
  }

  @VisibleForTesting
  public HeartbeatMonitor(Supplier<Instant> instantSupplier) {
    this.instantSupplier = instantSupplier;
    this.lastBeat = new AtomicReference<>(null);
  }

  public void beat() {
    lastBeat.set(instantSupplier.get());
  }

  public boolean isBeating() {
    final Instant instantFetched = lastBeat.get();
    final Instant now = instantSupplier.get();
    return instantFetched != null && instantFetched.plus(TIME_MAGNITUDE, TIME_UNIT).isAfter(now);
  }

}
