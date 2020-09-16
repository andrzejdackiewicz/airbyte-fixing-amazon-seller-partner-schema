/*
 * MIT License
 *
 * Copyright (c) 2020 Dataline
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

package io.dataline.commons.io;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.never;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class LineGobblerTest {

  @Test
  @SuppressWarnings("unchecked")
  void readAllLines() {
    final Consumer<String> consumer = Mockito.mock(Consumer.class);
    final InputStream is = new ByteArrayInputStream("test\ntest2\n".getBytes(StandardCharsets.UTF_8));

    LineGobbler.gobble(is, consumer);

    Mockito.verify(consumer).accept("test");
    Mockito.verify(consumer).accept("test2");
  }

  @Test
  @SuppressWarnings("unchecked")
  void shutdownOnSuccess() throws InterruptedException {
    final Consumer<String> consumer = Mockito.mock(Consumer.class);
    final InputStream is = new ByteArrayInputStream("test\ntest2\n".getBytes(StandardCharsets.UTF_8));

    final ExecutorService executor = Executors.newSingleThreadExecutor();
    executor.submit(new LineGobbler(is, consumer, executor));

    Mockito.verify(consumer, Mockito.times(2)).accept(anyString());
    executor.awaitTermination(10, TimeUnit.SECONDS);
    Assertions.assertTrue(executor.isTerminated());
  }

  @Test
  @SuppressWarnings("unchecked")
  void shutdownOnError() throws InterruptedException {
    final Consumer<String> consumer = Mockito.mock(Consumer.class);
    Mockito.doThrow(RuntimeException.class).when(consumer).accept(anyString());
    final InputStream is = Mockito.spy(new ByteArrayInputStream("test\ntest2\n".getBytes(StandardCharsets.UTF_8)));

    final ExecutorService executor = Executors.newSingleThreadExecutor();
    executor.submit(new LineGobbler(is, consumer, executor));

    Mockito.verify(consumer, never()).accept(anyString());
    executor.awaitTermination(10, TimeUnit.SECONDS);
    Assertions.assertTrue(executor.isTerminated());
  }

}
