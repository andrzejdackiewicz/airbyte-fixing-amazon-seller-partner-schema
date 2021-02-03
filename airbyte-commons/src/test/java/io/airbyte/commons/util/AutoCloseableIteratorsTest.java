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

package io.airbyte.commons.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import io.airbyte.commons.concurrency.VoidCallable;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

class AutoCloseableIteratorsTest {

  @Test
  void testFromIterator() throws Exception {
    final VoidCallable onClose = mock(VoidCallable.class);
    final AutoCloseableIterator<String> iterator = AutoCloseableIterators.fromIterator(MoreIterators.of("a", "b", "c"), onClose);

    assertNext(iterator, "a");
    assertNext(iterator, "b");
    assertNext(iterator, "c");
    iterator.close();

    verify(onClose).call();
  }

  @Test
  void testFromStream() throws Exception {
    final Stream<String> stream = spy(Stream.of("a", "b", "c"));
    final AutoCloseableIterator<String> iterator = AutoCloseableIterators.fromStream(stream);

    assertNext(iterator, "a");
    assertNext(iterator, "b");
    assertNext(iterator, "c");
    iterator.close();

    verify(stream).close();
  }

  private void assertNext(Iterator<String> iterator, String value) {
    assertTrue(iterator.hasNext());
    assertEquals(value, iterator.next());
  }

  @Test
  void testAppendOnClose() throws Exception {
    final VoidCallable onClose1 = mock(VoidCallable.class);
    final VoidCallable onClose2 = mock(VoidCallable.class);

    final AutoCloseableIterator<Integer> iterator = AutoCloseableIterators.fromIterator(MoreIterators.of(1, 2, 3), onClose1);
    final AutoCloseableIterator<Integer> iteratorWithExtraClose = AutoCloseableIterators.appendOnClose(iterator, onClose2);

    iteratorWithExtraClose.close();
    verify(onClose1).call();
    verify(onClose2).call();
  }

  @Test
  void testTransform() {
    final Iterator<Integer> transform = Iterators.transform(MoreIterators.of(1, 2, 3), i -> i + 1);
    assertEquals(ImmutableList.of(2, 3, 4), MoreIterators.toList(transform));
  }

  @Test
  void testConcatWithEagerClose() throws Exception {
    final VoidCallable onClose1 = mock(VoidCallable.class);
    final VoidCallable onClose2 = mock(VoidCallable.class);

    final AutoCloseableIterator<String> iterator = new CompositeIterator<>(ImmutableList.of(
        AutoCloseableIterators.fromIterator(MoreIterators.of("a", "b"), onClose1),
        AutoCloseableIterators.fromIterator(MoreIterators.of("d"), onClose2)));

    assertOnCloseInvocations(ImmutableList.of(), ImmutableList.of(onClose1, onClose2));
    assertNext(iterator, "a");
    assertNext(iterator, "b");
    assertNext(iterator, "d");
    assertOnCloseInvocations(ImmutableList.of(onClose1), ImmutableList.of(onClose2));
    assertFalse(iterator.hasNext());
    assertOnCloseInvocations(ImmutableList.of(onClose1, onClose2), ImmutableList.of());

    iterator.close();

    verify(onClose1, times(1)).call();
    verify(onClose2, times(1)).call();
  }

  private void assertOnCloseInvocations(List<VoidCallable> haveClosed, List<VoidCallable> haveNotClosed) throws Exception {
    for (VoidCallable voidCallable : haveClosed) {
      verify(voidCallable).call();
    }

    for (VoidCallable voidCallable : haveNotClosed) {
      verify(voidCallable, never()).call();
    }
  }

}
