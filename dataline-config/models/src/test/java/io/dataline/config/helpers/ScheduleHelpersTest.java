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

package io.dataline.config.helpers;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.dataline.config.Schedule;
import java.util.Arrays;
import org.junit.jupiter.api.Test;

class ScheduleHelpersTest {

  @Test
  public void testGetSecondsInUnit() {
    assertEquals(60, ScheduleHelpers.getSecondsInUnit(Schedule.TimeUnit.MINUTES));
    assertEquals(3600, ScheduleHelpers.getSecondsInUnit(Schedule.TimeUnit.HOURS));
    assertEquals(86_400, ScheduleHelpers.getSecondsInUnit(Schedule.TimeUnit.DAYS));
    assertEquals(604_800, ScheduleHelpers.getSecondsInUnit(Schedule.TimeUnit.WEEKS));
    assertEquals(2_592_000, ScheduleHelpers.getSecondsInUnit(Schedule.TimeUnit.MONTHS));
  }

  // Will throw if a new TimeUnit is added but an appropriate mapping is not included in this method.
  @Test
  public void testAllOfTimeUnitEnumValues() {
    Arrays.stream(Schedule.TimeUnit.values()).forEach(ScheduleHelpers::getSecondsInUnit);
  }

}
