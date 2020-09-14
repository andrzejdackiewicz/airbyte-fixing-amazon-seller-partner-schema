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

package io.dataline.scheduler;

import com.google.common.base.Preconditions;
import io.dataline.config.JobConfig;

public class ScopeHelper {

  private static final String SCOPE_DELIMITER = ":";

  public static String getScopePrefix(JobConfig.ConfigType configType) {
    return configType.value();
  }

  public static String createScope(JobConfig.ConfigType configType, String configId) {
    Preconditions.checkNotNull(configType);
    Preconditions.checkNotNull(configId);
    return getScopePrefix(configType) + SCOPE_DELIMITER + configId;
  }

  public static String getConfigId(String scope) {
    Preconditions.checkNotNull(scope);

    final String[] split = scope.split(SCOPE_DELIMITER);
    if (split.length <= 1) {
      return "";
    } else {
      return split[1];
    }
  }

}
