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

package io.airbyte.integrations.destination.gcs.util;

import static io.airbyte.integrations.destination.gcs.GcsDestinationConstants.NAME_TRANSFORMER;

import io.airbyte.protocol.models.AirbyteStream;
import java.util.LinkedList;
import java.util.List;

public class GcsOutputPathHelper {

  public static String getOutputPrefix(String bucketPath, AirbyteStream stream) {
    return getOutputPrefix(bucketPath, stream.getNamespace(), stream.getName());
  }

  /**
   * Prefix: <bucket-path>/<source-namespace-if-present>/<stream-name>
   */
  public static String getOutputPrefix(String bucketPath, String namespace, String streamName) {
    List<String> paths = new LinkedList<>();

    if (bucketPath != null) {
      paths.add(bucketPath);
    }
    if (namespace != null) {
      paths.add(NAME_TRANSFORMER.convertStreamName(namespace));
    }
    paths.add(NAME_TRANSFORMER.convertStreamName(streamName));

    return String.join("/", paths);
  }

}
