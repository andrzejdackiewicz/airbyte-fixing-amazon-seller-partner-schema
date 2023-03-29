// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package io.airbyte.integrations.destination.starrocks.exception;

public class StreamLoadFailException extends Exception {

  public StreamLoadFailException() {
    super();
  }

  public StreamLoadFailException(String message) {
    super(message);
  }

  public StreamLoadFailException(String message, Throwable cause) {
    super(message, cause);
  }

  public StreamLoadFailException(Throwable cause) {
    super(cause);
  }

  protected StreamLoadFailException(String message,
                                    Throwable cause,
                                    boolean enableSuppression,
                                    boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }

}
