/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.hops.util.exceptions;

/**
 * Exception thrown when the supplied dataframe for computing feature statistics is empty
 * 
 */
public class DataframeIsEmpty extends Exception {

  Integer status;

  public DataframeIsEmpty(String message) {
    super(message);
  }

  public DataframeIsEmpty(Integer status, String message) {
    super(message);
    this.status = status;
  }

  public Integer getStatus() {
    return status;
  }

  public void setStatus(Integer status) {
    this.status = status;
  }
}
