// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.bwsw.cloudstack.storage.kv.entity

/** Operation that where used to change data existing in some storage **/
sealed trait Operation

object Set extends Operation {
  override def toString: String = "set"
}

object Delete extends Operation {
  override def toString: String = "delete"
}

object Clear extends Operation {
  override def toString: String = "clear"
}

object Operation {

  /** Returns the operation by its string representation.
    *
    * @throws IllegalArgumentException if the string ain't match any of operations
    */
  def parse(string: String): Operation = string match {
    case "set" => Set
    case "delete" => Delete
    case "clear" => Clear
    case _ => throw new IllegalArgumentException("Invalid operation")
  }
}
