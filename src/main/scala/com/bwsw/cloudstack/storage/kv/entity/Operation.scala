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

/** Operation used to change data in a storage **/
sealed trait Operation

object Operation {

  private val SetValue = "set"
  private val DeleteValue = "delete"
  private val ClearValue = "clear"

  object Set extends Operation {
    override def toString: String = SetValue
  }

  object Delete extends Operation {
    override def toString: String = DeleteValue
  }

  object Clear extends Operation {
    override def toString: String = ClearValue
  }

  /** Returns the operation by its string representation.
    *
    * @throws scala.IllegalArgumentException if the string ain't match any of operations
    */
  def parse(string: String): Operation = string match {
    case SetValue => Set
    case DeleteValue => Delete
    case ClearValue => Clear
    case _ => throw new IllegalArgumentException("Invalid operation")
  }
}
