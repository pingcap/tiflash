// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <atomic>
#include <functional>


namespace DB
{
class ReadBuffer;
class WriteBuffer;


/** Copies data from ReadBuffer to WriteBuffer, all that is.
  */
void copyData(ReadBuffer & from, WriteBuffer & to);

/** Copies `bytes` bytes from ReadBuffer to WriteBuffer. If there are no `bytes` bytes, then throws an exception.
  */
void copyData(ReadBuffer & from, WriteBuffer & to, size_t bytes);

/** The same, with the condition to cancel.
  */
void copyData(ReadBuffer & from, WriteBuffer & to, std::atomic<int> & is_cancelled);
void copyData(ReadBuffer & from, WriteBuffer & to, size_t bytes, std::atomic<int> & is_cancelled);

void copyData(ReadBuffer & from, WriteBuffer & to, std::function<void()> cancellation_hook);
void copyData(ReadBuffer & from, WriteBuffer & to, size_t bytes, std::function<void()> cancellation_hook);

} // namespace DB
