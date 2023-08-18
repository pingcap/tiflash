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

#include <memory>
#include <mutex>


/** Allow to store and read-only usage of an object in several threads,
  *  and to atomically replace an object in another thread.
  * The replacement is atomic and reading threads can work with different versions of an object.
  *
  * Usage:
  *  MultiVersion<T> x;
  * - on data update:
  *  x.set(new value);
  * - on read-only usage:
  * {
  *     MultiVersion<T>::Version current_version = x.get();
  *     // use *current_version
  * }   // now we finish own current version; if the version is outdated and no one else is using it - it will be destroyed.
  *
  * All methods are thread-safe.
  */
template <typename T>
class MultiVersion
{
public:
    /// Version of object for usage. shared_ptr manage lifetime of version.
    using Version = std::shared_ptr<const T>;

    /// Default initialization - by nullptr.
    MultiVersion() = default;

    MultiVersion(std::unique_ptr<const T> && value) { set(std::move(value)); }

    /// Obtain current version for read-only usage. Returns shared_ptr, that manages lifetime of version.
    Version get() const
    {
        /// NOTE: is it possible to lock-free replace of shared_ptr?
        std::lock_guard lock(mutex);
        return current_version;
    }

    /// Update an object with new version.
    void set(std::unique_ptr<const T> && value)
    {
        std::lock_guard lock(mutex);
        current_version = std::move(value);
    }

private:
    Version current_version;
    mutable std::mutex mutex;
};
