// Copyright 2022 PingCAP, Ltd.
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

#include <Common/Exception.h>
#include <Common/HashTable/Hash.h>
#include <Common/SharedLibrary.h>
#include <Core/Types.h>
#include <common/ThreadPool.h>
#include <common/logger_useful.h>

#include <boost/core/noncopyable.hpp>
#include <functional>
#include <iostream>
#include <mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>

namespace DB
{
/** Lets you compile a piece of code that uses the server's header files into the dynamic library.
  * Conducts statistic of calls, and initiates compilation only on the N-th call for one key.
  * Compilation is performed asynchronously, in separate threads, if there are free threads.
  * NOTE: There is no cleaning of obsolete and unnecessary results.
  */
class Compiler
{
public:
    /** path - path to the directory with temporary files - the results of the compilation.
      * The compilation results are saved when the server is restarted,
      *  but use the revision number as part of the key. That is, they become obsolete when the server is updated.
      */
    Compiler(const std::string & path_, size_t threads);
    ~Compiler();

    using HashedKey = UInt128;

    using CodeGenerator = std::function<std::string()>;
    using ReadyCallback = std::function<void(SharedLibraryPtr &)>;

    /** Increase the counter for the given key `key` by one.
      * If the compilation result already exists (already open, or there is a file with the library),
      *  then return ready SharedLibrary.
      * Otherwise, if min_count_to_compile == 0, then initiate the compilation in the same thread, wait for it, and return the result.
      * Otherwise, if the counter has reached min_count_to_compile,
      *  initiate compilation in a separate thread, if there are free threads, and return nullptr.
      * Otherwise, return nullptr.
      */
    SharedLibraryPtr getOrCount(
        const std::string & key,
        UInt32 min_count_to_compile,
        const std::string & additional_compiler_flags,
        CodeGenerator get_code,
        ReadyCallback on_ready);

private:
    using Counts = std::unordered_map<HashedKey, UInt32, DefaultHash<UInt128>>;
    using Libraries = std::unordered_map<HashedKey, SharedLibraryPtr, DefaultHash<UInt128>>;
    using Files = std::unordered_set<std::string>;

    const std::string path;
    ThreadPool pool;

    /// Number of calls to `getOrCount`.
    Counts counts;

    /// Compiled and open libraries. Or nullptr for libraries in the compilation process.
    Libraries libraries;

    /// Compiled files remaining from previous runs, but not yet open.
    Files files;

    std::mutex mutex;

    Poco::Logger * log = &Poco::Logger::get("Compiler");


    void compile(
        HashedKey hashed_key,
        std::string file_name,
        const std::string & additional_compiler_flags,
        CodeGenerator get_code,
        ReadyCallback on_ready);
};

} // namespace DB
