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

#include <Common/config.h>

#if USE_QPL

#include <qpl/qpl.h>

#include <array>
#include <atomic>
#include <cstdio>
#include <map>
#include <memory>
#include <random>

//qpl block size for storing compressed data,aligned with ZLIB.
#define QPL_Compressbound(isize) ((isize) + ((isize) >> 12) + ((isize) >> 14) + ((isize) >> 25) + 13)
typedef unsigned int UInt32;
typedef int Int32;

namespace DB
{
namespace ErrorCodes
{
extern const int QPL_INIT_JOB_FAILED;
extern const int QPL_ACQUIRE_JOB_FAILED;
extern const int QPL_COMPRESS_DATA_FAILED;
extern const int QPL_DECOMPRESS_DATA_FAILED;
} // namespace ErrorCodes
namespace QPL
{
//Init QPL instance and offload data compression/decompression to QPL instance
class CodecDeflateQpl
{
public:
    explicit CodecDeflateQpl(qpl_path_t path);
    ~CodecDeflateQpl();

    static CodecDeflateQpl & getHardwareInstance();
    static CodecDeflateQpl & getSoftwareInstance();

    qpl_job * acquireJob(UInt32 & job_id);
    void releaseJob(UInt32 job_id);
    const bool & isJobPoolReady() { return job_pool_ready; }
    Int32 doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size);
    Int32 doCompressData(const char * source, UInt32 source_size, char * dest, UInt32 dest_size);

private:
    bool tryLockJob(UInt32 index);
    void unLockJob(UInt32 index);
    /// qpl excute path
    const char * qpl_excute_path;
    /// Maximum jobs running in parallel
    static constexpr auto MAX_JOB_NUMBER = 1024;
    /// Entire buffer for storing all job objects
    char * jobs_buffer;
    /// Job pool for storing all job object pointers
    std::array<qpl_job *, MAX_JOB_NUMBER> job_ptr_pool;
    /// Locks for accessing each job object pointers
    std::array<std::atomic_bool, MAX_JOB_NUMBER> job_ptr_locks;
    bool job_pool_ready;
    std::mt19937 random_engine;
    std::uniform_int_distribution<int> distribution;
};

Int32 QPL_compress(const char * source, int inputSize, char * dest, int maxOutputSize);
Int32 QPL_decompress(const char * source, int inputSize, char * dest, int maxOutputSize);

} //namespace QPL
} //namespace DB

#endif /* USE_QPL */
