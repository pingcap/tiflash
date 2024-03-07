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

#include <Common/config.h>

#if USE_QPL
#include <Common/Exception.h>
#include <IO/Compression/CodecDeflateQpl.h>

#include <cassert>
#include <memory>

namespace DB
{
namespace QPL
{
static constexpr Int32 RET_ERROR = -1;

CodecDeflateQpl::CodecDeflateQpl(qpl_path_t path)
    : random_engine(std::random_device()())
    , distribution(0, MAX_JOB_NUMBER - 1)
{
    UInt32 job_size = 0;
    qpl_excute_path = (path == qpl_path_hardware) ? "Hardware" : "Software";
    /// Get size required for saving a single qpl job object
    qpl_get_job_size(path, &job_size);
    /// Allocate entire buffer for storing all job objects
    jobs_buffer = new char[job_size * MAX_JOB_NUMBER];
    /// Initialize pool for storing all job object pointers
    /// Reallocate buffer by shifting address offset for each job object.
    qpl_job * qpl_job_ptr = nullptr;
    for (UInt32 index = 0; index < MAX_JOB_NUMBER; ++index)
    {
        try
        {
            qpl_job_ptr = (qpl_job *)(jobs_buffer + index * job_size);
            auto status = qpl_init_job(path, qpl_job_ptr);
            if (status != QPL_STS_OK)
            {
                job_pool_ready = false;
                delete[] jobs_buffer;
                if (path == qpl_path_hardware)
                {
                    throw Exception(
                        fmt::format(
                            "Initialization of IAA hardware failed: {} will attempt to use software DeflateQpl codec "
                            "instead of hardware DeflateQpl codec.",
                            std::to_string(status)),
                        ErrorCodes::QPL_INIT_JOB_FAILED);
                }
                else
                {
                    throw Exception(
                        fmt::format(
                            "Initialization of software DeflateQpl codec failed: {} QPL compression/decompression "
                            "cannot be enabled.",
                            std::to_string(status)),
                        ErrorCodes::QPL_INIT_JOB_FAILED);
                }
            }
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
            return;
        }
        job_ptr_pool[index] = qpl_job_ptr;
        unLockJob(index);
    }

    job_pool_ready = true;
}

CodecDeflateQpl::~CodecDeflateQpl()
{
    if (isJobPoolReady())
    {
        for (UInt32 i = 0; i < MAX_JOB_NUMBER; ++i)
        {
            if (job_ptr_pool[i])
            {
                while (!tryLockJob(i))
                    ;
                qpl_fini_job(job_ptr_pool[i]);
                unLockJob(i);
                job_ptr_pool[i] = nullptr;
            }
        }

        delete[] jobs_buffer;
        job_pool_ready = false;
    }
}

CodecDeflateQpl & CodecDeflateQpl::getHardwareInstance()
{
    static CodecDeflateQpl hw_codec(qpl_path_hardware);
    return hw_codec;
}

CodecDeflateQpl & CodecDeflateQpl::getSoftwareInstance()
{
    static CodecDeflateQpl sw_codec(qpl_path_software);
    return sw_codec;
}

void CodecDeflateQpl::releaseJob(UInt32 job_id)
{
    if (isJobPoolReady())
        unLockJob(MAX_JOB_NUMBER - job_id);
}

bool CodecDeflateQpl::tryLockJob(UInt32 index)
{
    bool expected = false;
    assert(index < MAX_JOB_NUMBER);
    return job_ptr_locks[index].compare_exchange_strong(expected, true);
}

void CodecDeflateQpl::unLockJob(UInt32 index)
{
    assert(index < MAX_JOB_NUMBER);
    job_ptr_locks[index].store(false);
}

qpl_job * CodecDeflateQpl::acquireJob(UInt32 & job_id)
{
    if (isJobPoolReady())
    {
        UInt32 retry = 0;
        auto index = distribution(random_engine);
        while (!tryLockJob(index))
        {
            index = distribution(random_engine);
            retry++;
            if (retry > MAX_JOB_NUMBER)
            {
                return nullptr;
            }
        }
        job_id = MAX_JOB_NUMBER - index;
        assert(index < MAX_JOB_NUMBER);
        return job_ptr_pool[index];
    }
    else
        return nullptr;
}

Int32 CodecDeflateQpl::doCompressData(const char * source, UInt32 source_size, char * dest, UInt32 dest_size)
{
    UInt32 job_id = 0;
    qpl_job * job_ptr = nullptr;
    UInt32 compressed_size = 0;
    try
    {
        if (!(job_ptr = acquireJob(job_id)))
        {
            throw Exception(
                std::string("DeflateQpl ") + qpl_excute_path
                    + " codec failed.(Details: doCompressData->acquireJob fail, probably job pool exhausted)",
                ErrorCodes::QPL_ACQUIRE_JOB_FAILED);
        }

        job_ptr->op = qpl_op_compress;
        job_ptr->next_in_ptr = reinterpret_cast<uint8_t *>(const_cast<char *>(source));
        job_ptr->next_out_ptr = reinterpret_cast<uint8_t *>(dest);
        job_ptr->available_in = source_size;
        job_ptr->level = qpl_default_level;
        job_ptr->available_out = dest_size;
        job_ptr->flags = QPL_FLAG_FIRST | QPL_FLAG_DYNAMIC_HUFFMAN | QPL_FLAG_LAST;

        qpl_status status = qpl_execute_job(job_ptr);

        if (status == QPL_STS_OK)
        {
            compressed_size = job_ptr->total_out;
            releaseJob(job_id);
            return compressed_size;
        }
        else
        {
            releaseJob(job_id);
            throw Exception(
                std::string("DeflateQpl ") + qpl_excute_path
                    + " codec failed, doCompressData->qpl_execute_job with error code:" + std::to_string(status),
                ErrorCodes::QPL_COMPRESS_DATA_FAILED);
        }
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
        return RET_ERROR;
    }
}

Int32 CodecDeflateQpl::doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size)
{
    UInt32 job_id = 0;
    qpl_job * job_ptr = nullptr;
    UInt32 decompressed_size = 0;
    try
    {
        if (!(job_ptr = acquireJob(job_id)))
        {
            throw Exception(
                std::string("DeflateQpl ") + qpl_excute_path
                    + " codec failed.(Details: doDeCompressData->acquireJob fail, probably job pool exhausted)",
                ErrorCodes::QPL_ACQUIRE_JOB_FAILED);
        }

        job_ptr->op = qpl_op_decompress;
        job_ptr->next_in_ptr = reinterpret_cast<uint8_t *>(const_cast<char *>(source));
        job_ptr->next_out_ptr = reinterpret_cast<uint8_t *>(dest);
        job_ptr->available_in = source_size;
        job_ptr->available_out = uncompressed_size;
        job_ptr->flags = QPL_FLAG_FIRST | QPL_FLAG_LAST;

        qpl_status status = qpl_execute_job(job_ptr);

        if (status == QPL_STS_OK)
        {
            decompressed_size = job_ptr->total_out;
            releaseJob(job_id);
            return decompressed_size;
        }
        else
        {
            releaseJob(job_id);
            throw Exception(
                std::string("DeflateQpl ") + qpl_excute_path
                    + " codec failed, doDeCompressData->qpl_execute_job with error code:" + std::to_string(status),
                ErrorCodes::QPL_DECOMPRESS_DATA_FAILED);
        }
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
        return RET_ERROR;
    }
}

Int32 QPL_compress(const char * source, int inputSize, char * dest, int maxOutputSize)
{
    Int32 res = RET_ERROR;
    if (CodecDeflateQpl::getHardwareInstance().isJobPoolReady())
        res = CodecDeflateQpl::getHardwareInstance().doCompressData(source, inputSize, dest, maxOutputSize);
    if (res == RET_ERROR)
        res = CodecDeflateQpl::getSoftwareInstance().doCompressData(source, inputSize, dest, maxOutputSize);
    return res;
}

Int32 QPL_decompress(const char * source, int inputSize, char * dest, int maxOutputSize)
{
    Int32 res = RET_ERROR;
    if (CodecDeflateQpl::getHardwareInstance().isJobPoolReady())
        res = CodecDeflateQpl::getHardwareInstance().doDecompressData(source, inputSize, dest, maxOutputSize);
    if (res == RET_ERROR)
        res = CodecDeflateQpl::getSoftwareInstance().doDecompressData(source, inputSize, dest, maxOutputSize);
    return res;
}

} //namespace QPL
} //namespace DB
#endif
