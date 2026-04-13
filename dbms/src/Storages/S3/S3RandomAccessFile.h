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

#include <Common/Exception.h>
#include <Common/Logger.h>
#include <Common/Stopwatch.h>
#include <IO/BaseFile/RandomAccessFile.h>
#include <Storages/DeltaMerge/ScanContext_fwd.h>
#include <Storages/S3/S3ReadLimiter_fwd.h>
#include <aws/s3/model/GetObjectResult.h>
#include <common/types.h>

#include <ext/scope_guard.h>
#include <istream>

/// Remove the population of thread_local from Poco
#ifdef thread_local
#undef thread_local
#endif

namespace DB::S3
{
class TiFlashS3Client;
} // namespace DB::S3

namespace DB::ErrorCodes
{
extern const int NOT_IMPLEMENTED;
}

namespace DB::S3
{
class S3RandomAccessFile final : public RandomAccessFile
{
public:
    /// Create a random-access S3 file reader for the remote object key.
    static RandomAccessFilePtr create(const String & remote_fname);

    S3RandomAccessFile(
        std::shared_ptr<TiFlashS3Client> client_ptr_,
        const String & remote_fname_,
        const DM::ScanContextPtr & scan_context_);

    ~S3RandomAccessFile() override;

    /// Seek to `offset` with `SEEK_SET`.
    /// Forward seeks may reuse the current stream or reopen it depending on the implementation path.
    [[nodiscard]] off_t seek(off_t offset, int whence) override;

    /// Read up to `size` bytes from the current offset and advance on success.
    [[nodiscard]] ssize_t read(char * buf, size_t size) override;

    /// Return the fully qualified remote path as "{bucket}/{remote_fname}".
    std::string getFileName() const override;

    /// Return the object key without the bucket prefix.
    std::string getInitialFileName() const override;

    [[nodiscard]] ssize_t pread(char * /*buf*/, size_t /*size*/, off_t /*offset*/) const override
    {
        throw Exception("S3RandomAccessFile not support pread", ErrorCodes::NOT_IMPLEMENTED);
    }

    int getFd() const override { return -1; }

    bool isClosed() const override { return is_close; }

    void close() override { is_close = true; }

    struct ReadFileInfo
    {
        UInt64 size = 0; // File size of `remote_fname` or `merged_filename`, mainly used for FileCache.
        DB::DM::ScanContextPtr scan_context;
    };

    [[nodiscard]] static auto setReadFileInfo(ReadFileInfo && read_file_info_)
    {
        read_file_info = std::move(read_file_info_);
        return ext::make_scope_guard([]() { read_file_info.reset(); });
    }

    /// Return a short diagnostic string for logging and tests.
    String summary() const;

private:
    /// Open or reopen the body stream for the current `cur_offset`.
    void initialize(std::string_view action);
    /// Reopen the object stream from `target_offset` and reset per-initialize retry state.
    void reopenAt(off_t target_offset, std::string_view action);
    off_t seekImpl(off_t offset, int whence);
    ssize_t readImpl(char * buf, size_t size);
    String readRangeOfObject();
    ssize_t readChunked(char * buf, size_t size);
    ssize_t finalizeRead(size_t requested_size, size_t actual_size, const Stopwatch & sw, std::istream & istr);
    off_t finalizeSeek(
        off_t target_offset,
        size_t requested_size,
        size_t actual_size,
        const Stopwatch & sw,
        std::istream & istr);
    off_t seekChunked(off_t offset);

    // When reading, it is necessary to pass the extra information of file, such file size, to S3RandomAccessFile::create.
    // It is troublesome to pass parameters layer by layer. So currently, use thread_local global variable to pass parameters.
    // TODO: refine these codes later.
    inline static thread_local std::optional<ReadFileInfo> read_file_info;

    std::shared_ptr<TiFlashS3Client> client_ptr;
    String remote_fname;

    off_t cur_offset;
    Aws::S3::Model::GetObjectResult read_result;
    Int64 content_length = 0;
    std::shared_ptr<S3ReadLimiter> read_limiter;
    std::shared_ptr<S3ReadMetricsRecorder> read_metrics_recorder;

    DB::LoggerPtr log;
    bool is_close = false;

    /// Count GetObject failures within the current initialize session only.
    Int32 cur_retry = 0;
    static constexpr Int32 max_retry = 3;
    DM::ScanContextPtr scan_context;
};

using S3RandomAccessFilePtr = std::shared_ptr<S3RandomAccessFile>;

} // namespace DB::S3
