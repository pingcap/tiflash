#pragma once

#include <Encryption/FileProvider.h>
#include <IO/CompressedReadBufferBase.h>
#include <IO/ReadBufferFromFileBase.h>
#include <Storages/DeltaMerge/File/DMConfiguration.h>
#include <time.h>

#include <memory>


namespace DB
{

/// CompressedSeekableReaderBuffer provides an extra abstraction layer to unify compressed buffers
/// This helps to unify CompressedReadBufferFromFileProvider<false> and CompressedReadBufferFromFileProvider<true>
struct CompressedSeekableReaderBuffer : public BufferWithOwnMemory<ReadBuffer>
{
    virtual void setProfileCallback(
        const ReadBufferFromFileBase::ProfileCallback & profile_callback_, clockid_t clock_type_ = CLOCK_MONOTONIC_COARSE)
    = 0;

    virtual void seek(size_t offset_in_compressed_file, size_t offset_in_decompressed_block) = 0;

    CompressedSeekableReaderBuffer() : BufferWithOwnMemory<ReadBuffer>(0) {}
};


/// Unlike CompressedReadBuffer, it can do seek.
template <bool has_checksum = true>
class CompressedReadBufferFromFileProvider
    : public CompressedReadBufferBase<has_checksum>
    , public BufferWithOwnMemory<ReadBuffer>
{
private:
    /** At any time, one of two things is true:
      * a) size_compressed = 0
      * b)
      *  - `working_buffer` contains the entire block.
      *  - `file_in` points to the end of this block.
      *  - `size_compressed` contains the compressed size of this block.
      */
    std::unique_ptr<ReadBufferFromFileBase> p_file_in;
    ReadBufferFromFileBase & file_in;
    size_t size_compressed = 0;

    bool nextImpl() override;

public:
    CompressedReadBufferFromFileProvider(
        FileProviderPtr & file_provider,
        const std::string & path,
        const EncryptionPath & encryption_path,
        size_t estimated_size,
        size_t aio_threshold,
        const ReadLimiterPtr & read_limiter_,
        size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE);

    /// @attention: estimated_size should be at least DBMS_DEFAULT_BUFFER_SIZE if one want to do seeking; however, if one knows that target file
    /// only consists of a single small frame, one can use a smaller estimated_size to reduce memory footprint.
    CompressedReadBufferFromFileProvider(FileProviderPtr & file_provider, const std::string & path, const EncryptionPath & encryption_path,
        size_t estimated_size, const ReadLimiterPtr & read_limiter, const DM::DMConfiguration & configuration);

    void seek(size_t offset_in_compressed_file, size_t offset_in_decompressed_block) override;

    size_t readBig(char * to, size_t n) override;

    void setProfileCallback(
        const ReadBufferFromFileBase::ProfileCallback & profile_callback_,
        clockid_t clock_type_ = CLOCK_MONOTONIC_COARSE) override
    {
        file_in.setProfileCallback(profile_callback_, clock_type_);
    }
};

} // namespace DB
