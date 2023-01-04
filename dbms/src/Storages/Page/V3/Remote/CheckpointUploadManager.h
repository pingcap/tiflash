#include <Common/nocopyable.h>
#include <Storages/DeltaMerge/Remote/DataStore/DataStore.h>
#include <Storages/Page/UniversalWriteBatch.h>
#include <Storages/Page/V3/BlobStore.h>
#include <Storages/Page/V3/PageDirectory.h>
#include <Storages/Page/V3/Remote/Proto/common.pb.h>
#include <common/types.h>

#include <condition_variable>

namespace Aws::S3
{
class S3Client;
}

namespace DB
{
class Context;
namespace S3
{
struct S3FilenameView;
}
} // namespace DB

namespace DB::PS::V3
{
class CheckpointUploadManager;
using CheckpointUploadManagerPtr = std::unique_ptr<CheckpointUploadManager>;


// TODO: Move it into Page/universal
class CheckpointUploadManager
{
public:
    using PageDirectoryPtr = PS::V3::universal::PageDirectoryPtr;
    using BlobStorePtr = PS::V3::universal::BlobStorePtr;

    static CheckpointUploadManagerPtr createForDebug(UInt64 store_id, PageDirectoryPtr & directory, BlobStorePtr & blob_store);

    void initStoreInfo(UInt64 store_id);

    bool createS3LockForWriteBatch(UniversalWriteBatch & write_batch);

    struct S3LockCreateResult
    {
        String lock_key;
        String err_msg;
        bool ok() const { return !lock_key.empty(); }
    };
    S3LockCreateResult createS3Lock(const S3::S3FilenameView & s3_file, UInt64 lock_store_id);

    void cleanAppliedS3ExternalFiles(std::unordered_set<String> && applied_s3files);

    struct DumpRemoteCheckpointOptions
    {
        /**
         * The directory where temporary files are generated.
         * Files are first generated in the temporary directory, then copied into the remote directory.
         */
        const std::string & temp_directory;

        /**
         * The writer info field in the dumped files.
         */
        const std::shared_ptr<const Remote::WriterInfo> writer_info;

        const ReadLimiterPtr read_limiter = nullptr;
        const WriteLimiterPtr write_limiter = nullptr;
    };

    struct DumpRemoteCheckpointResult
    {
        Strings data_file;
        String manifest_file;
    };

    DumpRemoteCheckpointResult dumpRemoteCheckpoint(DumpRemoteCheckpointOptions options);

    DISALLOW_COPY(CheckpointUploadManager);

private:
    explicit CheckpointUploadManager(PageDirectoryPtr & directory_, BlobStorePtr & blob_store_);

private:
    UInt64 store_id;

    String s3_bucket;
    std::shared_ptr<Aws::S3::S3Client> s3_client;

    PageDirectoryPtr & page_directory;
    BlobStorePtr & blob_store;

    std::mutex mtx_store_init;
    std::condition_variable cv_init;
    std::atomic<bool> inited_from_s3 = false;


    std::mutex mtx_checkpoint;
    UInt64 last_checkpoint_sequence = 0;

    std::shared_mutex mtx_checkpoint_manifest;
    std::atomic<UInt64> last_upload_sequence = 0;
    std::unordered_set<String> pre_locks_files;

    LoggerPtr log;
};

} // namespace DB::PS::V3
