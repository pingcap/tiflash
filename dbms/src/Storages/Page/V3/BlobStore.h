#pragma once

#include <mutex>
#include <Common/Exception.h>
#include <Storages/Page/V3/BlobFile.h>

namespace DB 
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace PS::V3
{
class BlobStore
{
public:

    std::list<BlobFile> getAllBlobFiles();

    write(char * buffer,size_t size)
    {
        // 
    }

#ifndef DBMS_PUBLIC_GTEST
private:
#endif

    class BlobStats2
    {
        public:
            void addStat(UInt64 blob_file_id)
            {
                
            }

            void rmStat(UInt64 blob_file_id)
            {

            }

            void chooseSpaceMap(size_t buf_size)
            {
                // using idle using ...
            }


        #ifndef DBMS_PUBLIC_GTEST
        private:
        #endif
            class BlobStat
            {
                UInt64 sm_max_caps;
                UInt64 sm_min_caps;

                UInt64 sm_total_size;
                UInt64 sm_valid_size;

                std::mutex sm_lock;
            };
        #ifndef DBMS_PUBLIC_GTEST
        private:
        #endif
            std::map<UInt64, std::shared_ptr<BlobStat>> sms;

            std::atomic<ull> total_sm_used;
            std::atomic<ull> total_sm_size;

            // todo
            std::atomic<ull> minimal_file_id;

            std::condition_variable write_mutex_cv;
            std::
    };


    class BlobStats
    {
        public:
        using ull = unsigned long long;
        using atomic_ull = std::atomic<ull>;
        using atomic_ptr_t = std::shared_ptr<atomic_ull>;
        using atomic_map = std::map<UInt64, atomic_ptr_t>;

        BlobStats(size_t max_blob_file_size)
            : limit_space_size(max_blob_file_size)
        {
            
        }

        // TBD : replace it with template
        void addSMMaxCaps(UInt64 blob_file_id)
        {
            std::lock_guard<std::mutex> guard(stats_cm_lock);

            auto sm_max_cpa_it = sm_max_caps.find(blob_file_id);
            if (sm_max_cpa_it != sm_max_caps.end()) 
            {
                throw Exception("Found current blob [blobid=" +  DB::toString(blob_file_id) +"] existed in BlobStats.", 
                    ErrorCodes::LOGICAL_ERROR);
            }

            sm_max_caps.insert({blob_file_id, std::make_shared<atomic_ull>(limit_space_size)});
        }

        void delSMMaxCaps(UInt64 blob_file_id)
        {
            std::lock_guard<std::mutex> guard(stats_cm_lock);

            auto sm_max_cpa_it = sm_max_caps.find(blob_file_id);
            if (sm_max_cpa_it == sm_max_caps.end()) 
            {
                throw Exception("Can't found current Blob [blobid=" +  DB::toString(blob_file_id) +"] in BlobStats.", 
                    ErrorCodes::LOGICAL_ERROR);
            }

            sm_max_caps.erase(sm_max_cpa_it);
        }

        // TBD : replace it with template
        void updateSMMaxCaps(size_t new_value, UInt64 blob_file_id)
        {
            if (new_value == 0)
            {
                return;
            }

            auto sm_max_cpa_it = sm_max_caps.find(blob_file_id);
            if (sm_max_cpa_it == sm_max_caps.end()) 
            {
                throw Exception("Can't found current Blob [blobid=" +  DB::toString(blob_file_id) +"] in BlobStats.", 
                    ErrorCodes::LOGICAL_ERROR);
            }

            auto sm_max_cpa = sm_max_cpa_it->second;
            sm_max_cpa->store(new_value, std::memory_order_seq_cst);
        };

        ull getSMMaxCaps(UInt64 blob_file_id)
        {
            auto sm_max_cpa_it = sm_max_caps.find(blob_file_id);
            if (sm_max_cpa_it == sm_max_caps.end()) 
            {
                throw Exception("Can't found current Blob [blobid=" +  DB::toString(blob_file_id) +"] in BlobStats.", 
                    ErrorCodes::LOGICAL_ERROR);
            }

            auto sm_max_cpa = sm_max_cpa_it->second;
            return sm_max_cpa->load(std::memory_order_seq_cst);
        }

        #ifndef DBMS_PUBLIC_GTEST
        private:
        #endif
            // RO , The max limit size of blobfile.
            size_t limit_space_size;

            atomic_map sm_max_caps;
            atomic_map sm_min_caps;

            atomic_map sm_total_sizes;
            atomic_map sm_valid_sizes;

            std::atomic<ull> total_sm_used;
            std::atomic<ull> total_sm_size;
            std::map<UInt64, std::mutex> sm_locks;

            std::mutex stats_cm_lock;
            
    };

private:

    // TBD: after single path work, do the multi-path
    // String choosePath();
};

} // namespace PS::V3
} // namespace DB