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

#include <Common/Logger.h>
#include <Common/nocopyable.h>
#include <Core/Types.h>
#include <Storages/Page/PageDefines.h>

#include <mutex>
#include <unordered_map>

namespace DB
{
class PathCapacityMetrics;
using PathCapacityMetricsPtr = std::shared_ptr<PathCapacityMetrics>;
class FileProvider;
using FileProviderPtr = std::shared_ptr<FileProvider>;

/// A class to manage global paths.
class PathPool;
/// A class to manage paths for the specified storage.
class StoragePathPool;

/// ===== Delegators to StoragePathPool ===== ///
/// Delegators to StoragePathPool. Use for managing the path of DTFiles.
class StableDiskDelegator;
/// Delegators to StoragePathPool. Use by PageStorage for managing the path of PageFiles.
class PSDiskDelegator;
using PSDiskDelegatorPtr = std::shared_ptr<PSDiskDelegator>;
class PSDiskDelegatorMulti;
class PSDiskDelegatorSingle;
class PSDiskDelegatorRaft;

/// A class to manage global paths.
class PathPool
{
public:
    PathPool() = default;

    // Constructor to be used during initialization
    PathPool(
        const Strings & main_data_paths,
        const Strings & latest_data_paths,
        const Strings & kvstore_paths,
        PathCapacityMetricsPtr global_capacity_,
        FileProviderPtr file_provider_,
        bool enable_raft_compatible_mode_ = false);

    // Constructor to create PathPool for one Storage
    StoragePathPool withTable(const String & database_, const String & table_, bool path_need_database_name_) const;

    // TODO: remove this outdated code
    bool isRaftCompatibleModeEnabled() const { return enable_raft_compatible_mode; }

    // Generate a delegator for managing the paths of `RegionPersister`.
    // Those paths are generated from `kvstore_paths`.
    // User should keep the pointer to track the PageFileID -> path index mapping.
    PSDiskDelegatorPtr getPSDiskDelegatorRaft();

    PSDiskDelegatorPtr getPSDiskDelegatorGlobalMulti(const String & prefix) const;
    PSDiskDelegatorPtr getPSDiskDelegatorGlobalSingle(const String & prefix) const;

public:
    /// Methods for the root PathPool ///
    Strings listPaths() const;

    const Strings & listKVStorePaths() const { return kvstore_paths; }

    const Strings & listGlobalPagePaths() const { return global_page_paths; }

public:
    // A thread safe wrapper for storing a map of <page data file id, path index>
    class PageFilePathMap
    {
    public:
        inline bool exist(const PageFileIdAndLevel & id_lvl) const
        {
            std::lock_guard guard(mtx);
            return page_id_to_index.count(id_lvl) > 0;
        }
        inline std::optional<UInt32> getIndex(const PageFileIdAndLevel & id_lvl) const
        {
            std::lock_guard guard(mtx);
            if (auto iter = page_id_to_index.find(id_lvl); iter != page_id_to_index.end())
                return iter->second;
            return std::nullopt;
        }
        inline void setIndex(const PageFileIdAndLevel & id_lvl, UInt32 index)
        {
            std::lock_guard gurad(mtx);
            page_id_to_index[id_lvl] = index;
        }
        inline void eraseIfExist(const PageFileIdAndLevel & id_lvl)
        {
            std::lock_guard gurad(mtx);
            if (auto iter = page_id_to_index.find(id_lvl);
                iter != page_id_to_index.end())
                page_id_to_index.erase(iter);
        }

    private:
        mutable std::mutex mtx;
        struct PageFileIdLvlHasher
        {
            std::size_t operator()(const PageFileIdAndLevel & id_lvl) const
            {
                return std::hash<PageFileId>()(id_lvl.first) ^ std::hash<PageFileLevel>()(id_lvl.second);
            }
        };
        std::unordered_map<PageFileIdAndLevel, UInt32, PageFileIdLvlHasher> page_id_to_index;
    };

    friend class PSDiskDelegatorRaft;
    friend class PSDiskDelegatorGlobalSingle;
    friend class PSDiskDelegatorGlobalMulti;

private:
    Strings main_data_paths;
    Strings latest_data_paths;
    Strings kvstore_paths;
    Strings global_page_paths;

    bool enable_raft_compatible_mode;

    PathCapacityMetricsPtr global_capacity;

    FileProviderPtr file_provider;

    LoggerPtr log;
};

class StableDiskDelegator
{
public:
    explicit StableDiskDelegator(StoragePathPool & pool_)
        : pool(pool_)
    {}

    Strings listPaths() const;

    String choosePath() const;

    // Get the path of the DTFile with file_id.
    // If throw_on_not_exist is false, return empty string when the path is not exists.
    String getDTFilePath(UInt64 file_id, bool throw_on_not_exist = true) const;

    void addDTFile(UInt64 file_id, size_t file_size, std::string_view path);

    void removeDTFile(UInt64 file_id);

    DISALLOW_COPY_AND_MOVE(StableDiskDelegator);

private:
    StoragePathPool & pool;
};

// TODO: the `freePageFileUsedSize` and `removePageFile`
// is not well design interface. We need refactor related
// methods later.
class PSDiskDelegator
{
public:
    PSDiskDelegator() = default;

    virtual ~PSDiskDelegator() = default;

    virtual bool fileExist(const PageFileIdAndLevel & id_lvl) const = 0;

    virtual size_t numPaths() const = 0;

    virtual String defaultPath() const = 0;

    virtual Strings listPaths() const = 0;

    virtual String choosePath(const PageFileIdAndLevel & id_lvl) = 0;

    virtual size_t addPageFileUsedSize(
        const PageFileIdAndLevel & id_lvl,
        size_t size_to_add,
        const String & pf_parent_path,
        bool need_insert_location)
        = 0;

    virtual void freePageFileUsedSize(
        const PageFileIdAndLevel & id_lvl,
        size_t size_to_free,
        const String & pf_parent_path)
        = 0;

    virtual String getPageFilePath(const PageFileIdAndLevel & id_lvl) const = 0;

    virtual void removePageFile(const PageFileIdAndLevel & id_lvl, size_t file_size, bool meta_left, bool remove_from_default_path) = 0;

    DISALLOW_COPY_AND_MOVE(PSDiskDelegator);
};

class PSDiskDelegatorMulti : public PSDiskDelegator
{
public:
    PSDiskDelegatorMulti(StoragePathPool & pool_, String prefix)
        : pool(pool_)
        , path_prefix(std::move(prefix))
    {}

    bool fileExist(const PageFileIdAndLevel & id_lvl) const override;

    size_t numPaths() const override;

    String defaultPath() const override;

    Strings listPaths() const override;

    String choosePath(const PageFileIdAndLevel & id_lvl) override;

    size_t addPageFileUsedSize(
        const PageFileIdAndLevel & id_lvl,
        size_t size_to_add,
        const String & pf_parent_path,
        bool need_insert_location) override;

    void freePageFileUsedSize(
        const PageFileIdAndLevel & id_lvl,
        size_t size_to_free,
        const String & pf_parent_path) override;

    String getPageFilePath(const PageFileIdAndLevel & id_lvl) const override;

    void removePageFile(const PageFileIdAndLevel & id_lvl, size_t file_size, bool meta_left, bool remove_from_default_path) override;

private:
    StoragePathPool & pool;
    const String path_prefix;
    // PageFileID -> path index
    PathPool::PageFilePathMap page_path_map;
    const UInt32 default_path_index = 0;
};

class PSDiskDelegatorSingle : public PSDiskDelegator
{
public:
    PSDiskDelegatorSingle(StoragePathPool & pool_, String prefix)
        : pool(pool_)
        , path_prefix(std::move(prefix))
    {}

    bool fileExist(const PageFileIdAndLevel & id_lvl) const override;

    size_t numPaths() const override;

    String defaultPath() const override;

    Strings listPaths() const override;

    String choosePath(const PageFileIdAndLevel & id_lvl) override;

    size_t addPageFileUsedSize(
        const PageFileIdAndLevel & id_lvl,
        size_t size_to_add,
        const String & pf_parent_path,
        bool need_insert_location) override;

    void freePageFileUsedSize(
        const PageFileIdAndLevel & id_lvl,
        size_t size_to_free,
        const String & pf_parent_path) override;

    String getPageFilePath(const PageFileIdAndLevel & id_lvl) const override;

    void removePageFile(const PageFileIdAndLevel & id_lvl, size_t file_size, bool meta_left, bool remove_from_default_path) override;

private:
    StoragePathPool & pool;
    const String path_prefix;
};

class PSDiskDelegatorRaft : public PSDiskDelegator
{
public:
    explicit PSDiskDelegatorRaft(PathPool & pool_);

    bool fileExist(const PageFileIdAndLevel & id_lvl) const override;

    size_t numPaths() const override;

    String defaultPath() const override;

    Strings listPaths() const override;

    String choosePath(const PageFileIdAndLevel & id_lvl) override;

    size_t addPageFileUsedSize(
        const PageFileIdAndLevel & id_lvl,
        size_t size_to_add,
        const String & pf_parent_path,
        bool need_insert_location) override;

    void freePageFileUsedSize(
        const PageFileIdAndLevel & id_lvl,
        size_t size_to_free,
        const String & pf_parent_path) override;

    String getPageFilePath(const PageFileIdAndLevel & id_lvl) const override;

    void removePageFile(const PageFileIdAndLevel & id_lvl, size_t file_size, bool meta_left, bool remove_from_default_path) override;

private:
    struct RaftPathInfo
    {
        String path;
    };
    using RaftPathInfos = std::vector<RaftPathInfo>;

    PathPool & pool;
    RaftPathInfos raft_path_infos;
    // PageFileID -> path index
    PathPool::PageFilePathMap page_path_map;
    const UInt32 default_path_index = 0;
};

class PSDiskDelegatorGlobalMulti : public PSDiskDelegator
{
public:
    PSDiskDelegatorGlobalMulti(const PathPool & pool_, String prefix)
        : pool(pool_)
        , path_prefix(std::move(prefix))
    {}

    bool fileExist(const PageFileIdAndLevel & id_lvl) const override;

    size_t numPaths() const override;

    String defaultPath() const override;

    Strings listPaths() const override;

    String choosePath(const PageFileIdAndLevel & id_lvl) override;

    size_t addPageFileUsedSize(
        const PageFileIdAndLevel & id_lvl,
        size_t size_to_add,
        const String & pf_parent_path,
        bool need_insert_location) override;

    void freePageFileUsedSize(
        const PageFileIdAndLevel & id_lvl,
        size_t size_to_free,
        const String & pf_parent_path) override;

    String getPageFilePath(const PageFileIdAndLevel & id_lvl) const override;

    void removePageFile(const PageFileIdAndLevel & id_lvl, size_t file_size, bool meta_left, bool remove_from_default_path) override;

private:
    const PathPool & pool;
    const String path_prefix;
    // PageFileID -> path index
    PathPool::PageFilePathMap page_path_map;
    const UInt32 default_path_index = 0;
};

class PSDiskDelegatorGlobalSingle : public PSDiskDelegator
{
public:
    PSDiskDelegatorGlobalSingle(const PathPool & pool_, String prefix)
        : pool(pool_)
        , path_prefix(std::move(prefix))
    {}

    bool fileExist(const PageFileIdAndLevel & id_lvl) const override;

    size_t numPaths() const override;

    String defaultPath() const override;

    Strings listPaths() const override;

    String choosePath(const PageFileIdAndLevel & id_lvl) override;

    size_t addPageFileUsedSize(
        const PageFileIdAndLevel & id_lvl,
        size_t size_to_add,
        const String & pf_parent_path,
        bool need_insert_location) override;

    void freePageFileUsedSize(
        const PageFileIdAndLevel & id_lvl,
        size_t size_to_free,
        const String & pf_parent_path) override;

    String getPageFilePath(const PageFileIdAndLevel & id_lvl) const override;

    void removePageFile(const PageFileIdAndLevel & id_lvl, size_t file_size, bool meta_left, bool remove_from_default_path) override;

private:
    const PathPool & pool;
    const String path_prefix;

    PathPool::PageFilePathMap page_path_map;
};

/// A class to manage paths for a specified physical table.
class StoragePathPool
{
public:
    static constexpr const char * STABLE_FOLDER_NAME = "stable";

    StoragePathPool(const Strings & main_data_paths,
                    const Strings & latest_data_paths,
                    String database_,
                    String table_,
                    bool path_need_database_name_,
                    PathCapacityMetricsPtr global_capacity_,
                    FileProviderPtr file_provider_);

    // Generate a lightweight delegator for managing stable data, such as choosing path for DTFile or getting DTFile path by ID and so on.
    // Those paths are generated from `main_path_infos` and `STABLE_FOLDER_NAME`
    StableDiskDelegator getStableDiskDelegator() { return StableDiskDelegator(*this); }

    // Generate a delegator for managing the paths of `StoragePool`.
    // Those paths are generated from `latest_path_infos` and `prefix`.
    // User should keep the pointer to track the PageFileID -> path index mapping.
    PSDiskDelegatorPtr getPSDiskDelegatorMulti(const String & prefix) { return std::make_shared<PSDiskDelegatorMulti>(*this, prefix); }

    // Generate a delegator for managing the paths of `StoragePool`.
    // Those paths are generated from the first path of `latest_path_infos` and `prefix`
    PSDiskDelegatorPtr getPSDiskDelegatorSingle(const String & prefix) { return std::make_shared<PSDiskDelegatorSingle>(*this, prefix); }

    bool createPSV2DeleteMarkFile();

    bool isPSV2Deleted() const;

    void clearPSV2ObsoleteData();

    void rename(const String & new_database, const String & new_table);

    void drop(bool recursive, bool must_success = true);

    void shutdown() { shutdown_called.store(true); }

    bool isShutdown() const { return shutdown_called.load(); }

    DISALLOW_COPY(StoragePathPool);

    StoragePathPool(StoragePathPool && rhs) noexcept;
    StoragePathPool & operator=(StoragePathPool && rhs);

private:
    String getStorePath(const String & extra_path_root, const String & database_name, const String & table_name) const;

    void renamePath(const String & old_path, const String & new_path);

    String getPSV2DeleteMarkFilePath() const;

private:
    using DMFilePathMap = std::unordered_map<UInt64, UInt32>;
    struct MainPathInfo
    {
        String path;
        // DMFileID -> file size
        std::unordered_map<UInt64, size_t> file_size_map;
    };
    using MainPathInfos = std::vector<MainPathInfo>;
    struct LatestPathInfo
    {
        String path;
    };
    using LatestPathInfos = std::vector<LatestPathInfo>;

    friend class StableDiskDelegator;
    friend class PSDiskDelegatorMulti;
    friend class PSDiskDelegatorSingle;

private:
    // Note that we keep an assumption that the size of `main_path_infos` and `latest_path_infos`
    // won't be changed during the whole runtime.
    // Path, size
    MainPathInfos main_path_infos;
    LatestPathInfos latest_path_infos;

    String database;
    String table;

    // This mutex mainly used to protect the `dt_file_path_map`
    mutable std::mutex mutex;
    // DMFileID -> path index
    DMFilePathMap dt_file_path_map;

    bool path_need_database_name = false;

    std::atomic<bool> shutdown_called;

    PathCapacityMetricsPtr global_capacity;

    FileProviderPtr file_provider;

    LoggerPtr log;
};

} // namespace DB
