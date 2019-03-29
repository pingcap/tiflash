#include <Common/StringUtils/StringUtils.h>
#include <Storages/Transaction/RegionPersister.h>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

RegionFile * RegionPersister::getOrCreateCurrentFile()
{
    std::lock_guard<std::mutex> map_lock(region_map_mutex);

    auto it = files.find(CURRENT_REGION_FILE_ID);
    if (it != files.end())
    {
        if (it->second->size() < config.file_size)
            return it->second.get();

        // current file is full
        auto cur_file = it->second;
        cur_file->resetId(++max_file_id);
        files.erase(it);
        files.emplace(cur_file->id(), cur_file);
    }

    bool ok = false;
    std::tie(it, ok) = files.emplace(CURRENT_REGION_FILE_ID, std::make_shared<RegionFile>(CURRENT_REGION_FILE_ID, path));

    return it->second.get();
}

RegionFile * RegionPersister::createGCFile()
{
    std::lock_guard<std::mutex> map_lock(region_map_mutex);

    auto file_id = ++max_file_id;
    auto [it, ok] = files.emplace(file_id, std::make_shared<RegionFile>(file_id, path));
    if (unlikely(!ok))
        throw Exception("Algorithm broken!", ErrorCodes::LOGICAL_ERROR);
    return it->second.get();
}

void RegionPersister::coverOldRegion(RegionFile * file, UInt64 region_id)
{
    for (const auto & p : files)
    {
        if (!file->tryCoverRegion(region_id, *p.second))
            break;
    }
}

void RegionPersister::drop(UInt64 region_id)
{
    std::lock_guard<std::mutex> persist_lock(persist_mutex);
    std::lock_guard<std::mutex> map_lock(region_map_mutex);

    valid_regions.get().erase(region_id);
    valid_regions.persist();

    for (const auto & p : files)
    {
        if (p.second->dropRegion(region_id))
            break;
    }

    region_index_map.erase(region_id);
}

void RegionPersister::persist(const RegionPtr & region, enginepb::CommandResponse * response)
{
    /// Multi threads persist is not yet supported.
    std::lock_guard<std::mutex> persist_lock(persist_mutex);
    size_t persist_parm = region->persistParm();
    doPersist(region, response);
    region->markPersisted();
    region->decPersistParm(persist_parm);
}

void RegionPersister::doPersist(const RegionPtr & region, enginepb::CommandResponse * response)
{
    auto region_id = region->id();
    UInt64 applied_index = region->getIndex();

    auto [it, ok] = region_index_map.emplace(region_id, applied_index);
    if (!ok)
    {
        // if equal, we should still overwrite it.
        if (it->second > applied_index)
        {
            LOG_INFO(log, region->toString() << " have already persisted index: " << it->second);
            return;
        }
    }

    auto & valid_region_set = valid_regions.get();
    if (valid_region_set.find(region_id) == valid_region_set.end())
    {
        valid_region_set.insert(region_id);
        valid_regions.persist();
    }

    RegionFile * cur_file = getOrCreateCurrentFile();
    auto writer = cur_file->createWriter();
    auto region_size = writer.write(region, response);

    {
        std::lock_guard<std::mutex> map_lock(region_map_mutex);

        auto exists = cur_file->addRegion(region_id, region_size);
        // We only cover old regions when region_id NOT exist in current file,
        // because current file is already the latest file. i.e. cover itself.
        if (!exists)
            coverOldRegion(cur_file, region_id);
    }

    it->second = applied_index;
}

/// Old regions are cover by newer regions with the same id.
std::vector<bool> valid_regions_in_file(std::vector<RegionFile::Reader::PersistMeta> & metas)
{
    std::vector<bool> use(metas.size());
    for (size_t index = 0; index < metas.size(); ++index)
    {
        auto region_id = metas[index].region_id;
        use[index] = true;
        // cover older region in this file.
        for (size_t less = 0; less < index; ++less)
        {
            if (region_id == metas[less].region_id)
            {
                use[less] = false;
            }
        }
    }

    return use;
}

void RegionPersister::restore(RegionMap & regions, const Region::RegionClientCreateFunc & region_client_create)
{
    std::lock_guard<std::mutex> persist_lock(persist_mutex);
    std::lock_guard<std::mutex> map_lock(region_map_mutex);

    Poco::File dir(path);
    std::vector<std::string> file_names;
    dir.list(file_names);

    std::vector<UInt64> file_ids;
    for (auto & name : file_names)
    {
        if (!endsWith(name, REGION_INDEX_FILE_SUFFIX))
            continue;
        name.erase(name.size() - REGION_INDEX_FILE_SUFFIX.size(), name.size());
        UInt64 file_id = std::stoull(name);
        file_ids.push_back(file_id);
    }

    // descending order
    std::sort(file_ids.begin(), file_ids.end(), std::greater<>());

    auto & valid_region_set = valid_regions.get();
    for (auto file_id : file_ids)
    {
        auto file = std::make_shared<RegionFile>(file_id, path);
        auto reader = file->createReader();

        auto metas = reader.regionMetas();
        std::vector<bool> use = valid_regions_in_file(metas);
        for (size_t index = 0; index < metas.size(); ++index)
        {
            auto region_id = metas[index].region_id;
            // Filter out those invalid or stale region.
            bool is_valid_region = valid_region_set.find(region_id) != valid_region_set.end();
            bool has_found_in_later_file = regions.find(region_id) != regions.end();
            if (use[index] && (!is_valid_region || has_found_in_later_file))
            {
                use[index] = false;
            }
        }

        reader.checkHash(use);

        UInt64 region_id;
        size_t index = 0;
        while ((region_id = reader.hasNext()) != InvalidRegionID)
        {
            if (use[index])
            {
                regions.emplace(region_id, reader.next(region_client_create));
                file->addRegion(region_id, metas[index].region_size);
            }
            else
            {
                reader.skipNext();
            }
            ++index;
        }

        files.emplace(file_id, file);
        if (file_id != CURRENT_REGION_FILE_ID && file_id > max_file_id)
            max_file_id = file_id;
    }

    for (auto && [_, region] : regions)
    {
        std::ignore = _;
        region_index_map[region->id()] = region->getIndex();
    }

    LOG_INFO(log, "restore " << regions.size() << " regions");
}

bool RegionPersister::gc()
{
    std::unordered_map<RegionFilePtr, std::set<UInt64>> merge_files;
    size_t candidate_total_size = 0;
    size_t migrate_region_count = 0;
    {
        std::lock_guard<std::mutex> map_lock(region_map_mutex);

        for (auto & p : files)
        {
            // Ignore current file
            if (p.first == CURRENT_REGION_FILE_ID)
                continue;

            auto & file = p.second;
            auto use_rate = file->useRate();

            bool is_candidate = use_rate < config.merge_hint_low_used_rate || file->size() < config.file_small_size;
            if (!is_candidate)
                continue;

            candidate_total_size += static_cast<size_t>(file->size() * use_rate);
            auto & migrate_region_ids = merge_files.try_emplace(file).first->second;

            for (auto it : file->regionAndSizeMap())
            {
                migrate_region_ids.insert(it.first);
                ++migrate_region_count;
            }

            if (candidate_total_size >= config.file_max_size)
                break;
        }
    }

    bool should_merge = merge_files.size() >= config.merge_hint_low_used_file_num
        || (merge_files.size() >= 2 && candidate_total_size >= config.merge_hint_low_used_file_total_size);
    if (!should_merge)
        return false;

    LOG_DEBUG(log, "GC decide to merge " << merge_files.size() << " files, containing " << migrate_region_count << " regions");

    if (!merge_files.empty() && migrate_region_count)
    {
        // Create the GC file if needed.
        RegionFile * gc_file = createGCFile();
        RegionFile::Writer gc_file_writer = gc_file->createWriter();

        for (auto & [file, migrate_region_ids] : merge_files)
        {
            auto reader = file->createReader();
            auto metas = reader.regionMetas();
            std::vector<bool> use = valid_regions_in_file(metas);

            reader.checkHash(use);

            UInt64 region_id;
            size_t index = 0;
            while ((region_id = reader.hasNext()) != InvalidRegionID)
            {
                if (use[index] && migrate_region_ids.count(region_id))
                {
                    auto region = reader.next([](pingcap::kv::RegionVerID) -> pingcap::kv::RegionClientPtr { return nullptr; });
                    auto region_size = gc_file_writer.write(region);
                    {
                        std::lock_guard<std::mutex> map_lock(region_map_mutex);

                        auto exists = gc_file->addRegion(region_id, region_size);
                        // We only cover old regions when region_id NOT exist in current file,
                        // because current file is already the latest file. i.e. cover itself.
                        if (!exists)
                            coverOldRegion(gc_file, region_id);
                    }
                }
                else
                {
                    reader.skipNext();
                }
                ++index;
            }
        }
    }

    if (!merge_files.empty())
    {
        std::lock_guard<std::mutex> map_lock(region_map_mutex);

        for (auto & [file, _] : merge_files)
        {
            (void)_;
            files.erase(file->id());
            file->destroy();
        }
    }

    LOG_DEBUG(log, "GC done");
    return true;
}

} // namespace DB
