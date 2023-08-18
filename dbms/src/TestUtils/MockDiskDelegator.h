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
#include <Storages/PathPool.h>

#include <string>

namespace DB
{
namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
}

namespace tests
{
class MockDiskDelegatorSingle final : public PSDiskDelegator
{
public:
    explicit MockDiskDelegatorSingle(String path_)
        : path(std::move(path_))
    {}

    bool fileExist(const PageFileIdAndLevel & /*id_lvl*/) const override { return true; }

    size_t numPaths() const override { return 1; }

    String defaultPath() const override { return path; }

    String getPageFilePath(const PageFileIdAndLevel & /*id_lvl*/) const override { return path; }

    void removePageFile(
        const PageFileIdAndLevel & /*id_lvl*/,
        size_t /*file_size*/,
        bool /*meta_left*/,
        bool /*remove_from_default_path*/) override
    {}

    Strings listPaths() const override { return Strings{path}; }

    String choosePath(const PageFileIdAndLevel & /*id_lvl*/) override { return path; }

    size_t addPageFileUsedSize(
        const PageFileIdAndLevel & /*id_lvl*/,
        size_t /*size_to_add*/,
        const String & /*pf_parent_path*/,
        bool /*need_insert_location*/) override
    {
        return 0;
    }

    void freePageFileUsedSize(
        const PageFileIdAndLevel & /*id_lvl*/,
        size_t /*size_to_free*/,
        const String & /*pf_parent_path*/) override
    {}

private:
    String path;
};

class MockDiskDelegatorMulti final : public PSDiskDelegator
{
public:
    explicit MockDiskDelegatorMulti(Strings paths_)
        : paths(std::move(paths_))
    {
        RUNTIME_CHECK(!paths.empty());
    }

    bool fileExist(const PageFileIdAndLevel & id_lvl) const override { return page_path_map.exist(id_lvl); }


    size_t numPaths() const override { return paths.size(); }

    String defaultPath() const override { return paths[0]; }

    String getPageFilePath(const PageFileIdAndLevel & id_lvl) const override
    {
        auto idx_opt = page_path_map.getIndex(id_lvl);
        RUNTIME_CHECK_MSG(
            idx_opt.has_value(),
            "Can not find path for PageFile [id={}_{}]",
            id_lvl.first,
            id_lvl.second);
        return paths[*idx_opt];
    }

    void removePageFile(
        const PageFileIdAndLevel & /*id_lvl*/,
        size_t /*file_size*/,
        bool /*meta_left*/,
        bool /*remove_from_default_path*/) override
    {}

    Strings listPaths() const override { return paths; }

    String choosePath(const PageFileIdAndLevel & id_lvl) override
    {
        if (auto idx_opt = page_path_map.getIndex(id_lvl); idx_opt.has_value())
        {
            return paths[*idx_opt];
        }
        auto chosen = paths[choose_idx];
        choose_idx = (choose_idx + 1) % paths.size();
        return chosen;
    }

    size_t addPageFileUsedSize(
        const PageFileIdAndLevel & id_lvl,
        size_t /*size_to_add*/,
        const String & pf_parent_path,
        bool need_insert_location) override
    {
        if (need_insert_location)
        {
            UInt32 index = UINT32_MAX;

            for (size_t i = 0; i < paths.size(); i++)
            {
                if (paths[i] == pf_parent_path)
                {
                    index = i;
                    break;
                }
            }

            RUNTIME_CHECK_MSG(index != UINT32_MAX, "Unrecognized path {}", pf_parent_path);
            page_path_map.setIndex(id_lvl, index);
        }
        return 0;
    }

    void freePageFileUsedSize(
        const PageFileIdAndLevel & /*id_lvl*/,
        size_t /*size_to_free*/,
        const String & /*pf_parent_path*/) override
    {}

private:
    Strings paths;
    size_t choose_idx = 0;
    // PageFileID -> path index
    PathPool::PageFilePathMap page_path_map;
};

} // namespace tests
} // namespace DB
