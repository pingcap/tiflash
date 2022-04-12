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

    bool fileExist(const PageFileIdAndLevel & /*id_lvl*/) const
    {
        return true;
    }

    size_t numPaths() const
    {
        return 1;
    }

    String defaultPath() const
    {
        return path;
    }

    String getPageFilePath(const PageFileIdAndLevel & /*id_lvl*/) const
    {
        return path;
    }

    void removePageFile(const PageFileIdAndLevel & /*id_lvl*/, size_t /*file_size*/, bool /*meta_left*/, bool /*remove_from_default_path*/) {}

    Strings listPaths() const
    {
        Strings paths;
        paths.emplace_back(path);
        return paths;
    }

    String choosePath(const PageFileIdAndLevel & /*id_lvl*/)
    {
        return path;
    }

    size_t addPageFileUsedSize(
        const PageFileIdAndLevel & /*id_lvl*/,
        size_t /*size_to_add*/,
        const String & /*pf_parent_path*/,
        bool /*need_insert_location*/)
    {
        return 0;
    }

    size_t freePageFileUsedSize(
        const PageFileIdAndLevel & /*id_lvl*/,
        size_t /*size_to_free*/,
        const String & /*pf_parent_path*/)
    {
        return 0;
    }

private:
    String path;
};

class MockDiskDelegatorMulti final : public PSDiskDelegator
{
public:
    explicit MockDiskDelegatorMulti(Strings paths_)
        : paths(std::move(paths_))
    {
        if (paths.empty())
            throw Exception("Should not generate MockDiskDelegatorMulti with empty paths");
    }

    bool fileExist(const PageFileIdAndLevel & /*id_lvl*/) const
    {
        return true;
    }


    size_t numPaths() const
    {
        return paths.size();
    }

    String defaultPath() const
    {
        return paths[0];
    }

    String getPageFilePath(const PageFileIdAndLevel & /*id_lvl*/) const
    {
        throw Exception("Not implemented", ErrorCodes::NOT_IMPLEMENTED);
    }

    void removePageFile(const PageFileIdAndLevel & /*id_lvl*/, size_t /*file_size*/, bool /*meta_left*/, bool /*remove_from_default_path*/) {}

    Strings listPaths() const
    {
        return paths;
    }

    String choosePath(const PageFileIdAndLevel & /*id_lvl*/)
    {
        auto chosen = paths[choose_idx];
        choose_idx = (choose_idx + 1) % paths.size();
        return chosen;
    }

    size_t addPageFileUsedSize(
        const PageFileIdAndLevel & /*id_lvl*/,
        size_t /*size_to_add*/,
        const String & /*pf_parent_path*/,
        bool /*need_insert_location*/)
    {
        return 0;
    }

    size_t freePageFileUsedSize(
        const PageFileIdAndLevel & /*id_lvl*/,
        size_t /*size_to_free*/,
        const String & /*pf_parent_path*/)
    {
        return 0;
    }

private:
    Strings paths;
    size_t choose_idx = 0;
};

} // namespace tests
} // namespace DB
