#pragma once
#include <Storages/PathPool.h>

#include <string>

namespace DB::tests
{
class MockDiskDelegatorSingle final : public PSDiskDelegator
{
public:
    MockDiskDelegatorSingle(String path_)
        : path(std::move(path_))
    {}

    size_t numPaths() const { return 1; }
    String defaultPath() const { return path; }
    String getPageFilePath(const PageFileIdAndLevel & /*id_lvl*/) const { return path; }
    void removePageFile(const PageFileIdAndLevel & /*id_lvl*/, size_t /*file_size*/, bool /*meta_left*/, bool /*remove_from_default_path*/) {}
    Strings listPaths() const
    {
        Strings paths;
        paths.emplace_back(path);
        return paths;
    }
    String choosePath(const PageFileIdAndLevel & /*id_lvl*/) { return path; }
    size_t addPageFileUsedSize(
        const PageFileIdAndLevel & /*id_lvl*/,
        size_t /*size_to_add*/,
        const String & /*pf_parent_path*/,
        bool /*need_insert_location*/)
    {
        return 0;
    }

private:
    String path;
};

class MockDiskDelegatorMulti final : public PSDiskDelegator
{
public:
    MockDiskDelegatorMulti(Strings paths_)
        : paths(std::move(paths_))
    {
        if (paths.empty())
            throw Exception("Should not generate MockDiskDelegatorMulti with empty paths");
    }

    size_t numPaths() const { return paths.size(); }
    String defaultPath() const { return paths[0]; }
    String getPageFilePath(const PageFileIdAndLevel & /*id_lvl*/) const { throw Exception("Not implemented"); }
    void removePageFile(const PageFileIdAndLevel & /*id_lvl*/, size_t /*file_size*/, bool /*meta_left*/, bool /*remove_from_default_path*/) {}
    Strings listPaths() const { return paths; }
    String choosePath(const PageFileIdAndLevel & /*id_lvl*/)
    {
        const auto chosen = paths[choose_idx];
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

private:
    Strings paths;
    size_t choose_idx = 0;
};

} // namespace DB::tests
