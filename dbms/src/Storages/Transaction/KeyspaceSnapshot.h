#pragma once

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#pragma GCC diagnostic ignored "-Wnon-virtual-dtor"
#ifdef __clang__
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
#endif
#include <pingcap/kv/Snapshot.h>
#pragma GCC diagnostic pop

#include <Storages/Transaction/TiKVKeyValue.h>
#include <Storages/Transaction/Types.h>
#include <pingcap/kv/Scanner.h>

namespace DB
{
struct KeyspaceScanner : public pingcap::kv::Scanner
{
    using Base = pingcap::kv::Scanner;

    KeyspaceScanner(Base scanner_, bool need_cut_)
        : Base(scanner_)
        , need_cut(need_cut_)
    {
    }

    std::string key();

private:
    bool need_cut;
};

class KeyspaceSnapshot
{
public:
    using Base = pingcap::kv::Snapshot;
    explicit KeyspaceSnapshot(KeyspaceID keyspace_id_, pingcap::kv::Cluster * cluster_, UInt64 version_);

    std::string Get(const std::string & key);
    std::string Get(pingcap::kv::Backoffer & bo, const std::string & key);
    KeyspaceScanner Scan(const std::string & begin, const std::string & end);

private:
    Base snap;
    std::string prefix;
    std::string encodeKey(const std::string & key);
};
} // namespace DB