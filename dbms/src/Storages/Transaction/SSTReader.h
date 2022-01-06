#pragma once

#include <Storages/Transaction/ProxyFFI.h>

namespace DB
{
struct SSTReader
{
    bool remained() const;
    BaseBuffView keyView() const;
    BaseBuffView valueView() const;
    void next();

    SSTReader(const SSTReader &) = delete;
    SSTReader(SSTReader &&) = delete;
    SSTReader(const TiFlashRaftProxyHelper * proxy_helper_, SSTView view);
    ~SSTReader();

private:
    const TiFlashRaftProxyHelper * proxy_helper;
    SSTReaderPtr inner;
    ColumnFamilyType type;
};


} // namespace DB
