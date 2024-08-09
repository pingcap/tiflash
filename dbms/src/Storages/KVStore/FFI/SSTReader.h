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
#include <Storages/KVStore/FFI/ProxyFFI.h>
#include <Storages/KVStore/MultiRaft/RegionState.h>

namespace DB
{
class SSTReader
{
public:
    using RegionRangeFilter = ImutRegionRangePtr;
    virtual bool remained() const = 0;
    // For a key like `zk1`, it will return `k1`.
    virtual BaseBuffView keyView() const = 0;
    virtual BaseBuffView valueView() const = 0;
    virtual void next() = 0;
    virtual size_t approxSize() const = 0;
    virtual std::vector<std::string> findSplitKeys(uint64_t splits_count) const = 0;
    virtual void seek(BaseBuffView && view) const = 0;
    virtual void seekToFirst() const = 0;
    virtual void seekToLast() const = 0;

    virtual ~SSTReader() = default;
};


class MonoSSTReader : public SSTReader
{
public:
    bool remained() const override;
    BaseBuffView keyView() const override;
    BaseBuffView valueView() const override;
    void next() override;
    SSTFormatKind sstFormatKind() const { return kind; }
    size_t approxSize() const override;
    std::vector<std::string> findSplitKeys(uint64_t splits_count) const override;
    void seek(BaseBuffView && view) const override;
    void seekToFirst() const override;
    void seekToLast() const override;

    DISALLOW_COPY_AND_MOVE(MonoSSTReader);
    MonoSSTReader(
        const TiFlashRaftProxyHelper * proxy_helper_,
        SSTView view,
        RegionRangeFilter range_,
        const LoggerPtr & log_);
    ~MonoSSTReader() override;

private:
    const TiFlashRaftProxyHelper * proxy_helper;
    SSTReaderPtr inner;
    ColumnFamilyType type;
    RegionRangeFilter range;
    SSTFormatKind kind;
    mutable bool tail_checked;
    LoggerPtr log;
};

/// MultiSSTReader helps when there are multiple sst files in a column family.
/// It is derived from virtual class SSTReader, so it can be holded in a SSTReaderPtr.
/// It also maintains instance of `R` which is normaly SSTReader(and MockSSTReader in tests),
/// to read a single sst file.
/// An `Initer` function is need to create a instance of `R` from a instance of `E`,
/// which is usually path of the SST file.
/// We introduce `R` and `E` to make this class testable.
template <typename R, typename E>
class MultiSSTReader : public SSTReader
{
public:
    using Initer
        = std::function<std::unique_ptr<R>(const TiFlashRaftProxyHelper *, E, RegionRangeFilter, const LoggerPtr &)>;

    DISALLOW_COPY_AND_MOVE(MultiSSTReader);

    // Whether the current key is valid.
    bool remained() const override { return mono->remained(); }
    BaseBuffView keyView() const override { return mono->keyView(); }
    BaseBuffView valueView() const override { return mono->valueView(); }
    void next() override
    {
        mono->next();
        // If there are no remained keys, we try to switch to next mono reader.
        this->maybeNextReader();
    }
    size_t approxSize() const override
    {
        if (args.size() > 1)
        {
            // There is no such case for now, we prefer to throw a exception here rather than tolerating overhead silently.
            throw Exception(ErrorCodes::LOGICAL_ERROR, "MultiSSTReader don't support approxSize for multiple ssts");
        }
        return mono->approxSize();
    }
    std::vector<std::string> findSplitKeys(uint64_t splits_count) const override
    {
        if (type != ColumnFamilyType::Write)
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "findSplitKeys can only be called on write cf");
        }
        if (args.size() > 1)
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "MultiSSTReader don't support findSplitKeys for multiple ssts");
        }
        return mono->findSplitKeys(splits_count);
    }
    void seek(BaseBuffView && view) const override
    {
        if (args.size() > 1)
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "MultiSSTReader don't support seek for multiple ssts");
        }
        return mono->seek(std::move(view));
    }
    void seekToFirst() const override
    {
        if (args.size() > 1)
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "MultiSSTReader don't support seek for multiple ssts");
        }
        return mono->seekToFirst();
    }
    void seekToLast() const override
    {
        if (args.size() > 1)
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "MultiSSTReader don't support seek for multiple ssts");
        }
        return mono->seekToLast();
    }

    // Switch to next mono reader if current SST is drained,
    // and we have a next sst file to read.
    void maybeNextReader()
    {
        if (likely(mono->remained()))
            return;

        sst_idx++;
        if (sst_idx < args.size())
        {
            // We don't drop if mono is the last instance for safety,
            // and it will be dropped as MultiSSTReader is dropped.
            LOG_INFO(
                log,
                "Open sst file {}, range={} sst_idx={} sst_tot={}",
                buffToStrView(args[sst_idx].path),
                range->toDebugString(),
                sst_idx,
                args.size());
            mono = initer(proxy_helper, args[sst_idx], range, log);
        }
    }

    MultiSSTReader(
        const TiFlashRaftProxyHelper * proxy_helper_,
        ColumnFamilyType type_,
        Initer initer_,
        std::vector<E> args_,
        LoggerPtr log_,
        RegionRangeFilter range_)
        : log(log_)
        , proxy_helper(proxy_helper_)
        , type(type_)
        , initer(initer_)
        , args(args_)
        , sst_idx(0)
        , range(range_)
    {
        assert(args.size() > 0);
        LOG_INFO(
            log,
            "Open sst file first {}, range={} sst_tot={}",
            buffToStrView(args[sst_idx].path),
            range->toDebugString(),
            args.size());
        mono = initer(proxy_helper, args[sst_idx], range, log);
    }

    ~MultiSSTReader() override
    {
        // The last sst reader will be dropped with inner.
    }

private:
    LoggerPtr log;
    /// Safety: `mono` is always valid during lifetime.
    /// The instance is ill-formed if the size of `args` is zero.
    mutable std::unique_ptr<R> mono;
    const TiFlashRaftProxyHelper * proxy_helper;
    const ColumnFamilyType type;
    Initer initer;
    std::vector<E> args;
    size_t sst_idx;
    RegionRangeFilter range;
};

} // namespace DB
