#pragma once

#include <Storages/Transaction/Types.h>

namespace DB
{
class Field;
struct DecodedRowElement;
struct DecodedRowBySchema;

template <bool is_key = false>
struct ValueExtraInfo
{
    ~ValueExtraInfo();

    const DecodedRowBySchema * load() const;

    void atomicUpdate(DecodedRowBySchema *& data) const;

    ValueExtraInfo() = default;

private:
    ValueExtraInfo(const ValueExtraInfo &) = delete;
    ValueExtraInfo(ValueExtraInfo &&) = delete;

private:
    mutable std::atomic<void *> decoded{nullptr};
};

template <>
struct ValueExtraInfo<true>
{
};

} // namespace DB
