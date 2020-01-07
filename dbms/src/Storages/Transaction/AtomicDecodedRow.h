#pragma once

#include <atomic>

#include <Storages/Transaction/Types.h>

namespace DB
{
class Field;
struct DecodedField;
struct DecodedRow;

template <bool is_key = false>
struct AtomicDecodedRow
{
    ~AtomicDecodedRow();

    const DecodedRow * load() const;

    void atomicUpdate(DecodedRow *& data) const;

    AtomicDecodedRow() = default;

private:
    AtomicDecodedRow(const AtomicDecodedRow &) = delete;
    AtomicDecodedRow(AtomicDecodedRow &&) = delete;

private:
    mutable std::atomic<void *> decoded{nullptr};
};

template <>
struct AtomicDecodedRow<true>
{
};

} // namespace DB
