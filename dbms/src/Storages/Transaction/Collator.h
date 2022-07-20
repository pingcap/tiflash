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

#include <Columns/Collator.h>
#include <Storages/Transaction/CollatorUtils.h>

namespace TiDB
{

struct TiDBCollatorPtr;
using TiDBCollators = std::vector<TiDBCollatorPtr>;

struct TiDBCollatorID
{
    enum
    {
        UTF8_GENERAL_CI = 33,
        UTF8MB4_GENERAL_CI = 45,
        UTF8_UNICODE_CI = 192,
        UTF8MB4_UNICODE_CI = 224,
        UTF8MB4_BIN = 46,
        LATIN1_BIN = 47,
        BINARY = 63,
        ASCII_BIN = 65,
        UTF8_BIN = 83,
    };

    int32_t getCollatorId() const { return collator_id; }
    bool isBinary() const { return collator_id == BINARY; }
    bool isCI() const
    {
        return collator_id == UTF8_UNICODE_CI || collator_id == UTF8_GENERAL_CI
            || collator_id == UTF8MB4_UNICODE_CI || collator_id == UTF8MB4_GENERAL_CI;
    }
    bool isBin() const
    {
        return collator_id == UTF8_BIN || collator_id == UTF8MB4_BIN
            || collator_id == ASCII_BIN || collator_id == LATIN1_BIN;
    }

    explicit TiDBCollatorID(int32_t id)
        : collator_id(id)
    {}

    int32_t collator_id;
};

class ITiDBCollator : public TiDBCollatorID
    , public ICollator
{
public:
    /// Get the collator according to the internal collation ID, which directly comes from tipb and has been properly
    /// de-rewritten - the "New CI Collation" will flip the sign of the collation ID.
    static TiDBCollatorPtr getCollator(int32_t id);

    /// Get the collator according to collator name
    static TiDBCollatorPtr getCollator(const std::string & name);

    class IPattern
    {
    public:
        virtual ~IPattern() = default;

        virtual void compile(const std::string & pattern, char escape) = 0;
        virtual bool match(const char * s, size_t length) const = 0;

    protected:
        IPattern() = default;
    };

    ~ITiDBCollator() override = default;

    /**
        compare with collation
        0   equal
        <0  {s1, length1} < {s2, length2} with collation
        >0  {s1, length1} > {s2, length2} with collation
    */
    int compare(const char * s1, size_t length1, const char * s2, size_t length2) const override = 0;

    virtual StringRef sortKey(const char * s, size_t length, std::string & container) const = 0;
    virtual std::unique_ptr<IPattern> pattern() const = 0;

    int32_t getCollatorId() const { return collator_id; }
    bool isBinary() const { return collator_id == BINARY; }
    bool isCI() const
    {
        return collator_id == UTF8_UNICODE_CI || collator_id == UTF8_GENERAL_CI
            || collator_id == UTF8MB4_UNICODE_CI || collator_id == UTF8MB4_GENERAL_CI;
    }
    bool isBin() const
    {
        return collator_id == UTF8_BIN || collator_id == UTF8MB4_BIN
            || collator_id == ASCII_BIN || collator_id == LATIN1_BIN;
    }

protected:
    explicit ITiDBCollator(int32_t collator_id_)
        : TiDBCollatorID(collator_id_)
    {}
};

/// these dummy_xxx are used as the default value to avoid too many meaningless
/// modification on the legacy ClickHouse code
extern TiDBCollators dummy_collators;
extern std::vector<std::string> dummy_sort_key_contaners;
extern std::string dummy_sort_key_contaner;

struct TiDBCollatorPtrImpl : TiDBCollatorID
{
    class ITiDBCollator const * ptr{};

    TiDBCollatorPtrImpl(class ITiDBCollator const * ptr_, int32_t collator_id_)
        : TiDBCollatorID(collator_id_)
        , ptr(ptr_)
    {
    }

    explicit TiDBCollatorPtrImpl(class ITiDBCollator const * ptr_)
        : TiDBCollatorID(ptr_ ? ptr_->getCollatorId() : 0)
        , ptr(ptr_)
    {
    }

    ALWAYS_INLINE inline int compare(const char * s1, size_t length1, const char * s2, size_t length2) const
    {
        if (likely(canUseFastPath()))
        {
            return DB::BinCollatorCompare<true>(s1, length1, s2, length2);
        }
        else
        {
            return compareIndirect(s1, length1, s2, length2);
        }
    }

    ALWAYS_INLINE inline int compareIndirect(const char * s1, size_t length1, const char * s2, size_t length2) const
    {
        return ptr->compare(s1, length1, s2, length2);
    }

    ALWAYS_INLINE inline bool canUseFastPath() const
    {
        return collator_id == ITiDBCollator::UTF8MB4_BIN;
    }

    ALWAYS_INLINE inline StringRef sortKey(const char * s, size_t length, std::string & container) const
    {
        if (likely(canUseFastPath()))
        {
            return fastPathSortKey(s, length);
        }
        else
        {
            return sortKeyIndirect(s, length, container);
        }
    }

    static ALWAYS_INLINE inline StringRef fastPathSortKey(const char * s, size_t length)
    {
        return DB::BinCollatorSortKey<true>(s, length);
    }

    ALWAYS_INLINE inline StringRef sortKeyIndirect(const char * s, size_t length, std::string & container) const
    {
        return ptr->sortKey(s, length, container);
    }

    ALWAYS_INLINE inline std::unique_ptr<ITiDBCollator::IPattern> pattern() const { return ptr->pattern(); }

    ALWAYS_INLINE int32_t getCollatorId() const { return collator_id; }
};

struct TiDBCollatorPtr
{
    TiDBCollatorPtrImpl inner;

    TiDBCollatorPtr(class ITiDBCollator const * ptr_, int32_t collator_id_)
        : inner(ptr_, collator_id_)
    {
    }
    TiDBCollatorPtr(class ITiDBCollator const * ptr_ = nullptr) // NOLINT
        : inner(ptr_)
    {
    }
    ALWAYS_INLINE bool operator==(const void * tar) const
    {
        return inner.ptr == tar;
    }
    ALWAYS_INLINE bool operator!=(const void * tar) const
    {
        return inner.ptr != tar;
    }

    ALWAYS_INLINE explicit operator bool() const
    {
        return inner.ptr;
    }
    ALWAYS_INLINE TiDBCollatorPtrImpl * operator->()
    {
        return &inner;
    }
    ALWAYS_INLINE const TiDBCollatorPtrImpl * operator->() const
    {
        return &inner;
    }
};

} // namespace TiDB
