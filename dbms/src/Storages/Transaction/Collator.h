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

#include <Columns/Collator.h>
#include <Storages/Transaction/CollatorCompare.h>
#include <common/StringRef.h>

#include <memory>
#include <unordered_map>

namespace TiDB
{

using TiDBCollatorPtr = class ITiDBCollator const *;
using TiDBCollators = std::vector<TiDBCollatorPtr>;

class ITiDBCollator : public ICollator
{
public:
    enum
    {
        UTF8_GENERAL_CI = 33,
        UTF8MB4_GENERAL_CI = 45,
        UTF8_UNICODE_CI = 192,
        UTF8MB4_UNICODE_CI = 224,
        UTF8MB4_0900_AI_CI = 255,
        UTF8MB4_0900_BIN = 309,
        UTF8MB4_BIN = 46,
        LATIN1_BIN = 47,
        BINARY = 63,
        ASCII_BIN = 65,
        UTF8_BIN = 83,
    };

    // internal wrapped collator types which are effective for `switch case`
    enum class CollatorType : uint32_t
    {
        // bin
        UTF8MB4_BIN = 0,
        UTF8_BIN,
        LATIN1_BIN,
        ASCII_BIN,
        // binary
        BINARY,
        // ----
        UTF8_GENERAL_CI,
        UTF8MB4_GENERAL_CI,
        UTF8_UNICODE_CI,
        UTF8MB4_UNICODE_CI,
        UTF8MB4_0900_AI_CI,
        UTF8MB4_0900_BIN,
        // ----
        MAX_,
    };

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

    int compare(const char * s1, size_t length1, const char * s2, size_t length2) const override = 0;

    ALWAYS_INLINE inline int compareFastPath(const char * s1, size_t length1, const char * s2, size_t length2) const
    {
        if (likely(isPaddingBinary()))
        {
            return DB::BinCollatorCompare<true>(s1, length1, s2, length2);
        }
        return compare(s1, length1, s2, length2);
    }

    virtual StringRef sortKey(const char * s, size_t length, std::string & container) const = 0;
    virtual std::unique_ptr<IPattern> pattern() const = 0;
    int32_t getCollatorId() const { return collator_id; }
    CollatorType getCollatorType() const { return collator_type; }
    bool isBinary() const;
    bool isCI() const;

    ALWAYS_INLINE static inline bool isPaddingBinary(CollatorType collator_type)
    {
        switch (collator_type)
        {
        case CollatorType::UTF8MB4_BIN:
        case CollatorType::UTF8_BIN:
        case CollatorType::LATIN1_BIN:
        case CollatorType::ASCII_BIN:
        {
            // collator_type < 4
            return true;
        }
        default:
            break;
        }
        return false;
    }

    ALWAYS_INLINE inline bool isPaddingBinary() const { return isPaddingBinary(getCollatorType()); }

    ALWAYS_INLINE inline StringRef sortKeyFastPath(const char * s, size_t length, std::string & container) const
    {
        if (likely(isPaddingBinary()))
        {
            return DB::BinCollatorSortKey<true>(s, length);
        }
        return sortKey(s, length, container);
    }

protected:
    explicit ITiDBCollator(int32_t collator_id_);
    int32_t collator_id; // collator id to be compatible with TiDB
    CollatorType collator_type{CollatorType::MAX_}; // collator type for internal usage
};

/// these dummy_xxx are used as the default value to avoid too many meaningless
/// modification on the legacy ClickHouse code
extern TiDBCollators dummy_collators;
extern std::vector<std::string> dummy_sort_key_contaners;
extern std::string dummy_sort_key_contaner;

ITiDBCollator::CollatorType GetTiDBCollatorType(const void * collator);

} // namespace TiDB
