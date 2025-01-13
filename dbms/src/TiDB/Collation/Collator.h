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
#include <Common/UTF8Helpers.h>
#include <TiDB/Collation/CollatorCompare.h>
#include <TiDB/Schema/TiDB_fwd.h>
#include <common/StringRef.h>

#include <memory>

namespace TiDB
{

class ITiDBCollator
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

    virtual ~ITiDBCollator() = default;

    virtual int compare(const char * s1, size_t length1, const char * s2, size_t length2) const = 0;

    ALWAYS_INLINE inline int compareFastPath(const char * s1, size_t length1, const char * s2, size_t length2) const
    {
        if (likely(isPaddingBinary()))
        {
            return DB::BinCollatorCompare<true>(s1, length1, s2, length2);
        }
        return compare(s1, length1, s2, length2);
    }

    // Convert raw string to collate string and return the length of each character
    virtual StringRef convert(const char * s, size_t length, std::string & container, std::vector<size_t> * lens) const
        = 0;
    virtual StringRef sortKeyNoTrim(const char * s, size_t length, std::string & container) const = 0;
    virtual StringRef sortKey(const char * s, size_t length, std::string & container) const = 0;
    virtual size_t maxBytesForOneChar() const = 0;
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

inline void fillLensForBinCollator(const char * start, const char * end, std::vector<size_t> * lens)
{
    lens->resize(0);
    const char * it = start;
    while (it != end)
    {
        UInt8 len = DB::UTF8::seqLength(static_cast<UInt8>(*it));
        lens->push_back(len);

        if likely (end - it >= len)
            it += len;
        else
            throw DB::Exception("Encounter invalid character");
    }
}

template <bool need_len>
StringRef convertForBinCollator(const char * start, size_t len, std::vector<size_t> * lens)
{
    if constexpr (need_len)
        TiDB::fillLensForBinCollator(start, start + len, lens);
    return DB::BinCollatorSortKey<false>(start, len);
}

template <typename Collator>
class Pattern : public ITiDBCollator::IPattern
{
public:
    void compile(const std::string & pattern, char escape) override;
    bool match(const char * s, size_t length) const override;

private:
    std::vector<typename Collator::CharType> chars;
    enum MatchType
    {
        Match,
        One,
        Any,
    };
    std::vector<MatchType> match_types;
};

template <typename T, bool padding = false>
class BinCollator final : public ITiDBCollator
{
public:
    explicit BinCollator(int32_t id)
        : ITiDBCollator(id)
    {}

    int compare(const char * s1, size_t length1, const char * s2, size_t length2) const override
    {
        return DB::BinCollatorCompare<padding>(s1, length1, s2, length2);
    }

    StringRef sortKey(const char * s, size_t length, std::string &) const override
    {
        return DB::BinCollatorSortKey<padding>(s, length);
    }

    StringRef sortKeyNoTrim(const char * s, size_t length, std::string &) const override
    {
        return convertForBinCollator<false>(s, length, nullptr);
    }

    StringRef convert(const char * s, size_t length, std::string &, std::vector<size_t> * lens) const override
    {
        return convertForBinCollator<true>(s, length, lens);
    }

    std::unique_ptr<IPattern> pattern() const override;

    size_t maxBytesForOneChar() const override
    {
        // BinCollator only trims trailing spaces,
        // so it does not increase the space required after decoding.
        // Hence, it returns 1 here.
        return 1;
    }

private:
    const std::string name = padding ? "BinaryPadding" : "Binary";

private:
    using WeightType = T;
    using CharType = T;

    static inline CharType decodeChar(const char * s, size_t & offset);

    static inline WeightType weight(CharType c) { return c; }

    static inline bool regexEq(CharType a, CharType b) { return weight(a) == weight(b); }

    friend class Pattern<BinCollator>;
};

using Rune = int32_t;
namespace UnicodeCI
{
using long_weight = struct
{
    uint64_t first;
    uint64_t second;
};
} // namespace UnicodeCI

class Unicode0400
{
public:
    static inline bool regexEq(Rune a, Rune b);
    static inline bool weight(uint64_t & first, uint64_t & second, Rune r);

private:
    static inline const UnicodeCI::long_weight & weightLutLongMap(Rune r);
};

class Unicode0900
{
public:
    static inline bool regexEq(Rune a, Rune b);

    static inline bool weight(uint64_t & first, uint64_t & second, Rune r);

private:
    static inline const UnicodeCI::long_weight & weightLutLongMap(Rune r);
};

template <typename T, bool padding>
class UCACICollator final : public ITiDBCollator
{
public:
    explicit UCACICollator(int32_t id)
        : ITiDBCollator(id)
    {}

    int compare(const char * s1, size_t length1, const char * s2, size_t length2) const override;

    StringRef convert(const char * s, size_t length, std::string & container, std::vector<size_t> * lens) const override
    {
        return convertImpl<true, false>(s, length, container, lens);
    }

    StringRef sortKey(const char * s, size_t length, std::string & container) const override
    {
        return convertImpl<false, true>(s, length, container, nullptr);
    }

    StringRef sortKeyNoTrim(const char * s, size_t length, std::string & container) const override
    {
        return convertImpl<false, false>(s, length, container, nullptr);
    }

    std::unique_ptr<IPattern> pattern() const override { return std::make_unique<Pattern<UCACICollator>>(); }

    size_t maxBytesForOneChar() const override
    {
        // Every char have 8 uint16 at most.
        return 8 * sizeof(uint16_t);
    }

private:
    const std::string name = "UnicodeCI";

private:
    using CharType = Rune;

    template <bool need_len, bool need_trim>
    StringRef convertImpl(const char * s, size_t length, std::string & container, std::vector<size_t> * lens) const;

    static inline CharType decodeChar(const char * s, size_t & offset);

    static inline void writeResult(uint64_t & w, std::string & container, size_t & total_size)
    {
        while (w != 0)
        {
            container[total_size++] = static_cast<char>(w >> 8);
            container[total_size++] = static_cast<char>(w);
            w >>= 16;
        }
    }

    static inline bool regexEq(CharType a, CharType b) { return T::regexEq(a, b); }

    static inline void weight(uint64_t & first, uint64_t & second, size_t & offset, size_t length, const char * s);

    static inline std::string_view preprocess(const char * s, size_t length);
    friend class Pattern<UCACICollator>;
};

namespace GeneralCI
{
using WeightType = uint16_t;
extern const std::array<WeightType, 256 * 256> weight_lut;
} // namespace GeneralCI

class GeneralCICollator final : public ITiDBCollator
{
public:
    explicit GeneralCICollator(int32_t id)
        : ITiDBCollator(id)
    {}

    int compare(const char * s1, size_t length1, const char * s2, size_t length2) const override;

    StringRef convert(const char * s, size_t length, std::string & container, std::vector<size_t> * lens) const override
    {
        return convertImpl<true, false>(s, length, container, lens);
    }

    StringRef sortKey(const char * s, size_t length, std::string & container) const override
    {
        return convertImpl<false, true>(s, length, container, nullptr);
    }

    StringRef sortKeyNoTrim(const char * s, size_t length, std::string & container) const override
    {
        return convertImpl<false, false>(s, length, container, nullptr);
    }

    std::unique_ptr<IPattern> pattern() const override { return std::make_unique<Pattern<GeneralCICollator>>(); }

    size_t maxBytesForOneChar() const override { return sizeof(WeightType); }

private:
    const std::string name = "GeneralCI";

private:
    using WeightType = GeneralCI::WeightType;
    using CharType = Rune;

    template <bool need_len, bool need_trim>
    StringRef convertImpl(const char * s, size_t length, std::string & container, std::vector<size_t> * lens) const;

    static inline CharType decodeChar(const char * s, size_t & offset);

    static inline WeightType weight(CharType c)
    {
        if (c > 0xFFFF)
            return 0xFFFD;
        return GeneralCI::weight_lut[c & 0xFFFF];
        //return !!(c >> 16) * 0xFFFD + (1 - !!(c >> 16)) * GeneralCI::weight_lut_0400[c & 0xFFFF];
    }

    static inline bool regexEq(CharType a, CharType b) { return weight(a) == weight(b); }

    friend class Pattern<GeneralCICollator>;
};

using UTF8MB4_BIN_TYPE = BinCollator<Rune, true>;
using UTF8MB4_0900_BIN_TYPE = BinCollator<Rune, false>;
using UCACI_0400_PADDING = UCACICollator<Unicode0400, true>;
using UCACI_0900_NON_PADDING = UCACICollator<Unicode0900, false>;
using BIN_COLLATOR_PADDING = BinCollator<char, true>;
using BIN_COLLATOR_NON_PADDING = BinCollator<char, false>;
} // namespace TiDB

#define APPLY_FOR_COLLATOR_TYPES_WITH_VARS(VAR_PREFIX, M) \
    M(VAR_PREFIX##_utf8_general_ci, TiDB::GeneralCICollator, TiDB::ITiDBCollator::UTF8_GENERAL_CI) \
    M(VAR_PREFIX##_utf8mb4_general_ci, TiDB::GeneralCICollator, TiDB::ITiDBCollator::UTF8MB4_GENERAL_CI) \
    M(VAR_PREFIX##_utf8_unicode_ci, TiDB::UCACI_0400_PADDING, TiDB::ITiDBCollator::UTF8_UNICODE_CI) \
    M(VAR_PREFIX##_utf8mb4_unicode_ci, TiDB::UCACI_0400_PADDING, TiDB::ITiDBCollator::UTF8MB4_UNICODE_CI) \
    M(VAR_PREFIX##_utf8mb4_0900_ai_ci, TiDB::UCACI_0900_NON_PADDING, TiDB::ITiDBCollator::UTF8MB4_0900_AI_CI) \
    M(VAR_PREFIX##_utf8mb4_0900_bin, TiDB::UTF8MB4_0900_BIN_TYPE, TiDB::ITiDBCollator::UTF8MB4_0900_BIN) \
    M(VAR_PREFIX##_utf8mb4_bin, TiDB::UTF8MB4_BIN_TYPE, TiDB::ITiDBCollator::UTF8MB4_BIN) \
    M(VAR_PREFIX##_latin1_bin, TiDB::BIN_COLLATOR_PADDING, TiDB::ITiDBCollator::LATIN1_BIN) \
    M(VAR_PREFIX##_binary, TiDB::BIN_COLLATOR_NON_PADDING, TiDB::ITiDBCollator::BINARY) \
    M(VAR_PREFIX##_ascii_bin, TiDB::BIN_COLLATOR_PADDING, TiDB::ITiDBCollator::ASCII_BIN) \
    M(VAR_PREFIX##_utf8_bin, TiDB::UTF8MB4_BIN_TYPE, TiDB::ITiDBCollator::UTF8_BIN)

#define APPLY_FOR_COLLATOR_TYPES(M) APPLY_FOR_COLLATOR_TYPES_WITH_VARS(tmp, M)
