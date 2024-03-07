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

#include <Common/Checksum.h>
#include <Common/FieldVisitors.h>
#include <Common/config.h>
#include <Common/getNumberOfCPUCores.h>
#include <Core/Field.h>
#include <DataStreams/SizeLimits.h>
#include <Flash/Pipeline/Schedule/TaskQueues/TaskQueueType.h>
#include <IO/Buffer/ReadBufferFromString.h>
#include <IO/Compression/CompressedStream.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Poco/String.h>
#include <Poco/Timespan.h>

namespace DB
{
namespace ErrorCodes
{
extern const int TYPE_MISMATCH;
extern const int UNKNOWN_LOAD_BALANCING;
extern const int UNKNOWN_OVERFLOW_MODE;
extern const int ILLEGAL_OVERFLOW_MODE;
extern const int UNKNOWN_COMPRESSION_METHOD;
extern const int CANNOT_PARSE_BOOL;
extern const int INVALID_CONFIG_PARAMETER;
extern const int BAD_ARGUMENTS;
} // namespace ErrorCodes


/** One setting for any type.
  * Stores a value within itself, as well as a flag - whether the value was changed.
  * This is done so that you can send to the remote servers only changed settings (or explicitly specified in the config) values.
  * That is, if the configuration was not specified in the config and was not dynamically changed, it is not sent to the remote server,
  *  and the remote server will use its default value.
  */

template <typename IntType>
struct SettingInt
{
public:
    bool changed = false;

    SettingInt(IntType x = 0)
        : value(x)
    {}
    SettingInt(const SettingInt & setting);

    operator IntType() const { return value.load(); }
    SettingInt & operator=(IntType x)
    {
        set(x);
        return *this;
    }
    SettingInt & operator=(const SettingInt & setting)
    {
        set(setting.value.load());
        return *this;
    }

    String toString() const;

    void set(IntType x);

    void set(const Field & x);

    void set(const String & x);

    void set(ReadBuffer & buf);

    IntType get() const;

    void write(WriteBuffer & buf) const;

private:
    std::atomic<IntType> value;
};

// Make SettingInt formatable by fmtlib
template <typename IntType>
ALWAYS_INLINE inline auto format_as(SettingInt<IntType> s)
{
    return s.get();
}

using SettingUInt64 = SettingInt<UInt64>;
using SettingInt64 = SettingInt<Int64>;
using SettingBool = SettingInt<bool>;


/** Unlike SettingUInt64, supports the value of 'auto' - the number of processor cores without taking into account SMT.
  * A value of 0 is also treated as auto.
  * When serializing, `auto` is written in the same way as 0.
  */
struct SettingMaxThreads
{
public:
    bool is_auto;
    bool changed = false;

    SettingMaxThreads(UInt64 x = 0)
        : is_auto(x == 0)
        , value(x ? x : getAutoValue())
    {}

    operator UInt64() const { return value; }
    SettingMaxThreads & operator=(UInt64 x)
    {
        set(x);
        return *this;
    }

    String toString() const
    {
        /// Instead of the `auto` value, we output the actual value to make it easier to see.
        return DB::toString(value);
    }

    void set(UInt64 x)
    {
        value = x ? x : getAutoValue();
        is_auto = x == 0;
        changed = true;
    }

    void set(const Field & x)
    {
        if (x.getType() == Field::Types::String)
            set(safeGet<const String &>(x));
        else
            set(safeGet<UInt64>(x));
    }

    void set(const String & x)
    {
        if (x == "auto")
            setAuto();
        else
            set(parse<UInt64>(x));
    }

    void set(ReadBuffer & buf)
    {
        UInt64 x = 0;
        readVarUInt(x, buf);
        set(x);
    }

    void write(WriteBuffer & buf) const { writeVarUInt(is_auto ? 0 : value, buf); }

    void setAuto()
    {
        value = getAutoValue();
        is_auto = true;
    }

    static UInt64 getAutoValue()
    {
        static auto res = getNumberOfLogicalCPUCores();
        return res;
    }

    UInt64 get() const { return value; }

private:
    UInt64 value;
};


struct SettingSeconds
{
public:
    bool changed = false;

    SettingSeconds(UInt64 seconds = 0)
        : value(seconds, 0)
    {}

    operator Poco::Timespan() const { return value; }
    SettingSeconds & operator=(const Poco::Timespan & x)
    {
        set(x);
        return *this;
    }

    Poco::Timespan::TimeDiff totalSeconds() const { return value.totalSeconds(); }

    String toString() const { return DB::toString(totalSeconds()); }

    void set(const Poco::Timespan & x)
    {
        value = x;
        changed = true;
    }

    void set(UInt64 x) { set(Poco::Timespan(x, 0)); }

    void set(const Field & x) { set(safeGet<UInt64>(x)); }

    void set(const String & x) { set(parse<UInt64>(x)); }

    void set(ReadBuffer & buf)
    {
        UInt64 x = 0;
        readVarUInt(x, buf);
        set(x);
    }

    void write(WriteBuffer & buf) const { writeVarUInt(value.totalSeconds(), buf); }

    Poco::Timespan get() const { return value; }

private:
    Poco::Timespan value;
};


struct SettingMilliseconds
{
public:
    bool changed = false;

    SettingMilliseconds(UInt64 milliseconds = 0)
        : value(milliseconds * 1000)
    {}

    operator Poco::Timespan() const { return value; }
    SettingMilliseconds & operator=(const Poco::Timespan & x)
    {
        set(x);
        return *this;
    }

    Poco::Timespan::TimeDiff totalMilliseconds() const { return value.totalMilliseconds(); }

    String toString() const { return DB::toString(totalMilliseconds()); }

    void set(const Poco::Timespan & x)
    {
        value = x;
        changed = true;
    }

    void set(UInt64 x) { set(Poco::Timespan(x * 1000)); }

    void set(const Field & x) { set(safeGet<UInt64>(x)); }

    void set(const String & x) { set(parse<UInt64>(x)); }

    void set(ReadBuffer & buf)
    {
        UInt64 x = 0;
        readVarUInt(x, buf);
        set(x);
    }

    void write(WriteBuffer & buf) const { writeVarUInt(value.totalMilliseconds(), buf); }

    Poco::Timespan get() const { return value; }

private:
    Poco::Timespan value;
};


struct SettingFloat
{
public:
    bool changed = false;

    SettingFloat(float x = 0)
        : value(x)
    {}
    SettingFloat(const SettingFloat & setting) { value.store(setting.value.load()); }
    operator float() const { return value.load(); }
    SettingFloat & operator=(float x)
    {
        set(x);
        return *this;
    }
    SettingFloat & operator=(const SettingFloat & setting)
    {
        set(setting.value.load());
        return *this;
    }

    String toString() const { return DB::toString(value.load()); }

    void set(float x)
    {
        value.store(x);
        changed = true;
    }

    void set(const Field & x)
    {
        if (x.getType() == Field::Types::UInt64)
        {
            set(safeGet<UInt64>(x));
        }
        else if (x.getType() == Field::Types::Int64)
        {
            set(safeGet<Int64>(x));
        }
        else if (x.getType() == Field::Types::Float64)
        {
            set(safeGet<Float64>(x));
        }
        else
            throw Exception(
                std::string("Bad type of setting. Expected UInt64, Int64 or Float64, got ") + x.getTypeName(),
                ErrorCodes::TYPE_MISMATCH);
    }

    void set(const String & x) { set(parse<float>(x)); }

    void set(ReadBuffer & buf)
    {
        String x;
        readBinary(x, buf);
        set(x);
    }

    void write(WriteBuffer & buf) const { writeBinary(toString(), buf); }

    float get() const { return value.load(); }

private:
    std::atomic<float> value;
};

// Make SettingFloat formatable by fmtlib
ALWAYS_INLINE inline auto format_as(SettingFloat s)
{
    return s.get();
}

/// MemoryLimit can either be an UInt64 (means memory limit in bytes),
/// or be a float-point number (means memory limit ratio of total RAM, from 0.0 to 1.0).
/// 0 or 0.0 means unlimited.
struct SettingMemoryLimit
{
public:
    bool changed = false;

    using UInt64OrDouble = std::variant<UInt64, double>;
    struct ToStringVisitor;

    explicit SettingMemoryLimit(UInt64 bytes = 0);
    explicit SettingMemoryLimit(double percent = 0.0);

    SettingMemoryLimit(const SettingMemoryLimit & setting);
    SettingMemoryLimit & operator=(UInt64OrDouble x);
    SettingMemoryLimit & operator=(const SettingMemoryLimit & setting);

    void set(UInt64OrDouble x);
    void set(UInt64 x);
    void set(double x);
    void set(const Field & x);
    void set(const String & x);
    void set(ReadBuffer & buf);

    String toString() const;
    void write(WriteBuffer & buf) const;
    UInt64OrDouble get() const;

    UInt64 getActualBytes(UInt64 total_ram) const;

private:
    UInt64OrDouble value;
};

struct SettingDouble
{
public:
    bool changed = false;

    SettingDouble(double x = 0)
        : value(x)
    {}
    SettingDouble(const SettingDouble & setting) { value.store(setting.value.load()); }
    operator double() const { return value.load(); }
    SettingDouble & operator=(double x)
    {
        set(x);
        return *this;
    }
    SettingDouble & operator=(const SettingDouble & setting)
    {
        set(setting.value.load());
        return *this;
    }

    String toString() const { return DB::toString(value.load()); }

    void set(double x)
    {
        value.store(x);
        changed = true;
    }

    void set(const Field & x)
    {
        if (x.getType() == Field::Types::UInt64)
        {
            set(safeGet<UInt64>(x));
        }
        else if (x.getType() == Field::Types::Int64)
        {
            set(safeGet<Int64>(x));
        }
        else if (x.getType() == Field::Types::Float64)
        {
            set(safeGet<Float64>(x));
        }
        else
            throw Exception(
                std::string("Bad type of setting. Expected UInt64, Int64 or Float64, got ") + x.getTypeName(),
                ErrorCodes::TYPE_MISMATCH);
    }

    void set(const String & x) { set(parse<double>(x)); }

    void set(ReadBuffer & buf)
    {
        String x;
        readBinary(x, buf);
        set(x);
    }

    void write(WriteBuffer & buf) const { writeBinary(toString(), buf); }

    double get() const { return value.load(); }

private:
    std::atomic<double> value;
};

// Make SettingDouble formatable by fmtlib
ALWAYS_INLINE inline auto format_as(SettingDouble s)
{
    return s.get();
}

enum class LoadBalancing
{
    /// among replicas with a minimum number of errors selected randomly
    RANDOM = 0,
    /// a replica is selected among the replicas with the minimum number of errors
    /// with the minimum number of distinguished characters in the replica name and local hostname
    NEAREST_HOSTNAME,
    /// replicas are walked through strictly in order; the number of errors does not matter
    IN_ORDER,
};

struct SettingLoadBalancing
{
public:
    bool changed = false;

    SettingLoadBalancing(LoadBalancing x)
        : value(x)
    {}

    operator LoadBalancing() const { return value; }
    SettingLoadBalancing & operator=(LoadBalancing x)
    {
        set(x);
        return *this;
    }

    static LoadBalancing getLoadBalancing(const String & s)
    {
        if (s == "random")
            return LoadBalancing::RANDOM;
        if (s == "nearest_hostname")
            return LoadBalancing::NEAREST_HOSTNAME;
        if (s == "in_order")
            return LoadBalancing::IN_ORDER;

        throw Exception(
            "Unknown load balancing mode: '" + s + "', must be one of 'random', 'nearest_hostname', 'in_order'",
            ErrorCodes::UNKNOWN_LOAD_BALANCING);
    }

    String toString() const
    {
        const char * strings[] = {"random", "nearest_hostname", "in_order"};
        if (value < LoadBalancing::RANDOM || value > LoadBalancing::IN_ORDER)
            throw Exception("Unknown load balancing mode", ErrorCodes::UNKNOWN_LOAD_BALANCING);
        return strings[static_cast<size_t>(value)];
    }

    void set(LoadBalancing x)
    {
        value = x;
        changed = true;
    }

    void set(const Field & x) { set(safeGet<const String &>(x)); }

    void set(const String & x) { set(getLoadBalancing(x)); }

    void set(ReadBuffer & buf)
    {
        String x;
        readBinary(x, buf);
        set(x);
    }

    void write(WriteBuffer & buf) const { writeBinary(toString(), buf); }

    LoadBalancing get() const { return value; }

private:
    LoadBalancing value;
};

template <bool enable_mode_any>
struct SettingOverflowMode
{
public:
    bool changed = false;

    SettingOverflowMode(OverflowMode x = OverflowMode::THROW)
        : value(x)
    {}

    operator OverflowMode() const { return value; }
    SettingOverflowMode & operator=(OverflowMode x)
    {
        set(x);
        return *this;
    }

    static OverflowMode getOverflowModeForGroupBy(const String & s)
    {
        if (s == "throw")
            return OverflowMode::THROW;
        if (s == "break")
            return OverflowMode::BREAK;

        throw Exception(
            "Unknown overflow mode: '" + s + "', must be one of 'throw', 'break', 'any'",
            ErrorCodes::UNKNOWN_OVERFLOW_MODE);
    }

    static OverflowMode getOverflowMode(const String & s) { return getOverflowModeForGroupBy(s); }

    String toString() const
    {
        const char * strings[] = {"throw", "break"};

        if (value < OverflowMode::THROW || value > OverflowMode::BREAK)
            throw Exception("Unknown overflow mode", ErrorCodes::UNKNOWN_OVERFLOW_MODE);

        return strings[static_cast<size_t>(value)];
    }

    void set(OverflowMode x)
    {
        value = x;
        changed = true;
    }

    void set(const Field & x) { set(safeGet<const String &>(x)); }

    void set(const String & x) { set(getOverflowMode(x)); }

    void set(ReadBuffer & buf)
    {
        String x;
        readBinary(x, buf);
        set(x);
    }

    void write(WriteBuffer & buf) const { writeBinary(toString(), buf); }

    OverflowMode get() const { return value; }

private:
    OverflowMode value;
};

struct SettingChecksumAlgorithm
{
public:
    bool changed = false;

    SettingChecksumAlgorithm(ChecksumAlgo x = ChecksumAlgo::XXH3) // NOLINT(google-explicit-constructor)
        : value(x)
    {}

    operator ChecksumAlgo() const { return value; } // NOLINT(google-explicit-constructor)
    SettingChecksumAlgorithm & operator=(ChecksumAlgo x)
    {
        set(x);
        return *this;
    }

    void set(ChecksumAlgo x)
    {
        value = x;
        changed = true;
    }

    void set(const Field & x) { set(safeGet<const String &>(x)); }

    void set(const String & x) { set(getChecksumAlgorithm(x)); }

    void set(ReadBuffer & buf)
    {
        String x;
        readBinary(x, buf);
        set(x);
    }

    void write(WriteBuffer & buf) const { writeBinary(toString(), buf); }

    ChecksumAlgo get() const { return value; }

    String toString() const
    {
        if (value == ChecksumAlgo::XXH3)
            return "xxh3";
        if (value == ChecksumAlgo::City128)
            return "city128";
        if (value == ChecksumAlgo::CRC32)
            return "crc32";
        if (value == ChecksumAlgo::CRC64)
            return "crc64";
        if (value == ChecksumAlgo::None)
            return "none";

        throw Exception(
            "invalid checksum algorithm value: " + ::DB::toString(static_cast<size_t>(value)),
            ErrorCodes::INVALID_CONFIG_PARAMETER);
    }

private:
    static ChecksumAlgo getChecksumAlgorithm(const String & s)
    {
        if (s == "xxh3")
            return ChecksumAlgo::XXH3;
        if (s == "city128")
            return ChecksumAlgo::City128;
        if (s == "crc32")
            return ChecksumAlgo::CRC32;
        if (s == "crc64")
            return ChecksumAlgo::CRC64;
        if (s == "none")
            return ChecksumAlgo::None;

        throw Exception(
            "Unknown checksum algorithm: '" + s + "', must be one of 'xxh3', 'city128', 'crc32', 'crc64', 'none'",
            ErrorCodes::INVALID_CONFIG_PARAMETER);
    }
    ChecksumAlgo value;
};

struct SettingCompressionMethod
{
public:
    bool changed = false;

    SettingCompressionMethod(CompressionMethod x = CompressionMethod::LZ4)
        : value(x)
    {}

    operator CompressionMethod() const { return value; }
    SettingCompressionMethod & operator=(CompressionMethod x)
    {
        set(x);
        return *this;
    }

    static CompressionMethod getCompressionMethod(const String & s)
    {
        String lower_str = Poco::toLower(s);
        if (lower_str == "lz4")
            return CompressionMethod::LZ4;
        if (lower_str == "lz4hc")
            return CompressionMethod::LZ4HC;
        if (lower_str == "zstd")
            return CompressionMethod::ZSTD;
#if USE_QPL
        if (lower_str == "qpl")
            return CompressionMethod::QPL;
        throw Exception(
            "Unknown compression method: '" + s + "', must be one of 'lz4', 'lz4hc', 'zstd', 'qpl'",
            ErrorCodes::UNKNOWN_COMPRESSION_METHOD);
#else
        throw Exception(
            "Unknown compression method: '" + s + "', must be one of 'lz4', 'lz4hc', 'zstd'",
            ErrorCodes::UNKNOWN_COMPRESSION_METHOD);
#endif
    }

    String toString() const
    {
#if USE_QPL
        const char * strings[] = {nullptr, "lz4", "lz4hc", "zstd", "qpl"};
        auto compression_method_last = CompressionMethod::QPL;
#else
        const char * strings[] = {nullptr, "lz4", "lz4hc", "zstd"};
        auto compression_method_last = CompressionMethod::ZSTD;
#endif

        if (value < CompressionMethod::LZ4 || value > compression_method_last)
            throw Exception("Unknown compression method", ErrorCodes::UNKNOWN_COMPRESSION_METHOD);

        return strings[static_cast<size_t>(value)];
    }

    void set(CompressionMethod x)
    {
        value = x;
        changed = true;
    }

    void set(const Field & x) { set(safeGet<const String &>(x)); }

    void set(const String & x) { set(getCompressionMethod(x)); }

    void set(ReadBuffer & buf)
    {
        String x;
        readBinary(x, buf);
        set(x);
    }

    void write(WriteBuffer & buf) const { writeBinary(toString(), buf); }

    CompressionMethod get() const { return value; }

private:
    CompressionMethod value;
};

struct SettingTaskQueueType
{
public:
    bool changed = false;

    SettingTaskQueueType(TaskQueueType x = TaskQueueType::DEFAULT)
        : value(x)
    {}

    operator TaskQueueType() const { return value; }
    SettingTaskQueueType & operator=(TaskQueueType x)
    {
        set(x);
        return *this;
    }

    static TaskQueueType getTaskQueueType(const String & s);

    String toString() const;

    void set(TaskQueueType x)
    {
        value = x;
        changed = true;
    }

    void set(const Field & x) { set(safeGet<const String &>(x)); }

    void set(const String & x) { set(getTaskQueueType(x)); }

    void set(ReadBuffer & buf)
    {
        String x;
        readBinary(x, buf);
        set(x);
    }

    void write(WriteBuffer & buf) const { writeBinary(toString(), buf); }

    TaskQueueType get() const { return value; }

private:
    TaskQueueType value;
};

/// The setting for executing distributed subqueries inside IN or JOIN sections.
enum class DistributedProductMode
{
    DENY = 0, /// Disable
    LOCAL, /// Convert to local query
    GLOBAL, /// Convert to global query
    ALLOW /// Enable
};

struct SettingString
{
public:
    bool changed = false;

    SettingString(const String & x = String{})
        : value(x)
    {}

    operator String() const { return value; }
    SettingString & operator=(const String & x)
    {
        set(x);
        return *this;
    }

    String toString() const { return value; }

    void set(const String & x)
    {
        value = x;
        changed = true;
    }

    void set(const Field & x) { set(safeGet<const String &>(x)); }

    void set(ReadBuffer & buf)
    {
        String x;
        readBinary(x, buf);
        set(x);
    }

    void write(WriteBuffer & buf) const { writeBinary(value, buf); }

    String get() const { return value; }

private:
    String value;
};

} // namespace DB
