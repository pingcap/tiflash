#pragma once

#include <Common/Checksum.h>
#include <Common/FieldVisitors.h>
#include <Common/getNumberOfPhysicalCPUCores.h>
#include <Core/Field.h>
#include <DataStreams/SizeLimits.h>
#include <IO/CompressedStream.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Poco/Timespan.h>


namespace DB
{
namespace ErrorCodes
{
extern const int TYPE_MISMATCH;
extern const int UNKNOWN_LOAD_BALANCING;
extern const int UNKNOWN_OVERFLOW_MODE;
extern const int ILLEGAL_OVERFLOW_MODE;
extern const int UNKNOWN_TOTALS_MODE;
extern const int UNKNOWN_COMPRESSION_METHOD;
extern const int UNKNOWN_DISTRIBUTED_PRODUCT_MODE;
extern const int UNKNOWN_GLOBAL_SUBQUERIES_METHOD;
extern const int CANNOT_PARSE_BOOL;
extern const int INVALID_CONFIG_PARAMETER;
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

    void write(WriteBuffer & buf) const
    {
        writeVarUInt(is_auto ? 0 : value, buf);
    }

    void setAuto()
    {
        value = getAutoValue();
        is_auto = true;
    }

    UInt64 getAutoValue() const
    {
        static auto res = getAutoValueImpl();
        return res;
    }

    /// Executed once for all time. Executed from one thread.
    UInt64 getAutoValueImpl() const
    {
        return getNumberOfPhysicalCPUCores();
    }

    UInt64 get() const
    {
        return value;
    }

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

    String toString() const
    {
        return DB::toString(totalSeconds());
    }

    void set(const Poco::Timespan & x)
    {
        value = x;
        changed = true;
    }

    void set(UInt64 x)
    {
        set(Poco::Timespan(x, 0));
    }

    void set(const Field & x)
    {
        set(safeGet<UInt64>(x));
    }

    void set(const String & x)
    {
        set(parse<UInt64>(x));
    }

    void set(ReadBuffer & buf)
    {
        UInt64 x = 0;
        readVarUInt(x, buf);
        set(x);
    }

    void write(WriteBuffer & buf) const
    {
        writeVarUInt(value.totalSeconds(), buf);
    }

    Poco::Timespan get() const
    {
        return value;
    }

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

    String toString() const
    {
        return DB::toString(totalMilliseconds());
    }

    void set(const Poco::Timespan & x)
    {
        value = x;
        changed = true;
    }

    void set(UInt64 x)
    {
        set(Poco::Timespan(x * 1000));
    }

    void set(const Field & x)
    {
        set(safeGet<UInt64>(x));
    }

    void set(const String & x)
    {
        set(parse<UInt64>(x));
    }

    void set(ReadBuffer & buf)
    {
        UInt64 x = 0;
        readVarUInt(x, buf);
        set(x);
    }

    void write(WriteBuffer & buf) const
    {
        writeVarUInt(value.totalMilliseconds(), buf);
    }

    Poco::Timespan get() const
    {
        return value;
    }

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

    String toString() const
    {
        return DB::toString(value.load());
    }

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
            throw Exception(std::string("Bad type of setting. Expected UInt64, Int64 or Float64, got ") + x.getTypeName(), ErrorCodes::TYPE_MISMATCH);
    }

    void set(const String & x)
    {
        set(parse<float>(x));
    }

    void set(ReadBuffer & buf)
    {
        String x;
        readBinary(x, buf);
        set(x);
    }

    void write(WriteBuffer & buf) const
    {
        writeBinary(toString(), buf);
    }

    float get() const
    {
        return value.load();
    }

private:
    std::atomic<float> value;
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

    String toString() const
    {
        return DB::toString(value.load());
    }

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
            throw Exception(std::string("Bad type of setting. Expected UInt64, Int64 or Float64, got ") + x.getTypeName(), ErrorCodes::TYPE_MISMATCH);
    }

    void set(const String & x)
    {
        set(parse<double>(x));
    }

    void set(ReadBuffer & buf)
    {
        String x;
        readBinary(x, buf);
        set(x);
    }

    void write(WriteBuffer & buf) const
    {
        writeBinary(toString(), buf);
    }

    double get() const
    {
        return value.load();
    }

private:
    std::atomic<double> value;
};

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

        throw Exception("Unknown load balancing mode: '" + s + "', must be one of 'random', 'nearest_hostname', 'in_order'",
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

    void set(const Field & x)
    {
        set(safeGet<const String &>(x));
    }

    void set(const String & x)
    {
        set(getLoadBalancing(x));
    }

    void set(ReadBuffer & buf)
    {
        String x;
        readBinary(x, buf);
        set(x);
    }

    void write(WriteBuffer & buf) const
    {
        writeBinary(toString(), buf);
    }

    LoadBalancing get() const
    {
        return value;
    }

private:
    LoadBalancing value;
};


/// Which rows should be included in TOTALS.
enum class TotalsMode
{
    /// Count HAVING for all read rows;
    ///  including those not in max_rows_to_group_by
    ///  and have not passed HAVING after grouping.
    BEFORE_HAVING = 0,
    /// Count on all rows except those that have not passed HAVING;
    ///  that is, to include in TOTALS all the rows that did not pass max_rows_to_group_by.
    AFTER_HAVING_INCLUSIVE = 1,
    /// Include only the rows that passed and max_rows_to_group_by, and HAVING.
    AFTER_HAVING_EXCLUSIVE = 2,
    /// Automatically select between INCLUSIVE and EXCLUSIVE,
    AFTER_HAVING_AUTO = 3,
};

struct SettingTotalsMode
{
public:
    bool changed = false;

    SettingTotalsMode(TotalsMode x)
        : value(x)
    {}

    operator TotalsMode() const { return value; }
    SettingTotalsMode & operator=(TotalsMode x)
    {
        set(x);
        return *this;
    }

    static TotalsMode getTotalsMode(const String & s)
    {
        if (s == "before_having")
            return TotalsMode::BEFORE_HAVING;
        if (s == "after_having_exclusive")
            return TotalsMode::AFTER_HAVING_EXCLUSIVE;
        if (s == "after_having_inclusive")
            return TotalsMode::AFTER_HAVING_INCLUSIVE;
        if (s == "after_having_auto")
            return TotalsMode::AFTER_HAVING_AUTO;

        throw Exception("Unknown totals mode: '" + s + "', must be one of 'before_having', 'after_having_exclusive', 'after_having_inclusive', 'after_having_auto'", ErrorCodes::UNKNOWN_TOTALS_MODE);
    }

    String toString() const
    {
        switch (value)
        {
        case TotalsMode::BEFORE_HAVING:
            return "before_having";
        case TotalsMode::AFTER_HAVING_EXCLUSIVE:
            return "after_having_exclusive";
        case TotalsMode::AFTER_HAVING_INCLUSIVE:
            return "after_having_inclusive";
        case TotalsMode::AFTER_HAVING_AUTO:
            return "after_having_auto";

        default:
            throw Exception("Unknown TotalsMode enum value", ErrorCodes::UNKNOWN_TOTALS_MODE);
        }
    }

    void set(TotalsMode x)
    {
        value = x;
        changed = true;
    }

    void set(const Field & x)
    {
        set(safeGet<const String &>(x));
    }

    void set(const String & x)
    {
        set(getTotalsMode(x));
    }

    void set(ReadBuffer & buf)
    {
        String x;
        readBinary(x, buf);
        set(x);
    }

    void write(WriteBuffer & buf) const
    {
        writeBinary(toString(), buf);
    }

    TotalsMode get() const
    {
        return value;
    }

private:
    TotalsMode value;
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
        if (s == "any")
            return OverflowMode::ANY;

        throw Exception("Unknown overflow mode: '" + s + "', must be one of 'throw', 'break', 'any'", ErrorCodes::UNKNOWN_OVERFLOW_MODE);
    }

    static OverflowMode getOverflowMode(const String & s)
    {
        OverflowMode mode = getOverflowModeForGroupBy(s);

        if (mode == OverflowMode::ANY && !enable_mode_any)
            throw Exception("Illegal overflow mode: 'any' is only for 'group_by_overflow_mode'", ErrorCodes::ILLEGAL_OVERFLOW_MODE);

        return mode;
    }

    String toString() const
    {
        const char * strings[] = {"throw", "break", "any"};

        if (value < OverflowMode::THROW || value > OverflowMode::ANY)
            throw Exception("Unknown overflow mode", ErrorCodes::UNKNOWN_OVERFLOW_MODE);

        return strings[static_cast<size_t>(value)];
    }

    void set(OverflowMode x)
    {
        value = x;
        changed = true;
    }

    void set(const Field & x)
    {
        set(safeGet<const String &>(x));
    }

    void set(const String & x)
    {
        set(getOverflowMode(x));
    }

    void set(ReadBuffer & buf)
    {
        String x;
        readBinary(x, buf);
        set(x);
    }

    void write(WriteBuffer & buf) const
    {
        writeBinary(toString(), buf);
    }

    OverflowMode get() const
    {
        return value;
    }

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

    void set(const Field & x)
    {
        set(safeGet<const String &>(x));
    }

    void set(const String & x)
    {
        set(getChecksumAlgorithm(x));
    }

    void set(ReadBuffer & buf)
    {
        String x;
        readBinary(x, buf);
        set(x);
    }

    void write(WriteBuffer & buf) const
    {
        writeBinary(toString(), buf);
    }

    ChecksumAlgo get() const
    {
        return value;
    }

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

        throw Exception("invalid checksum algorithm value: " + ::DB::toString(static_cast<size_t>(value)), ErrorCodes::INVALID_CONFIG_PARAMETER);
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

        throw Exception("Unknown checksum algorithm: '" + s + "', must be one of 'xxh3', 'city128', 'crc32', 'crc64', 'none'", ErrorCodes::INVALID_CONFIG_PARAMETER);
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
        if (s == "lz4")
            return CompressionMethod::LZ4;
        if (s == "lz4hc")
            return CompressionMethod::LZ4HC;
        if (s == "zstd")
            return CompressionMethod::ZSTD;

        throw Exception("Unknown compression method: '" + s + "', must be one of 'lz4', 'lz4hc', 'zstd'", ErrorCodes::UNKNOWN_COMPRESSION_METHOD);
    }

    String toString() const
    {
        const char * strings[] = {nullptr, "lz4", "lz4hc", "zstd"};

        if (value < CompressionMethod::LZ4 || value > CompressionMethod::ZSTD)
            throw Exception("Unknown compression method", ErrorCodes::UNKNOWN_COMPRESSION_METHOD);

        return strings[static_cast<size_t>(value)];
    }

    void set(CompressionMethod x)
    {
        value = x;
        changed = true;
    }

    void set(const Field & x)
    {
        set(safeGet<const String &>(x));
    }

    void set(const String & x)
    {
        set(getCompressionMethod(x));
    }

    void set(ReadBuffer & buf)
    {
        String x;
        readBinary(x, buf);
        set(x);
    }

    void write(WriteBuffer & buf) const
    {
        writeBinary(toString(), buf);
    }

    CompressionMethod get() const
    {
        return value;
    }

private:
    CompressionMethod value;
};

/// The setting for executing distributed subqueries inside IN or JOIN sections.
enum class DistributedProductMode
{
    DENY = 0, /// Disable
    LOCAL, /// Convert to local query
    GLOBAL, /// Convert to global query
    ALLOW /// Enable
};

struct SettingDistributedProductMode
{
public:
    bool changed = false;

    SettingDistributedProductMode(DistributedProductMode x)
        : value(x)
    {}

    operator DistributedProductMode() const { return value; }
    SettingDistributedProductMode & operator=(DistributedProductMode x)
    {
        set(x);
        return *this;
    }

    static DistributedProductMode getDistributedProductMode(const String & s)
    {
        if (s == "deny")
            return DistributedProductMode::DENY;
        if (s == "local")
            return DistributedProductMode::LOCAL;
        if (s == "global")
            return DistributedProductMode::GLOBAL;
        if (s == "allow")
            return DistributedProductMode::ALLOW;

        throw Exception("Unknown distributed product mode: '" + s + "', must be one of 'deny', 'local', 'global', 'allow'",
                        ErrorCodes::UNKNOWN_DISTRIBUTED_PRODUCT_MODE);
    }

    String toString() const
    {
        const char * strings[] = {"deny", "local", "global", "allow"};
        if (value < DistributedProductMode::DENY || value > DistributedProductMode::ALLOW)
            throw Exception("Unknown distributed product mode", ErrorCodes::UNKNOWN_DISTRIBUTED_PRODUCT_MODE);
        return strings[static_cast<size_t>(value)];
    }

    void set(DistributedProductMode x)
    {
        value = x;
        changed = true;
    }

    void set(const Field & x)
    {
        set(safeGet<const String &>(x));
    }

    void set(const String & x)
    {
        set(getDistributedProductMode(x));
    }

    void set(ReadBuffer & buf)
    {
        String x;
        readBinary(x, buf);
        set(x);
    }

    void write(WriteBuffer & buf) const
    {
        writeBinary(toString(), buf);
    }

    DistributedProductMode get() const
    {
        return value;
    }

private:
    DistributedProductMode value;
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

    String toString() const
    {
        return value;
    }

    void set(const String & x)
    {
        value = x;
        changed = true;
    }

    void set(const Field & x)
    {
        set(safeGet<const String &>(x));
    }

    void set(ReadBuffer & buf)
    {
        String x;
        readBinary(x, buf);
        set(x);
    }

    void write(WriteBuffer & buf) const
    {
        writeBinary(value, buf);
    }

    String get() const
    {
        return value;
    }

private:
    String value;
};

} // namespace DB
