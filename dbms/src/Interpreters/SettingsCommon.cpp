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

#include <Interpreters/SettingsCommon.h>

#include <magic_enum.hpp>

namespace DB
{
template <typename IntType>
SettingInt<IntType>::SettingInt(const SettingInt & setting)
{
    value.store(setting.value.load());
}

template <typename IntType>
String SettingInt<IntType>::toString() const
{
    return DB::toString(value.load());
}

template <typename IntType>
void SettingInt<IntType>::set(IntType x)
{
    value.store(x);
    changed = true;
}

template <typename IntType>
void SettingInt<IntType>::set(const Field & x)
{
    set(applyVisitor(FieldVisitorConvertToNumber<IntType>(), x));
}

template <typename IntType>
void SettingInt<IntType>::set(const String & x)
{
    set(parse<IntType>(x));
}

template <typename IntType>
void SettingInt<IntType>::set(ReadBuffer & buf)
{
    IntType x = 0;
    readVarT(x, buf);
    set(x);
}

template <typename IntType>
IntType SettingInt<IntType>::get() const
{
    return value.load();
}

template <typename IntType>
void SettingInt<IntType>::write(WriteBuffer & buf) const
{
    writeVarT(value.load(), buf);
}

template <>
void SettingInt<bool>::set(const String & x)
{
    if (x.size() == 1)
    {
        if (x[0] == '0')
            set(false);
        else if (x[0] == '1')
            set(true);
        else
            throw Exception("Cannot parse bool from string '" + x + "'", ErrorCodes::CANNOT_PARSE_BOOL);
    }
    else
    {
        ReadBufferFromString buf(x);
        if (checkStringCaseInsensitive("true", buf))
            set(true);
        else if (checkStringCaseInsensitive("false", buf))
            set(false);
        else
            throw Exception("Cannot parse bool from string '" + x + "'", ErrorCodes::CANNOT_PARSE_BOOL);
    }
}

template <>
void SettingInt<bool>::set(ReadBuffer & buf)
{
    UInt64 x = 0;
    readVarT(x, buf);
    set(x);
}

template <>
void SettingInt<bool>::write(WriteBuffer & buf) const
{
    UInt64 val = value.load();
    writeVarT(val, buf);
}
template struct SettingInt<UInt64>;
template struct SettingInt<Int64>;
template struct SettingInt<bool>;

TaskQueueType SettingTaskQueueType::getTaskQueueType(const String & s)
{
    auto value = magic_enum::enum_cast<TaskQueueType>(Poco::toUpper(s));
    RUNTIME_CHECK_MSG(value, "Unknown task queue type: '{}'", s);
    return *value;
}

String SettingTaskQueueType::toString() const
{
    return String(magic_enum::enum_name(value));
}

struct SettingMemoryLimit::ToStringVisitor
{
    String operator()(UInt64 x) const { return DB::toString(x); }
    String operator()(double x) const { return DB::toString(x); }
};

SettingMemoryLimit::SettingMemoryLimit(UInt64 bytes)
    : value(bytes)
{}
SettingMemoryLimit::SettingMemoryLimit(double percent)
    : value(percent)
{}
SettingMemoryLimit::SettingMemoryLimit(const SettingMemoryLimit & setting)
{
    value = setting.value;
}
SettingMemoryLimit & SettingMemoryLimit::operator=(UInt64OrDouble x)
{
    set(x);
    return *this;
}
SettingMemoryLimit & SettingMemoryLimit::operator=(const SettingMemoryLimit & setting)
{
    set(setting.value);
    return *this;
}

void SettingMemoryLimit::set(UInt64OrDouble x)
{
    value = x;
    changed = true;
}
void SettingMemoryLimit::set(UInt64 x)
{
    value = x;
    changed = true;
}
void SettingMemoryLimit::set(double x)
{
    if (x < 0.0 || x >= 1.0)
        throw Exception(
            "Memory limit (in double) should be in range [0.0, 1.0), it means a ratio of total RAM, or you can set it "
            "in UInt64, which means the limit bytes.",
            ErrorCodes::INVALID_CONFIG_PARAMETER);
    value = x;
    changed = true;
}
void SettingMemoryLimit::set(const Field & x)
{
    if (x.getType() == Field::Types::UInt64)
    {
        set(safeGet<UInt64>(x));
    }
    else if (x.getType() == Field::Types::Float64)
    {
        set(safeGet<Float64>(x));
    }
    else
        throw Exception(
            std::string("Bad type of setting. Expected UInt64 or Float64, got ") + x.getTypeName(),
            ErrorCodes::TYPE_MISMATCH);
}
void SettingMemoryLimit::set(const String & x)
{
    if (x.find('.') != std::string::npos)
        set(parse<double>(x));
    else
        set(parse<UInt64>(x));
}
void SettingMemoryLimit::set(ReadBuffer & buf)
{
    String x;
    readBinary(x, buf);
    set(x);
}

String SettingMemoryLimit::toString() const
{
    return std::visit(ToStringVisitor(), value);
}
void SettingMemoryLimit::write(WriteBuffer & buf) const
{
    writeBinary(toString(), buf);
}
SettingMemoryLimit::UInt64OrDouble SettingMemoryLimit::get() const
{
    return value;
}

UInt64 SettingMemoryLimit::getActualBytes(UInt64 total_ram) const
{
    return std::visit(
        [&](auto && arg) -> UInt64 {
            using T = std::decay_t<decltype(arg)>;
            if constexpr (std::is_same_v<T, UInt64>)
            {
                return arg;
            }
            else if constexpr (std::is_same_v<T, double>)
            {
                return total_ram * arg;
            }
        },
        get());
}

} // namespace DB
