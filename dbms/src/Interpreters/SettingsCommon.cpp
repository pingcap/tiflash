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
} // namespace DB
