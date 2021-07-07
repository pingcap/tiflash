#pragma once

#include <string>

#include <Common/BitHelpers.h>
#include <IO/WriteBufferFromVector.h>
#include <common/StringRef.h>

namespace DB
{

/** Writes the data to a string.
  * Note: before using the resulting string, destroy this object.
  * Use 15 as initial_size to fit the local buffer of gcc's std::string.
  */
using WriteBufferFromString = WriteBufferFromVector<std::string, 15>;

namespace detail
{
/// For correct order of initialization.
class StringHolder
{
protected:
    std::string value;
};
}

/// Creates the string by itself and allows to get it.
class WriteBufferFromOwnString : public detail::StringHolder, public WriteBufferFromString
{
public:
    WriteBufferFromOwnString() : WriteBufferFromString(value) {}

    StringRef stringRef() const { return isFinished() ? StringRef(value) : StringRef(value.data(), pos - value.data()); }

    std::string & str()
    {
        finalize();
        return value;
    }

    /// Can't reuse WriteBufferFromOwnString after releaseStr
    std::string releaseStr()
    {
        finalize();
        return std::move(value);
    }
};

}
