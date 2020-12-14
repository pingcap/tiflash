#pragma once

#include <Storages/Transaction/Types.h>

#include <atomic>
#include <ostream>

namespace DB
{
class FieldVisitorToDebugString;
}

class Redact
{
public:
    static void setRedactLog(bool v);

    static std::string handleToDebugString(const DB::HandleID handle);
    static std::string keyToDebugString(const char * key, size_t size);

    static std::string keyToHexString(const char * key, size_t size);

    static void keyToDebugString(const char * key, size_t size, std::ostream & oss);

    friend class DB::FieldVisitorToDebugString;

protected:
    Redact() {}

private:
    // Log user data to log only when this flag is set to false.
    static std::atomic<bool> REDACT_LOG;
};
