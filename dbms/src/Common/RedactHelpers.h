#pragma once

#include <atomic>
#include <ostream>
#include <Storages/Transaction/Types.h>

class Redact
{
public:
    static void setRedactLog(bool v);

    static std::string handleToDebugString(const DB::HandleID handle);
    static std::string keyToDebugString(const char * key, size_t size);

    static std::string keyToHexString(const char *key, size_t size);

    static void keyToDebugString(const char * key, size_t size, std::ostream & oss);

protected:
    Redact() {}

private:
    // Log user data to log only when this flag is set to false.
    static std::atomic<bool> REDACT_LOG;
};
