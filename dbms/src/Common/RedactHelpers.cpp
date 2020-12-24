#include <Common/RedactHelpers.h>
#include <Common/hex.h>
#include <IO/WriteHelpers.h>
#include <common/Types.h>
#include <pingcap/RedactHelpers.h>

#include <iomanip>

std::atomic<bool> Redact::REDACT_LOG = false;

void Redact::setRedactLog(bool v)
{
    pingcap::Redact::setRedactLog(v); // set redact flag for client-c
    Redact::REDACT_LOG.store(v, std::memory_order_relaxed);
}

std::string Redact::handleToDebugString(const DB::HandleID handle)
{
    if (Redact::REDACT_LOG.load(std::memory_order_relaxed))
        return "?";

    // Encode as string
    return DB::toString(handle);
}

std::string Redact::keyToHexString(const char * key, size_t size)
{
    // Encode as upper hex string
    std::string buf(size * 2, '\0');
    char * pos = buf.data();
    for (size_t i = 0; i < size; ++i)
    {
        writeHexByteUppercase((UInt8)(key[i]), pos);
        pos += 2;
    }
    return buf;
}

std::string Redact::keyToDebugString(const char * key, const size_t size)
{
    if (Redact::REDACT_LOG.load(std::memory_order_relaxed))
        return "?";

    return Redact::keyToHexString(key, size);
}

void Redact::keyToDebugString(const char * key, const size_t size, std::ostream & oss)
{
    if (Redact::REDACT_LOG.load(std::memory_order_relaxed))
    {
        oss << "?";
        return;
    }

    // Encode as upper hex string
    const auto flags = oss.flags();
    oss << std::uppercase << std::setfill('0') << std::hex;
    for (size_t i = 0; i < size; ++i)
    {
        // width need to be set for each output (https://stackoverflow.com/questions/405039/permanent-stdsetw)
        oss << std::setw(2) << Int32(UInt8(key[i]));
    }
    oss.flags(flags); // restore flags
}
