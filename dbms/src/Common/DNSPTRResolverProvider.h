#pragma once


#include <memory>
#include <string>
#include <unordered_set>

namespace DB
{
struct DNSPTRResolver
{
    virtual ~DNSPTRResolver() = default;

    virtual std::unordered_set<std::string> resolve(const std::string & ip) = 0;

    virtual std::unordered_set<std::string> resolve_v6(const std::string & ip) = 0;
};
/*
 * Provides a ready-to-use DNSPTRResolver instance.
 * It hides 3rd party lib dependencies, handles initialization and lifetime.
 * Since `get` function is static, it can be called from any context. Including cached static functions.
 **/
class DNSPTRResolverProvider
{
public:
    static std::shared_ptr<DNSPTRResolver> get();
};
} // namespace DB
