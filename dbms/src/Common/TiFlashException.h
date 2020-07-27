#pragma once

#include <Common/Exception.h>
#include <Core/Types.h>

#include <map>
#include <memory>
#include <optional>

namespace DB
{

/// TiFlashError is core struct of standard error,
/// which contains all information about an error except message.
struct TiFlashError
{
    const String error_class;
    const String error_code;
    const String description;
    const String workaround;
};

/// TiFlashErrorRegistry will registers and checks all errors when TiFlash startup
class TiFlashErrorRegistry
{
public:
    // Meyer's Singleton, thread safe after C++11
    static TiFlashErrorRegistry & getInstance()
    {
        static TiFlashErrorRegistry registry;
        return registry;
    }

    std::optional<TiFlashError> get(const String & error_class, const String & error_code) const;

    std::optional<TiFlashError> get(const String & error_class, const Int32 & error_code) const;

private:
    TiFlashErrorRegistry() {}

    void registerError(const String & error_class, const String & error_code, const String & description, const String & workaround);

    void registerErrorWithNumericCode(
        const String & error_class, const Int32 & error_code, const String & description, const String & workaround);

    void initialize();

private:
    std::map<std::pair<String, String>, TiFlashError> all_errors;
};

/// TiFlashException implements TiDB's standardized error.
/// See https://github.com/pingcap/tidb/blob/master/docs/design/2020-05-08-standardize-error-codes-and-messages.md
class TiFlashException : public Exception
{
public:
    TiFlashException(const String & _msg, const TiFlashError & _error) : msg(_msg), error(_error) {}

private:
    String msg;
    TiFlashError error;
};

} // namespace DB
