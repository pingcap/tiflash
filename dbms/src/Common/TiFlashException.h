#pragma once

#include <Common/Exception.h>

#include <map>
#include <memory>
#include <optional>
#include <string>

namespace DB
{

namespace ErrorClass
{
const std::string PageStorage = "PageStorage";
const std::string DeltaTree = "DeltaTree";
const std::string DDL = "DDL";
const std::string Coprocessor = "Coprocessor";

const std::string Table = "Table";
const std::string Decimal = "Decimal";
const std::string BroadcastJoin = "BroadcastJoin";
} // namespace ErrorClass

/// TiFlashError is core struct of standard error,
/// which contains all information about an error except message.
struct TiFlashError
{
    const std::string error_class;
    const std::string error_code;
    const std::string description;
    const std::string workaround;
    const std::string message_template;
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

    static TiFlashError simpleGet(const std::string & error_class, const std::string & error_code)
    {
        auto instance = getInstance();
        auto error = instance.get(error_class, error_code);
        if (error.has_value())
        {
            return error.value();
        }
        else
        {
            throw Exception("Unregistered TiFlashError: FLASH:" + error_class + ":" + error_code);
        }
    }

    static TiFlashError simpleGet(const std::string & error_class, int error_code)
    {
        return simpleGet(error_class, std::to_string(error_code));
    }

    std::optional<TiFlashError> get(const std::string & error_class, const std::string & error_code) const
    {
        auto error = all_errors.find({error_class, error_code});
        if (error != all_errors.end())
        {
            return error->second;
        }
        else
        {
            return {};
        }
    }

    std::optional<TiFlashError> get(const std::string & error_class, int error_code) const
    {
        return get(error_class, std::to_string(error_code));
    }

private:
    TiFlashErrorRegistry() { initialize(); }

    void registerError(const std::string & error_class, const std::string & error_code, const std::string & description,
        const std::string & workaround, const std::string & message_template = "");

    void registerErrorWithNumericCode(const std::string & error_class, int error_code, const std::string & description,
        const std::string & workaround, const std::string & message_template = "");

    void initialize();

private:
    std::map<std::pair<std::string, std::string>, TiFlashError> all_errors;
};

/// TiFlashException implements TiDB's standardized error.
/// See https://github.com/pingcap/tidb/blob/master/docs/design/2020-05-08-standardize-error-codes-and-messages.md
class TiFlashException : public Exception
{
public:
    TiFlashException(const std::string & _msg, const TiFlashError & _error) : Exception(_msg), error(_error) {}

    const char * name() const throw() override { return "DB::TiFlashException"; }
    const char * className() const throw() override { return "DB::TiFlashException"; }

    TiFlashError getError() const { return error; }

    std::string standardText() const;

private:
    TiFlashError error;
};

} // namespace DB
