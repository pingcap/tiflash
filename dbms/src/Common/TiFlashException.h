#pragma once

#include <Common/Exception.h>

#include <ext/singleton.h>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace DB
{

#define ERROR_CLASS_LIST \
    X(PageStorage)       \
    X(DeltaTree)         \
    X(DDL)               \
    X(Coprocessor)       \
    X(Table)             \
    X(Decimal)           \
    X(BroadcastJoin)

namespace ErrorClass
{
#define X(class_name) const std::string class_name = #class_name;
ERROR_CLASS_LIST
#undef X
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

    std::string standardName() const { return "FLASH:" + error_class + ":" + error_code; }
};

/// TiFlashErrorRegistry will registers and checks all errors when TiFlash startup
class TiFlashErrorRegistry : public ext::singleton<TiFlashErrorRegistry>
{
public:
    friend ext::singleton<TiFlashErrorRegistry>;

    static TiFlashError simpleGet(const std::string & error_class, const std::string & error_code)
    {
        auto & _instance = instance();
        auto error = _instance.get(error_class, error_code);
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

    std::vector<TiFlashError> allErrors() const
    {
        std::vector<TiFlashError> res;
        res.reserve(all_errors.size());
        for (auto error : all_errors)
        {
            res.push_back(error.second);
        }
        return res;
    }

protected:
    TiFlashErrorRegistry() { initialize(); }

private:
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
