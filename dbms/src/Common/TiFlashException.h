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

/// A central place for defining your error class and error code.
/// C(error_class, error...)
/// E(error_code, description, workaround, message_template)
///
/// Example:
///   C(Foo, E(Bar, "Blabla", "Do nothing", "Bar error"); E(Baz, "Abaaba", "Do something", "Baz error");)
///
/// Notice:
///   - Use clang-format to format your code
///   - Use semicolon(;) to split errors
///   - After adding an error, please execute `tiflash errgen <tics-dir>/errors.toml`
#define ERROR_CLASS_LIST                                                                                                             \
    C(PageStorage,                                                                                                                   \
        E(FileSizeNotMatch, "Some files' size don't match their metadata.",                                                          \
            "This is a critical error which should rarely occur, please contact with developer, \n"                                  \
            "better providing information about your cluster(log, topology information etc.).",                                      \
            "");)                                                                                                                    \
    C(DeltaTree,                                                                                                                     \
        E(Internal, "DeltaTree internal error.",                                                                                     \
            "Please contact with developer, \n"                                                                                      \
            "better providing information about your cluster(log, topology information etc.).",                                      \
            "");)                                                                                                                    \
    C(DDL,                                                                                                                           \
        E(MissingTable, "Table information is missing in TiFlash or TiKV.",                                                          \
            "This error will occur when there is difference of schema infomation between TiKV and TiFlash, \n"                       \
            "for example a table has been dropped in TiKV while hasn't been dropped in TiFlash yet(since DDL operation is "          \
            "asynchronized). \n"                                                                                                     \
            "TiFlash will keep retrying to synchronize all schemas, so you don't need to take it too serious. \n"                    \
            "If there are massive MissingTable errors, please contact with developer, \n"                                            \
            "better providing information about your cluster(log, topology information etc.).",                                      \
            "");                                                                                                                     \
        E(TableTypeNotMatch, "Table type in TiFlash is different from that in TiKV.",                                                \
            "This error will occur when there is difference of schema information between TiKV and TiFlash. \n"                      \
            "Please contact with developer, \n"                                                                                      \
            "better providing information about your cluster(log, topology information etc.).",                                      \
            "");                                                                                                                     \
        E(ExchangePartitionError, "EXCHANGE PARTITION error.",                                                                       \
            "Please contact with developer, \n"                                                                                      \
            "better providing information about your cluster(log, topology information etc.).",                                      \
            "");                                                                                                                     \
        E(Internal, "TiFlash DDL internal error.",                                                                                   \
            "Please contact with developer, \n"                                                                                      \
            "better providing information about your cluster(log, topology information etc.).",                                      \
            "");                                                                                                                     \
        E(StaleSchema, "Schema is stale and need to reload all schema.",                                                             \
            "This error will be recover by reload all schema automatically.",                                                        \
            "");)                                                                                                                    \
    C(Coprocessor,                                                                                                                   \
        E(BadRequest, "Bad TiDB coprocessor request.",                                                                               \
            "This error is usually caused by incorrect TiDB DAGRequest. \n"                                                          \
            "Please contact with developer, \n"                                                                                      \
            "better providing information about your cluster(log, topology information etc.).",                                      \
            "");                                                                                                                     \
        E(Unimplemented, "Some features are unimplemented.",                                                                         \
            "This error may caused by unmatched TiDB and TiFlash versions, \n"                                                       \
            "and should not occur in common case. \n"                                                                                \
            "Please contact with developer, \n"                                                                                      \
            "better providing information about your cluster(log, topology information etc.).",                                      \
            "");                                                                                                                     \
        E(Internal, "TiFlash Coprocessor internal error.",                                                                           \
            "Please contact with developer, \n"                                                                                      \
            "better providing information about your cluster(log, topology information etc.).",                                      \
            "");                                                                                                                     \
        E(MemoryLimitExceeded, "TiFlash memory limit exceeded.",                                                                     \
            "Please modify the config parameters 'max_memory_usage' and 'max_memory_usage_for_all_queries'.", "");)                  \
    C(Table,                                                                                                                         \
        E(SchemaVersionError, "Schema version of target table in TiFlash is different from that in query.",                          \
            "TiFlash will sync the newest schema from TiDB before processing every query. \n"                                        \
            "If there is a DDL operation performed as soon as your query was sent, this error may occur. \n"                         \
            "Please retry your query after a short time(about 30 seconds).",                                                         \
            "");                                                                                                                     \
        E(SyncError, "Schema synchronize error.",                                                                                    \
            "This is a critical error which should rarely occur, please contact with developer, \n"                                  \
            "better providing information about your cluster(log, topology information etc.).",                                      \
            "");                                                                                                                     \
        E(NotExists, "Table does not exist.",                                                                                        \
            "This error may occur when send query to TiFlash as soon as the target table is dropped or truncated. \n"                \
            "Please retry your query after a short time(about 30 seconds)",                                                          \
            "");)                                                                                                                    \
    C(Decimal,                                                                                                                       \
        E(Overflow, "Decimal value overflow.",                                                                                       \
            "This error will occur when TiFlash is trying to convert an value to decimal type that can't fit the value. \n"          \
            "It's usually caused by invalid DDL operation or invalid CAST expression, please check your SQL statement.",             \
            "");)                                                                                                                    \
    C(BroadcastJoin,                                                                                                                 \
        E(TooManyColumns, "Number of columns to read exceeds limit.",                                                                \
            "Please try to reduce your joined columns. \n"                                                                           \
            "If this error still remains, \n"                                                                                        \
            "please contact with developer, \n"                                                                                      \
            "better providing information about your cluster(log, topology information etc.).",                                      \
            "");                                                                                                                     \
        E(Internal, "Broadcast Join internal error.",                                                                                \
            "Please contact with developer, \n"                                                                                      \
            "better providing information about your cluster(log, topology information etc.).",                                      \
            "");)                                                                                                                    \
    C(Encryption,                                                                                                                    \
        E(Internal, "Encryption internal error.",                                                                                    \
            "Please contact with developer, \n"                                                                                      \
            "better providing information about your cluster(log, topology information etc.).",                                      \
            "");)                                                                                                                    \
    C(MPP,                                                                                                                           \
        E(Internal, "MPP internal error.",                                                                                           \
            "Please contact with developer, \n"                                                                                      \
            "better providing information about your cluster(log, topology information etc.).",                                      \
            "");)                                                                                                                    \
    C(Types, E(Truncated, "Data is truncated during conversion.", "", ""); E(WrongValue, "Input value is in wrong format", "", "");) \
    C(Expression, E(DivisionByZero, "Division by 0.", "", "");)

/// TiFlashError is core struct of standard error,
/// which contains all information about an error except message.
struct TiFlashError
{
    const std::string error_class;
    const std::string error_code;
    const std::string message_template;
    const std::string workaround;
    const std::string description;

    std::string standardName() const { return "FLASH:" + error_class + ":" + error_code; }
    bool is(const TiFlashError & other) const { return error_class == other.error_class && error_code == other.error_code; }
};

namespace Errors
{
#define C(class_name, ...)                \
    namespace class_name                  \
    {                                     \
    const std::string NAME = #class_name; \
    __VA_ARGS__                           \
    }
#define E(error_code, desc, workaround, message_template) \
    const TiFlashError error_code{NAME, #error_code, desc, workaround, message_template};

ERROR_CLASS_LIST
#undef C
#undef E
} // namespace Errors


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
