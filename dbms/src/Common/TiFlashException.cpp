#include <Common/TiFlashException.h>

#include <set>

namespace DB
{

void TiFlashErrorRegistry::initialize()
{
    // Used to check uniqueness of classes
    std::set<std::string> all_classes;

    using namespace ErrorClass;

/// Register error classes, and check their uniqueness
#define X(class_name)                                                                              \
    if (auto [_, took_place] = all_classes.insert(class_name); !took_place)                        \
    {                                                                                              \
        (void)_;                                                                                   \
        throw Exception("Error Class " + class_name + " is duplicate, please check related code"); \
    }

    ERROR_CLASS_LIST
#undef X

    // Example:
    // registerError(MyClass, "Unimplemented",
    //     "This is a sample error"
    //     "which you can reference",
    //     "no need to workaround");
    registerError(PageStorage, "FileSizeNotMatch", //
        /* Description */ "Some files' size don't match their metadata.",
        /* Workaround */
        "This is a critical error which should rarely occur, please contact with developer, \n"
        "better providing information about your cluster(log, topology information etc.).");

    registerError(Table, "SchemaVersionError", //
        /* Description */ "Schema version of target table in TiFlash is different from that in query.",
        /* Workaround */
        "TiFlash will sync the newest schema from TiDB before processing every query. \n"
        "If there is a DDL operation performed as soon as your query was sent, this error may occur. \n"
        "Please retry your query after a short time(about 30 seconds).");

    registerError(Table, "SyncError", //
        /* Description */ "Schema synchronize error.",
        /* Workaround */
        "This is a critical error which should rarely occur, please contact with developer, \n"
        "better providing information about your cluster(log, topology information etc.).");

    registerError(Table, "NotExists", //
        /* Description */ "Table does not exist.",
        /* Workaround */
        "This error may occur when send query to TiFlash as soon as the target table is dropped or truncated. \n"
        "Please retry your query after a short time(about 30 seconds)");

    registerError(Decimal, "Overflow", //
        /* Description */ "Decimal value overflow.",
        /* Workaround */
        "This error will occur when TiFlash is trying to convert an value to decimal type that can't fit the value. \n"
        "It's usually caused by invalid DDL operation or invalid CAST expression, please check your SQL statement. ");

    registerError(DDL, "MissingTable", //
        /* Description */ "Table information is missing in TiFlash or TiKV.",
        /* Workaround */
        "This error will occur when there is difference of schema infomation between TiKV and TiFlash, \n"
        "for example a table has been dropped in TiKV while hasn't been dropped in TiFlash yet(since DDL operation is asynchronized). \n"
        "TiFlash will keep retrying to synchronize all schemas, so you don't need to take it too serious. \n"
        "If there are massive MissingTable errors, please contact with developer, \n"
        "better providing information about your cluster(log, topology information etc.).");

    registerError(DDL, "TableTypeNotMatch", //
        /* Description */ "Table type in TiFlash is different from that in TiKV.",
        /* Workaround */
        "This error will occur when there is difference of schema information between TiKV and TiFlash. \n"
        "Please contact with developer, \n"
        "better providing information about your cluster(log, topology information etc.).");

    registerError(DDL, "ExchangePartitionError", //
        /* Description */ "EXCHANGE PARTITION error.",
        /* Workaround */
        "Please contact with developer, \n"
        "better providing information about your cluster(log, topology information etc.).");

    registerError(DDL, "Internal", //
        /* Description */ "TiFlash DDL internal error.",
        /* Workaround */
        "Please contact with developer, \n"
        "better providing information about your cluster(log, topology information etc.).");

    registerError(Coprocessor, "BadRequest", //
        /* Description */ "Bad TiDB coprocessor request.",
        /* Workaround */
        "This error is usually caused by incorrect TiDB DAGRequest. \n"
        "Please contact with developer, \n"
        "better providing information about your cluster(log, topology information etc.).");

    registerError(Coprocessor, "Unimplemented", //
        /* Description */ "Some features are unimplemented.",
        /* Workaround */
        "This error may caused by unmatched TiDB and TiFlash versions, \n"
        "and should not occur in common case. \n"
        "Please contact with developer, \n"
        "better providing information about your cluster(log, topology information etc.).");

    registerError(Coprocessor, "Internal", //
        /* Description */ "TiFlash Coprocessor internal error.",
        /* Workaround */
        "Please contact with developer, \n"
        "better providing information about your cluster(log, topology information etc.).");

    registerError(BroadcastJoin, "TooManyColumns", //
        /* Description */ "Number of columns to read exceeds limit.",
        /* Workaround */
        "Please try to reduce your joined columns. \n"
        "If this error still remains, \n"
        "please contact with developer, \n"
        "better providing information about your cluster(log, topology information etc.).");

    registerError(BroadcastJoin, "Internal", //
        /* Description */ "Broadcast Join internal error.",
        /* Workaround */
        "Please contact with developer, \n"
        "better providing information about your cluster(log, topology information etc.).");
}

void TiFlashErrorRegistry::registerError(const std::string & error_class, const std::string & error_code, const std::string & description,
    const std::string & workaround, const std::string & message_template)
{
    TiFlashError error{error_class, error_code, description, workaround, message_template};
    if (all_errors.find({error_class, error_code}) == all_errors.end())
    {
        all_errors.emplace(std::make_pair(error_class, error_code), std::move(error));
    }
    else
    {
        throw Exception("TiFLashError: " + error_class + ":" + error_code + " has been registered.");
    }
}

void TiFlashErrorRegistry::registerErrorWithNumericCode(const std::string & error_class, int error_code, const std::string & description,
    const std::string & workaround, const std::string & message_template)
{
    std::string error_code_str = std::to_string(error_code);
    registerError(error_class, error_code_str, description, workaround, message_template);
}

// Standard error text with format:
// [{Component}:{ErrorClass}:{ErrorCode}] {message}
std::string TiFlashException::standardText() const
{
    std::string text{};
    if (!message().empty())
    {
        text.append("[");
        text.append("FLASH:" + error.error_class + ":" + error.error_code);
        text.append("] ");
        text.append(message());
    }
    return text;
}

} // namespace DB
