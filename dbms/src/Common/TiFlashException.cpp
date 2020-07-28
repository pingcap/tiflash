#include <Common/TiFlashException.h>

#include <set>

namespace DB
{

void TiFlashErrorRegistry::initialize()
{
    // Used to check uniqueness of classes
    std::set<std::string> all_classes;

#define REGISTER_ERROR_CLASS(class_name)                                                            \
    const std::string class_name = #class_name;                                                     \
    do                                                                                              \
    {                                                                                               \
        if (auto [_, took_place] = all_classes.insert(class_name); took_place)                      \
            throw Exception("Error Class " #class_name " is duplicate, please check related code"); \
    } while (0)

    // Register error classes with macro REGISTER_ERROR_CLASS.
    // For example, REGISTER_ERROR_CLASS(Foo) will register a class named "Foo",
    // and the macro parameter Foo is a DB::std::string variable defined in current scope with value "Foo".
    // All class should be named in Pascal case(words start with upper case).
    REGISTER_ERROR_CLASS(PageStorage);
    REGISTER_ERROR_CLASS(DeltaTree);
    REGISTER_ERROR_CLASS(DDL);
    REGISTER_ERROR_CLASS(Coprocessor);
    REGISTER_ERROR_CLASS(TableSchema);

#undef REGISTER_ERROR_CLASS

    // Example:
    // registerError(MyClass, "Unimplemented",
    //     "This is a sample error"
    //     "which you can reference",
    //     "no need to workaround");
    registerError(PageStorage, "FileSizeNotMatch", //
        /* Description */ "Some files' size don't match their metadata.",
        /* Workaround */
        "This is a critical error which should rarely occur, please report it to https://asktug.com, "
        "better with information about your TiFlash log and environment");
    
    registerError(TableSchema, "SchemaVersionError", //
        /* Description */ "Schema version of target table in TiFlash is different from that in query.",
        /* Workaround */
        "TiFlash will sync the newest schema from TiDB before processing every query. "
        "If there is a DDL operation performed as soon as your query was sent, this error may occur. "
        "Please retry your query after a short time(about 30 seconds).");
}

void TiFlashErrorRegistry::registerError(
    const std::string & error_class, const std::string & error_code, const std::string & description, const std::string & workaround)
{
    TiFlashError error{error_class, error_code, description, workaround};
    if (all_errors.find({error_class, error_code}) == all_errors.end())
    {
        all_errors.emplace(std::make_pair(error_class, error_code), std::move(error));
    }
    else
    {
        throw Exception("TiFLashError: " + error_class + ":" + error_code + " has been registered.");
    }
}

void TiFlashErrorRegistry::registerErrorWithNumericCode(
    const std::string & error_class, int error_code, const std::string & description, const std::string & workaround)
{
    std::string error_code_str = std::to_string(error_code);
    registerError(error_class, error_code_str, description, workaround);
}

} // namespace DB