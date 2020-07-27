#include <Common/TiFlashException.h>

#include <set>

namespace DB
{

void TiFlashErrorRegistry::initialize()
{
    // Used to check uniqueness of classes
    std::set<String> class_map;

#define REGISTER_ERROR_CLASS(class_name)                                                            \
    const String class_name = #class_name;                                                          \
    do                                                                                              \
    {                                                                                               \
        if (auto [_, took_place] = class_map.insert(class_name); took_place)                        \
            throw Exception("Error Class " #class_name " is duplicate, please check related code"); \
    } while (0)

    // Register error classes with macro REGISTER_ERROR_CLASS.
    // For example, REGISTER_ERROR_CLASS(Foo) will register a class named "Foo",
    // and the macro parameter Foo is a DB::String variable defined in current scope with value "Foo".
    // All class should be named in Pascal case(words start with upper case).
    REGISTER_ERROR_CLASS(DeltaTree);
    REGISTER_ERROR_CLASS(DDL);
    REGISTER_ERROR_CLASS(Coprocessor);

#undef REGISTER_ERROR_CLASS

    // Example:
    // registerError(MyClass, "Unimplemented",
    //     "This is a sample error"
    //     "which you can reference",
    //     "no need to workaround");
    registerError(Coprocessor, "", "", "");
}

} // namespace DB