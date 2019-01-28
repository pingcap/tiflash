find_package (Poco COMPONENTS Foundation REQUIRED)

set(Poco_Foundation_LIBRARY PocoFoundation)

message(STATUS "Using Poco: ${Poco_INCLUDE_DIRS} : ${Poco_Foundation_LIBRARY},${Poco_VERSION}")
