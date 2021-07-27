# These files are located in separate library, because they are used by Yandex.Metrika code
#  in places when no dependency on whole "dbms" library is possible.

include(${TIFLASH_SOURCE_DIR}/cmake/dbms_glob_sources.cmake)

add_headers_and_sources(tiflash_common_stringutils .)

add_library(string_utils ${SPLIT_SHARED} ${tiflash_common_stringutils_headers} ${tiflash_common_stringutils_sources})
target_include_directories (string_utils PRIVATE ${DBMS_INCLUDE_DIR})
