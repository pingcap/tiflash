include(${TiFlash_SOURCE_DIR}/cmake/dbms_glob_sources.cmake)
add_headers_and_sources(tiflash_aggregate_functions .)

list(REMOVE_ITEM tiflash_aggregate_functions_sources
    AggregateFunctionFactory.cpp
    AggregateFunctionCombinatorFactory.cpp
    AggregateFunctionState.cpp
    parseAggregateFunctionParameters.cpp
    FactoryHelpers.cpp
)

list(REMOVE_ITEM tiflash_aggregate_functions_headers
    IAggregateFunction.h
    IAggregateFunctionCombinator.h
    AggregateFunctionFactory.h
    AggregateFunctionCombinatorFactory.h
    AggregateFunctionState.h
    parseAggregateFunctionParameters.h
    FactoryHelpers.h
)

add_library(tiflash_aggregate_functions ${tiflash_aggregate_functions_sources})
target_link_libraries(tiflash_aggregate_functions dbms)
target_include_directories (tiflash_aggregate_functions BEFORE PRIVATE ${COMMON_INCLUDE_DIR})
