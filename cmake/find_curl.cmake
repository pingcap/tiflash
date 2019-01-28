find_package (CURL REQUIRED)

if (NOT CURL_FOUND)
  message (FATAL_ERROR "Curl Not Found!")
endif (NOT CURL_FOUND)

message (STATUS "Using CURL: ${CURL_INCLUDE_DIRS} : ${CURL_LIBRARIES}")
