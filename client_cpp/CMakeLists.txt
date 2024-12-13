cmake_minimum_required(VERSION 3.10)

project(quantdata CXX)

set(CMAKE_CXX_STANDARD 17)

if(MSVC)
  # Windows下生成dll同时生成lib，用于其他项目链接
  set(CMAKE_WINDOWS_EXPORT_ALL_SYMBOLS ON)
endif()

find_package(fmt CONFIG REQUIRED)
include_directories("${CMAKE_CURRENT_SOURCE_DIR}/include")
include_directories("${CMAKE_CURRENT_SOURCE_DIR}/third_party")

find_library(duckdb duckdb REQUIRED)

add_library(qdduckdb SHARED
  "${CMAKE_CURRENT_SOURCE_DIR}/src/quantdata.duckdb.cpp"
  "${CMAKE_CURRENT_SOURCE_DIR}/src/datetime.cpp"
  "${CMAKE_CURRENT_SOURCE_DIR}/third_party/duckdb/duckdb.hpp"
)
target_link_libraries(qdduckdb PUBLIC ${duckdb} PUBLIC fmt::fmt)


find_package(mongocxx REQUIRED)
find_package(bsoncxx REQUIRED)
add_library(qdmongodb SHARED
  "${CMAKE_CURRENT_SOURCE_DIR}/src/quantdata.mongodb.cpp"
  "${CMAKE_CURRENT_SOURCE_DIR}/src/datetime.cpp"
)
target_link_libraries(qdmongodb
  PUBLIC mongo::bsoncxx_shared
  PUBLIC mongo::mongocxx_shared
  PUBLIC fmt::fmt)


find_package(yaml-cpp CONFIG REQUIRED)
add_library(quantdata SHARED
  "${CMAKE_CURRENT_SOURCE_DIR}/src/quantdata.duckdb.cpp"
  "${CMAKE_CURRENT_SOURCE_DIR}/third_party/duckdb/duckdb.hpp"
  "${CMAKE_CURRENT_SOURCE_DIR}/src/quantdata.mongodb.cpp"
  "${CMAKE_CURRENT_SOURCE_DIR}/src/datetime.cpp"
  "${CMAKE_CURRENT_SOURCE_DIR}/src/quantdata.cpp"
)
target_link_libraries(quantdata
  PUBLIC ${duckdb}
  PUBLIC mongo::bsoncxx_shared
  PUBLIC mongo::mongocxx_shared
  PUBLIC yaml-cpp::yaml-cpp
  PUBLIC fmt::fmt)


# For CTest
enable_testing()

# test duckdb
add_executable(test_duckdb "${CMAKE_CURRENT_SOURCE_DIR}/test/test_duckdb.cpp")
target_link_libraries(test_duckdb qdduckdb)
add_test(NAME "test_duckdb" COMMAND "test_duckdb" WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}")
set_tests_properties("test_duckdb" PROPERTIES FAIL_REGULAR_EXPRESSION "error")

# test mongodb
add_executable(test_mongodb "${CMAKE_CURRENT_SOURCE_DIR}/test/test_mongodb.cpp")
target_link_libraries(test_mongodb qdmongodb)
add_test(NAME "test_mongodb" COMMAND "test_mongodb" WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}")
set_tests_properties("test_mongodb" PROPERTIES FAIL_REGULAR_EXPRESSION "error")

# test datetime
add_executable(test_datetime "${CMAKE_CURRENT_SOURCE_DIR}/test/test_datetime.cpp" "${CMAKE_CURRENT_SOURCE_DIR}/src/datetime.cpp")
target_link_libraries(test_datetime)
add_test(NAME "test_datetime" COMMAND "test_datetime" WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}")

# test quantdata
add_executable(test_quantdata "${CMAKE_CURRENT_SOURCE_DIR}/test/test_quantdata.cpp")
target_link_libraries(test_quantdata quantdata)
add_test(NAME "test_quantdata" COMMAND "test_quantdata" WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}")
