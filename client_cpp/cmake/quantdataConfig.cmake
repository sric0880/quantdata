include(CMakeFindDependencyMacro)
# https://stackoverflow.com/questions/64846805/how-do-i-specify-an-optional-dependency-in-a-cmake-package-configuration-file
# find_dependency() is used only for (initially) REQUIRED packages.
find_package(fmt CONFIG)
find_package(yaml-cpp CONFIG)
find_package(mongocxx)
find_package(bsoncxx)
find_package(duckdb)
include("${CMAKE_CURRENT_LIST_DIR}/quantdata_Targets.cmake")