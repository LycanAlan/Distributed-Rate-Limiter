cmake_minimum_required(VERSION 3.22)
project(rate_limiter CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

find_package(GTest REQUIRED)
find_package(PkgConfig REQUIRED)
# Debian's HiredisConfig.cmake defines no target, so go through pkg-config.
pkg_check_modules(HIREDIS REQUIRED IMPORTED_TARGET hiredis)

file(GLOB RLCORE_SOURCES CONFIGURE_DEPENDS ${CMAKE_SOURCE_DIR}/src/*.cpp)
list(REMOVE_ITEM RLCORE_SOURCES ${CMAKE_SOURCE_DIR}/src/main.cpp)
add_library(rlcore STATIC ${RLCORE_SOURCES})
target_include_directories(rlcore PUBLIC src)
target_compile_options(rlcore PRIVATE -Wall -Wextra -Werror)
target_link_libraries(rlcore PUBLIC PkgConfig::HIREDIS)

if(EXISTS ${CMAKE_SOURCE_DIR}/src/main.cpp)
  add_executable(gateway src/main.cpp)
  target_compile_options(gateway PRIVATE -Wall -Wextra -Werror)
  target_link_libraries(gateway rlcore)
endif()

# One test executable per tests/test_<area>.cpp file: build/test_<area>
enable_testing()
include(GoogleTest)
file(GLOB TEST_SOURCES CONFIGURE_DEPENDS ${CMAKE_SOURCE_DIR}/tests/test_*.cpp)
foreach(src ${TEST_SOURCES})
  get_filename_component(name ${src} NAME_WE)
  add_executable(${name} ${src})
  target_compile_options(${name} PRIVATE -Wall -Wextra)
  target_compile_definitions(${name} PRIVATE LUA_DIR="${CMAKE_SOURCE_DIR}/lua")
  target_link_libraries(${name} rlcore GTest::gtest_main)
  gtest_discover_tests(${name} DISCOVERY_TIMEOUT 30)
endforeach()
