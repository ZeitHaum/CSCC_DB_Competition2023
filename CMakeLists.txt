cmake_minimum_required(VERSION 3.16)
project(RMDB)

set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

# add_definitions(-DNDEBUG) #是否禁用assert.

# set(CMAKE_BUILD_TYPE "Release")
set(CMAKE_BUILD_TYPE "Debug")

set(CMAKE_CXX_STANDARD 17)
# set(CMAKE_CXX_FLAGS "-Wall -O0 -g -ggdb3")
# set(CMAKE_CXX_FLAGS "-Wall -O0 -g -ggdb3 -fsanitize=thread")
# set(CMAKE_CXX_FLAGS "-Wall -O0 -g -ggdb3 -ltsan -fsanitize=thread")
# set(CMAKE_CXX_FLAGS "-Wall -O0 -g -ggdb3 -fsanitize=undefined")
# set(CMAKE_CXX_FLAGS "-Wall -O0 -g -ggdb3 -fsanitize=address -fsanitize-recover=address")
# set(CMAKE_CXX_FLAGS "-Wall -O2 -g -ggdb3")
set(CMAKE_CXX_FLAGS "-Wall -O3")

# set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -O0 -g")
# set(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} -O0 -g")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -O3")
set(CMAKE_C_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -O3")

enable_testing()
add_subdirectory(src)
add_subdirectory(deps)
