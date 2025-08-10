cmake_minimum_required(VERSION 3.13)

project(redis-starter-cpp)

file(GLOB_RECURSE SOURCE_FILES src/*.cpp)

set(CMAKE_CXX_STANDARD 23) # Enable the C++23 standard
set(THREADS_PREFER_PTHREAD_FLAG ON)

find_package(Threads REQUIRED)
find_package(asio CONFIG REQUIRED)

add_subdirectory(3rd_party/rdbparse)

add_executable(server ${SOURCE_FILES})

target_link_libraries(server PRIVATE asio asio::asio)
target_link_libraries(server PRIVATE Threads::Threads)
target_link_libraries(server PRIVATE rdbparse)

target_include_directories(server PRIVATE src/resp)
target_include_directories(server PRIVATE src/)
target_include_directories(server PRIVATE 3rd_party/rdbparse/include)
target_include_directories(server PRIVATE 3rd_party/rdbparse/src)