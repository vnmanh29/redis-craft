cmake_minimum_required(VERSION 3.10)
project(rdbparse CXX)

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Set build type
if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()

# Compiler flags
set(CMAKE_CXX_FLAGS_DEBUG "-O0 -g -D__XDEBUG__ -pg")
set(CMAKE_CXX_FLAGS_RELEASE "-O2 -DNDEBUG -fno-omit-frame-pointer")

# Platform-specific settings
if(APPLE)
  # macOS: no -lrt needed
elseif(UNIX)
  set(EXTRA_LIBS rt)
endif()

# Source files
file(GLOB SRC_FILES src/*.cc)
add_library(rdbparse STATIC ${SRC_FILES})

target_include_directories(rdbparse PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(rdbparse pthread ${EXTRA_LIBS})

# Clean target
set_directory_properties(PROPERTIES ADDITIONAL_MAKE_CLEAN_FILES "${CMAKE_BINARY_DIR}/lib")

# Optionally, enable warnings
if (CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
  target_compile_options(rdbparse PRIVATE
          -Wall -Wextra -Wsign-compare -Wno-unused-parameter -Wno-redundant-decls
          -Wwrite-strings -Wpointer-arith -Wreorder -Wswitch -Wsign-promo
          -Woverloaded-virtual -Wnon-virtual-dtor -Wno-missing-field-initializers
  )
endif()