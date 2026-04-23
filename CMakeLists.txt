cmake_minimum_required(VERSION 3.10)
project(bookstore LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    add_compile_options(-Wall -Wextra -O2)
endif()

file(GLOB_RECURSE SOURCES "src/*.cpp")

add_executable(code ${SOURCES})
