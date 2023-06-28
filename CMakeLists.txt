cmake_minimum_required(VERSION 3.24)
project(miniredis)

set(CMAKE_CXX_STANDARD 17)

# file sources
set(SOURCES
        avl.cpp
        avl.h
        common.h
        hashtable.cpp
        hashtable.h
        heap.cpp
        heap.h
        list.h
        server.cpp
        thread_pool.cpp
        thread_pool.h
        zset.cpp
        zset.h
        )

# sever executable
add_executable(miniredis ${SOURCES})
