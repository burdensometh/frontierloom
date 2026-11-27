cmake_minimum_required(VERSION 3.20)
project(FrontierLoom VERSION 1.0.0 LANGUAGES CXX)

if(NOT CMAKE_SYSTEM_NAME STREQUAL "Linux")
    message(FATAL_ERROR "FrontierLoom uses Linux worker processes and checkpoint primitives")
endif()

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
option(FRONTIERLOOM_SANITIZERS "Enable AddressSanitizer and UndefinedBehaviorSanitizer" OFF)

add_library(frontier_core
    graph/graph.cpp
    graph/manifest.cpp
    graph/partition.cpp
    graph/distribution.cpp
    kernel/program.cpp
    kernel/contract.cpp
    kernel/reference.cpp
    mailbox/batch.cpp
    mailbox/exchange.cpp
    barrier/barrier.cpp
    checkpoint/snapshot.cpp
    checkpoint/audit.cpp
    checkpoint/store.cpp
    workers/wire.cpp
    workers/process.cpp
    workers/pool.cpp
    workers/engine.cpp
    workers/metrics.cpp
    workers/result.cpp
    workers/settings.cpp
)
target_include_directories(frontier_core PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}")
target_compile_definitions(frontier_core PRIVATE _GNU_SOURCE)
target_compile_options(frontier_core PUBLIC -Wall -Wextra -Wpedantic -Wconversion -Wshadow)
if(FRONTIERLOOM_SANITIZERS)
    target_compile_options(frontier_core PUBLIC -fsanitize=address,undefined -fno-omit-frame-pointer)
    target_link_options(frontier_core PUBLIC -fsanitize=address,undefined)
endif()

add_executable(frontierloom app/main.cpp app/inspection.cpp)
target_link_libraries(frontierloom PRIVATE frontier_core)

add_executable(frontier-bench app/bench.cpp)
target_link_libraries(frontier-bench PRIVATE frontier_core)

include(CTest)
if(BUILD_TESTING)
    add_executable(frontier-tests tests/main.cpp)
    target_link_libraries(frontier-tests PRIVATE frontier_core)
    foreach(suite graph kernel exchange snapshot shard engine)
        add_test(NAME "${suite}"
            COMMAND frontier-tests "${suite}" "${CMAKE_CURRENT_SOURCE_DIR}/samples")
        set_tests_properties("${suite}" PROPERTIES TIMEOUT 45)
    endforeach()
endif()
