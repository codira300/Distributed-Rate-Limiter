cmake_minimum_required(VERSION 3.24)

project(distributed_rate_limiter VERSION 1.0.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

option(RL_BUILD_TESTS "Build tests" ON)
option(RL_BUILD_BENCHMARKS "Build benchmark tools" ON)
option(RL_ENABLE_WARNINGS "Enable compiler warnings" ON)
option(RL_ENABLE_SANITIZERS "Enable ASan and UBSan" OFF)
option(RL_ENABLE_TSAN "Enable ThreadSanitizer" OFF)
option(RL_WARNINGS_AS_ERRORS "Treat warnings as errors" OFF)

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")

find_package(Threads REQUIRED)
find_package(nlohmann_json QUIET)

if(NOT nlohmann_json_FOUND)
    add_library(nlohmann_json INTERFACE)
    target_include_directories(nlohmann_json INTERFACE "${CMAKE_CURRENT_SOURCE_DIR}/third_party/nlohmann")
    add_library(nlohmann_json::nlohmann_json ALIAS nlohmann_json)
endif()

find_package(PkgConfig QUIET)
if(PkgConfig_FOUND)
    pkg_check_modules(HIREDIS QUIET hiredis)
endif()

if(NOT HIREDIS_FOUND)
    find_path(HIREDIS_INCLUDE_DIR hiredis/hiredis.h)
    find_library(HIREDIS_LIBRARY NAMES hiredis)
    if(NOT HIREDIS_INCLUDE_DIR OR NOT HIREDIS_LIBRARY)
        message(FATAL_ERROR "hiredis was not found. Install hiredis (headers and library) or set CMAKE_PREFIX_PATH.")
    endif()
    add_library(hiredis::hiredis INTERFACE IMPORTED)
    target_include_directories(hiredis::hiredis INTERFACE "${HIREDIS_INCLUDE_DIR}")
    target_link_libraries(hiredis::hiredis INTERFACE "${HIREDIS_LIBRARY}")
else()
    add_library(hiredis::hiredis INTERFACE IMPORTED)
    target_include_directories(hiredis::hiredis INTERFACE ${HIREDIS_INCLUDE_DIRS})
    target_link_libraries(hiredis::hiredis INTERFACE ${HIREDIS_LIBRARIES})
    target_link_directories(hiredis::hiredis INTERFACE ${HIREDIS_LIBRARY_DIRS})
endif()

add_library(rl_httplib INTERFACE)
target_include_directories(rl_httplib INTERFACE "${CMAKE_CURRENT_SOURCE_DIR}/third_party/httplib")
if(WIN32)
    target_compile_definitions(rl_httplib INTERFACE _WIN32_WINNT=0x0A00 NOMINMAX WIN32_LEAN_AND_MEAN)
endif()

set(TOKEN_BUCKET_LUA_PATH "${CMAKE_CURRENT_SOURCE_DIR}/scripts/token_bucket.lua")
set(SLIDING_WINDOW_LUA_PATH "${CMAKE_CURRENT_SOURCE_DIR}/scripts/sliding_window.lua")
set_property(DIRECTORY APPEND PROPERTY CMAKE_CONFIGURE_DEPENDS
    "${TOKEN_BUCKET_LUA_PATH}"
    "${SLIDING_WINDOW_LUA_PATH}"
)
file(READ "${TOKEN_BUCKET_LUA_PATH}" TOKEN_BUCKET_LUA)
file(READ "${SLIDING_WINDOW_LUA_PATH}" SLIDING_WINDOW_LUA)
configure_file(
    "${CMAKE_CURRENT_SOURCE_DIR}/src/redis/lua_scripts.hpp.in"
    "${CMAKE_CURRENT_BINARY_DIR}/generated/lua_scripts.hpp"
    @ONLY
)

function(rl_enable_warnings target)
    if(NOT RL_ENABLE_WARNINGS)
        return()
    endif()
    if(MSVC)
        target_compile_options(${target} PRIVATE /W4 /permissive-)
        if(RL_WARNINGS_AS_ERRORS)
            target_compile_options(${target} PRIVATE /WX)
        endif()
    else()
        target_compile_options(${target} PRIVATE
            -Wall -Wextra -Wpedantic -Wshadow -Wconversion -Wsign-conversion
            -Wnon-virtual-dtor -Wold-style-cast
        )
        if(RL_WARNINGS_AS_ERRORS)
            target_compile_options(${target} PRIVATE -Werror)
        endif()
    endif()
endfunction()

if(RL_ENABLE_SANITIZERS)
    if(MSVC)
        message(FATAL_ERROR "RL_ENABLE_SANITIZERS requires GCC or Clang")
    endif()
    add_compile_options(-fsanitize=address,undefined -fno-omit-frame-pointer)
    add_link_options(-fsanitize=address,undefined)
endif()

if(RL_ENABLE_TSAN)
    if(MSVC)
        message(FATAL_ERROR "RL_ENABLE_TSAN requires GCC or Clang")
    endif()
    add_compile_options(-fsanitize=thread -fno-omit-frame-pointer)
    add_link_options(-fsanitize=thread)
endif()

add_library(rl_core STATIC
    src/policy.cpp
    src/service/rate_limiter_registry.cpp
    src/algorithms/token_bucket.cpp
    src/algorithms/sliding_window.cpp
    src/redis/redis_client.cpp
    src/observability/metrics.cpp
    src/observability/logger.cpp
)

target_include_directories(rl_core
    PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/include"
    PRIVATE "${CMAKE_CURRENT_BINARY_DIR}/generated"
)
target_link_libraries(rl_core PUBLIC hiredis::hiredis Threads::Threads)
if(WIN32)
    target_link_libraries(rl_core PUBLIC ws2_32)
endif()
rl_enable_warnings(rl_core)

add_library(rl_service STATIC
    src/config/policy_registry.cpp
    src/config/service_config.cpp
    src/service/rate_limiter_service.cpp
)

target_include_directories(rl_service
    PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/include"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/src"
)
target_link_libraries(rl_service PUBLIC rl_core nlohmann_json::nlohmann_json)
rl_enable_warnings(rl_service)

add_executable(rl-server
    src/cli/server_main.cpp
    src/transport/http_server.cpp
)
target_include_directories(rl-server PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/src")
target_link_libraries(rl-server PRIVATE rl_service rl_httplib nlohmann_json::nlohmann_json)
rl_enable_warnings(rl-server)

add_executable(rl-lb
    src/cli/lb_main.cpp
    src/transport/load_balancer.cpp
)
target_include_directories(rl-lb PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/src")
target_link_libraries(rl-lb PRIVATE rl_httplib nlohmann_json::nlohmann_json Threads::Threads)
if(WIN32)
    target_link_libraries(rl-lb PRIVATE ws2_32)
endif()
rl_enable_warnings(rl-lb)

add_executable(rl-demo demo/three_server_demo.cpp)
target_link_libraries(rl-demo PRIVATE rl_core)
rl_enable_warnings(rl-demo)

if(RL_BUILD_BENCHMARKS)
    add_executable(rl-bench benchmarks/bench.cpp)
    target_link_libraries(rl-bench PRIVATE rl_httplib nlohmann_json::nlohmann_json Threads::Threads)
    if(WIN32)
        target_link_libraries(rl-bench PRIVATE ws2_32)
    endif()
    rl_enable_warnings(rl-bench)

    add_executable(rl-redis-bench benchmarks/redis_bench.cpp)
    target_link_libraries(rl-redis-bench PRIVATE rl_core Threads::Threads)
    if(WIN32)
        target_link_libraries(rl-redis-bench PRIVATE ws2_32)
    endif()
    rl_enable_warnings(rl-redis-bench)
endif()

if(RL_BUILD_TESTS)
    enable_testing()
    add_executable(rl_tests tests/test_main.cpp)
    target_include_directories(rl_tests PRIVATE
        "${CMAKE_CURRENT_SOURCE_DIR}/src"
        "${CMAKE_CURRENT_SOURCE_DIR}")
    target_link_libraries(rl_tests PRIVATE rl_service nlohmann_json::nlohmann_json)
    rl_enable_warnings(rl_tests)

    add_test(NAME rl_tests COMMAND rl_tests)
    set_tests_properties(rl_tests PROPERTIES
        ENVIRONMENT "RL_TEST_REDIS_HOST=127.0.0.1;RL_TEST_REDIS_PORT=6379"
        WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}"
        TIMEOUT 120
    )

    add_executable(rl_cluster_test tests/cluster_test.cpp)
    target_link_libraries(rl_cluster_test PRIVATE rl_httplib nlohmann_json::nlohmann_json Threads::Threads)
    if(WIN32)
        target_link_libraries(rl_cluster_test PRIVATE ws2_32)
    endif()
    rl_enable_warnings(rl_cluster_test)

    add_test(NAME rl_cluster_test
        COMMAND rl_cluster_test
            "$<TARGET_FILE:rl-server>"
            "$<TARGET_FILE:rl-lb>"
            "${CMAKE_CURRENT_SOURCE_DIR}/config/policies.json")
    set_tests_properties(rl_cluster_test PROPERTIES
        ENVIRONMENT "RL_TEST_REDIS_HOST=127.0.0.1;RL_TEST_REDIS_PORT=6379;RL_CLUSTER_PORT_BASE=18181"
        WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}"
        TIMEOUT 180
    )

    add_executable(rl_failure_test tests/failure_test.cpp)
    target_link_libraries(rl_failure_test PRIVATE rl_httplib nlohmann_json::nlohmann_json Threads::Threads)
    if(WIN32)
        target_link_libraries(rl_failure_test PRIVATE ws2_32)
    endif()
    rl_enable_warnings(rl_failure_test)

    add_test(NAME rl_failure_test
        COMMAND rl_failure_test
            "$<TARGET_FILE:rl-server>"
            "${CMAKE_CURRENT_SOURCE_DIR}/config/policies.json")
    set_tests_properties(rl_failure_test PROPERTIES
        WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}"
        TIMEOUT 120
    )

    add_test(NAME rl_demo COMMAND rl-demo)
    set_tests_properties(rl_demo PROPERTIES
        ENVIRONMENT "RL_TEST_REDIS_HOST=127.0.0.1;RL_TEST_REDIS_PORT=6379"
        WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}"
        TIMEOUT 120
    )
endif()

include(GNUInstallDirs)
install(TARGETS rl_core rl_service rl-server rl-lb
    EXPORT rlTargets
    RUNTIME DESTINATION "${CMAKE_INSTALL_BINDIR}"
    LIBRARY DESTINATION "${CMAKE_INSTALL_LIBDIR}"
    ARCHIVE DESTINATION "${CMAKE_INSTALL_LIBDIR}"
)
install(DIRECTORY include/ DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}")
install(FILES config/policies.json DESTINATION "${CMAKE_INSTALL_DATADIR}/distributed-rate-limiter")
