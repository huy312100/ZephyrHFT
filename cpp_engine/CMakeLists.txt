cmake_minimum_required(VERSION 3.20)
project(hft_engine VERSION 0.1.0 LANGUAGES CXX)

if(POLICY CMP0177)
    cmake_policy(SET CMP0177 NEW)  # normalise absolute install destinations
endif()

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
    set(CMAKE_BUILD_TYPE Release CACHE STRING "Build type" FORCE)
endif()

option(HFT_BUILD_PYTHON "Build the pybind11 extension module" ON)
option(HFT_BUILD_TESTS "Build C++ unit tests" ON)
option(HFT_ENABLE_NATIVE "Tune code generation for the build host" ON)
option(HFT_ENABLE_LTO "Enable link-time optimisation" ON)

add_library(hft_core STATIC
    src/ws_parser.cpp
)
add_library(hft::core ALIAS hft_core)

target_include_directories(hft_core PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)

target_compile_options(hft_core PUBLIC
    $<$<CXX_COMPILER_ID:AppleClang,Clang,GNU>:-Wall;-Wextra;-Wpedantic;-Wshadow>
    $<$<CXX_COMPILER_ID:AppleClang,Clang,GNU>:-O3>
)

if(HFT_ENABLE_NATIVE)
    target_compile_options(hft_core PUBLIC
        $<$<CXX_COMPILER_ID:AppleClang,Clang,GNU>:-march=native>
    )
endif()

if(HFT_ENABLE_LTO)
    include(CheckIPOSupported)
    check_ipo_supported(RESULT HFT_IPO_SUPPORTED OUTPUT HFT_IPO_ERROR)
    if(HFT_IPO_SUPPORTED)
        set_property(TARGET hft_core PROPERTY INTERPROCEDURAL_OPTIMIZATION TRUE)
    else()
        message(STATUS "IPO/LTO unavailable: ${HFT_IPO_ERROR}")
    endif()
endif()

# ---------------------------------------------------------------------------
# Python extension
# ---------------------------------------------------------------------------
if(HFT_BUILD_PYTHON)
    find_package(pybind11 CONFIG QUIET)
    if(NOT pybind11_FOUND)
        message(WARNING
            "pybind11 not found; install it with `python3 -m pip install pybind11` "
            "or pass -Dpybind11_DIR=$(python3 -m pybind11 --cmakedir).")
    else()
        pybind11_add_module(hft_native MODULE bindings/pybind_module.cpp)
        target_link_libraries(hft_native PRIVATE hft_core)
        target_compile_definitions(hft_native PRIVATE PYBIND11_DETAILED_ERROR_MESSAGES=1)
        set(HFT_PYTHON_PACKAGE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../python_analytics/native")
        set_target_properties(hft_native PROPERTIES
            LIBRARY_OUTPUT_DIRECTORY "${HFT_PYTHON_PACKAGE_DIR}"
        )
        install(TARGETS hft_native LIBRARY DESTINATION "${HFT_PYTHON_PACKAGE_DIR}")
    endif()
endif()

# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------
if(HFT_BUILD_TESTS)
    enable_testing()
    add_subdirectory(tests)
endif()

install(TARGETS hft_core ARCHIVE DESTINATION lib)
install(DIRECTORY include/ DESTINATION include)
