# The minimum version can not be 3.16 because of the LINK_LANGUAGE generator expression in Config.cmake.in
cmake_minimum_required(VERSION 3.20)

add_executable(rsf_dump test_rsf.c)
target_compile_definitions(rsf_dump PRIVATE RSF_DUMP)

# add_executable(rsf_open test_rsf.c)
# target_compile_definitions(rsf_open PRIVATE RSF_OPEN)

set(TARGETS
    # rsf_open
    rsf_dump)

foreach(TARGET IN ITEMS ${TARGETS})
    target_link_libraries(${TARGET} PRIVATE ${PROJECT_NAME})
    target_include_directories(${TARGET} PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../src)
endforeach()
