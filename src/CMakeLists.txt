# The minimum version can not be 3.16 because of the LINK_LANGUAGE generator expression in Config.cmake.in
cmake_minimum_required(VERSION 3.20)

set(PROJECT_F_FILES
   rsf_mod.F90
)

set(PROJECT_C_FILES
   rsf.c
)

set(PROJECT_HEADER_FILES
   rsf/rsf_int.h
   rsf/rsf.h
   rsf/rsf.hf
   rsf/xdf_rsf_error_codes.h
)

add_library(${PROJECT_NAME} ${PROJECT_F_FILES} ${PROJECT_C_FILES} ${PROJECT_HEADER_FILES})
   
set_target_properties(${PROJECT_NAME} PROPERTIES POSITION_INDEPENDENT_CODE ON)
target_include_directories(${PROJECT_NAME} PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

install(TARGETS ${PROJECT_NAME})
install(FILES 
   ${CMAKE_CURRENT_BINARY_DIR}/rsf_mod.mod
   DESTINATION include
)

install(FILES
   rsf/rsf.h
   rsf/rsf.hf
   rsf/xdf_rsf_error_codes.h
   DESTINATION include/rsf
)
