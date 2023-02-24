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
   public_include/rsf/rsf.h
   public_include/rsf/rsf.hf
   public_include/rsf/xdf_rsf_error_codes.h
)

add_library(${PROJECT_NAME} ${PROJECT_F_FILES} ${PROJECT_C_FILES} ${PROJECT_HEADER_FILES})
   
set_target_properties(${PROJECT_NAME} PROPERTIES POSITION_INDEPENDENT_CODE ON)
target_include_directories(${PROJECT_NAME} PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
target_include_directories(${PROJECT_NAME} PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/public_include)

# Where to put modules
get_target_property(LIBRARY_DIR ${PROJECT_NAME} BINARY_DIR)
set(MODULE_DIR ${LIBRARY_DIR}/include/dummy)
set_target_properties(${PROJECT_NAME} PROPERTIES Fortran_MODULE_DIRECTORY ${MODULE_DIR})
target_include_directories(${PROJECT_NAME} INTERFACE ${MODULE_DIR})

install(TARGETS ${PROJECT_NAME})
install(FILES 
   ${MODULE_DIR}/rsf_mod.mod
   DESTINATION include
)

install(FILES
   public_include/rsf/rsf.h
   public_include/rsf/rsf.hf
   public_include/rsf/xdf_rsf_error_codes.h
   DESTINATION include/rsf
)
