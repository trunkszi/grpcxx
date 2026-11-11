include_guard(GLOBAL)

foreach(codec IN ITEMS zstd lz4 snappy)
  if(NOT TARGET grpcxx_dep::${codec})
    if(codec STREQUAL "zstd")
      set(header zstd.h)
    elseif(codec STREQUAL "lz4")
      set(header lz4frame.h)
    else()
      set(header snappy-c.h)
    endif()
    find_path(GRPCXX_${codec}_INCLUDE_DIR NAMES ${header} REQUIRED)
    find_library(GRPCXX_${codec}_LIBRARY NAMES ${codec} REQUIRED)
    add_library(grpcxx_dep::${codec} UNKNOWN IMPORTED GLOBAL)
    set_target_properties(grpcxx_dep::${codec} PROPERTIES
      IMPORTED_LOCATION "${GRPCXX_${codec}_LIBRARY}"
      INTERFACE_INCLUDE_DIRECTORIES "${GRPCXX_${codec}_INCLUDE_DIR}")
  endif()
endforeach()
