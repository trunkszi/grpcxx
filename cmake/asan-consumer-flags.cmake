# Triplet chainload runs before VCPKG_*_FLAGS exist, so those flags never reach
# *_INIT. Ports still receive them as vcpkg configure arguments. This file is
# pulled in at the end of project(), after *_INIT has been copied here, which
# keeps the toolchain -B and --sysroot fragments.
macro(_grpcxx_append_asan variable flags)
  if(NOT "${${variable}}" MATCHES "-fsanitize=address")
    set("${variable}" "${${variable}} ${flags}")
    set("${variable}" "${${variable}}" CACHE STRING "" FORCE)
  endif()
endmacro()

set(_grpcxx_asan_compile "-fsanitize=address,undefined -fno-sanitize-recover=all -fno-omit-frame-pointer")
set(_grpcxx_asan_link "-fsanitize=address,undefined")
_grpcxx_append_asan(CMAKE_C_FLAGS "${_grpcxx_asan_compile}")
_grpcxx_append_asan(CMAKE_CXX_FLAGS "${_grpcxx_asan_compile}")
_grpcxx_append_asan(CMAKE_EXE_LINKER_FLAGS "${_grpcxx_asan_link}")
_grpcxx_append_asan(CMAKE_SHARED_LINKER_FLAGS "${_grpcxx_asan_link}")
_grpcxx_append_asan(CMAKE_MODULE_LINKER_FLAGS "${_grpcxx_asan_link}")
unset(_grpcxx_asan_compile)
unset(_grpcxx_asan_link)
