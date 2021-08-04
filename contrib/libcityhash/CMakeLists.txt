add_library(cityhash
	src/city.cc
	src/city_avx2.c
	src/city_mb_flush_avx2.asm
	src/city_mb_submit_avx2.asm
	src/city_mb_mgr_datastruct.asm
	src/city_mb_x8_avx2.asm

	include/citycrc.h
	include/city.h
	include/datastruct.asm
	include/city_mb.h
	src/config.h)

add_executable(city_hash_test src/city_hash_test.cc)
target_link_libraries(city_hash_test cityhash)

target_include_directories (cityhash BEFORE PUBLIC include)
target_include_directories (cityhash PRIVATE src)
