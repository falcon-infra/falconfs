cmake_minimum_required(VERSION 3.10)
project(TestSliceKeyGen)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(GTest REQUIRED)
find_package(glog REQUIRED)
find_package(jsoncpp REQUIRED)


# 编译 test_slice_key_gen.cpp
add_executable(test_slice_key_gen
    test_slice_key_gen.cpp
    ../src/slice_key_gen.cpp
    ../../common/src/log/logging.cpp
    ../src/falcon_kv_meta_mock.cpp
)
target_include_directories(test_slice_key_gen PRIVATE
    ../src/include
    ../src/mock_include
    ../../common/src/include/
    # /opt/boostio/include
)
target_link_libraries(test_slice_key_gen pthread glog::glog)



# 编译 test_kvserver.cpp
set(
    SOURCES
    ../src/falcon_kv_server.cpp
    ../src/slice_key_gen.cpp
    ../src/falcon_kv_meta_mock.cpp
    ../src/bio_c.cpp
    # falconlog相关的依赖实现
    ../../common/src/log/logging.cpp
)
add_library(my_local_kvserver STATIC ${SOURCES})
target_include_directories(my_local_kvserver PRIVATE
    ../src/include
    ../src/mock_include
    ../../common/src/include
    /usr/local/obs/include
    jsoncpp
    # /opt/boostio/include
    
)
add_executable(test_kvserver test_kvserver.cpp)
target_include_directories(test_kvserver PRIVATE 
    ../src/include 
    ../src/mock_include
    # /opt/boostio/include
)
target_link_libraries(test_kvserver 
    my_local_kvserver 
    pthread 
    glog::glog
    GTest::gtest
    GTest::gtest_main
    jsoncpp_lib
)
