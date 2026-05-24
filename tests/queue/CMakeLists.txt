set(target queue_test)

add_executable(${target}
    bounded_queue.cpp
    unbounded_queue.cpp
    priority_queue.cpp
)

target_include_directories(${target}
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/..
)

target_link_libraries(${target}
    PRIVATE
        GTest::GTest
        GTest::Main
        queue
)

add_test(NAME ${target} COMMAND ${target})