set(target dispatcher_test)

add_executable(${target}
    task_dispatcher.cpp
)

target_include_directories(${target}
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}
)

target_link_libraries(${target}
    PRIVATE
        GTest::GTest
        GTest::Main
        task_dispatcher
)

add_test(NAME ${target} COMMAND ${target})

add_subdirectory(queue)