#pragma once

#include <memory>

#include "queue/priority_queue.hpp"
#include "thread_pool/thread_pool.hpp"
#include "types.hpp"

namespace dispatcher {

inline queue::PriorityQueue::Config DefaultConfig = {{TaskPriority::High, {true, 1000}},
                                                     {TaskPriority::Normal, {false}}};

class TaskDispatcher {
    thread_pool::ThreadPool thread_pool_;

public:
    TaskDispatcher(size_t thread_count, const queue::PriorityQueue::Config &config = DefaultConfig);

    void schedule(TaskPriority priority, queue::Task task);
    ~TaskDispatcher();
};

}  // namespace dispatcher