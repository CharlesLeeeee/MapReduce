#include "threadpool.h"
#include <iostream>



ThreadPool_t *ThreadPool_create(int num){
    ThreadPool_t * tp = new ThreadPool_t;
    tp->tasks = new ThreadPool_work_queue_t;
    for(int i=0;i<num;i++){
        pthread_t * thread = new pthread_t;
        tp->threads.push_back(thread);
        pthread_create(tp->threads[i],NULL,(void *(*)(void*))(Thread_run),tp);
    }
    return tp;
}

void ThreadPool_destroy(ThreadPool_t *tp){
    pthread_mutex_lock(&tp->kill_mutex);
    while(tp->num_tasks>0)
        pthread_cond_wait(&tp->kill_cond,&tp->kill_mutex);
    pthread_mutex_unlock(&tp->kill_mutex);
    for(size_t i=0;i<tp->threads.size();i++){
        pthread_cancel(*tp->threads[i]);
        delete tp->threads[i];
    }
    tp->threads.clear();
    delete tp;
}

bool ThreadPool_add_work(ThreadPool_t *tp, thread_func_t func, void *arg){
    ThreadPool_work_t * work = new ThreadPool_work_t;
    work->func = func;
    work->arg = arg;
    tp->tasks->works.push(work);
    return true;
}

ThreadPool_work_t *ThreadPool_get_work(ThreadPool_t *tp){
    while(tp->tasks->works.empty()){
        pthread_cond_wait(&tp->get_cond,&tp->mutex);
    }
    ThreadPool_work_t * work = tp->tasks->works.front();
    tp->tasks->works.pop();
    return work;
}

void * Thread_run(ThreadPool_t *tp){
    while(tp->num_tasks > 0){
        pthread_mutex_lock(&tp->mutex);
        ThreadPool_work_t * work = ThreadPool_get_work(tp);
        pthread_mutex_unlock(&tp->mutex);
        work->func(work->arg);
        pthread_mutex_lock(&tp->count_mutex);
        tp->num_tasks--;
        pthread_cond_signal(&tp->kill_cond);
        pthread_mutex_unlock(&tp->count_mutex);
        delete work;
    }
    pthread_exit(0);
}