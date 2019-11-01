#include "threadpool.h"
#include <iostream>

pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t get_cond =  PTHREAD_COND_INITIALIZER;
bool getting = false; 


ThreadPool_t *ThreadPool_create(int num){
    ThreadPool_t * tp = new ThreadPool_t;
    tp->tasks = new ThreadPool_work_queue_t;
    for(int i=0;i<num;i++){
        pthread_t * thread = new pthread_t;
        tp->threads.push_back(thread);
    }
    return tp;
}

void ThreadPool_destroy(ThreadPool_t *tp){
    for(size_t i=0;i<tp->threads.size();i++){
        pthread_join(*tp->threads[i],NULL);
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
    if(tp->num_threadsworking < (int)tp->threads.size()){
        pthread_create(tp->threads[tp->num_threadsworking++],NULL,(void *(*)(void*))(Thread_run),tp);
    }
    return true;
}

ThreadPool_work_t *ThreadPool_get_work(ThreadPool_t *tp){
    if(!tp->tasks->works.empty()){
        getting = true; 
        ThreadPool_work_t * work = tp->tasks->works.front();
        tp->tasks->works.pop();
        tp->num_tasks--;
        getting = false;
        pthread_cond_signal(&get_cond);
        return work;
    }
    pthread_cond_broadcast(&get_cond);
    return NULL;
}

void * Thread_run(ThreadPool_t *tp){
    while(tp->num_tasks){
        pthread_mutex_lock(&mutex);
        while(getting){
            pthread_cond_wait(&get_cond,&mutex);
        }
        ThreadPool_work_t * work = ThreadPool_get_work(tp);
        pthread_mutex_unlock(&mutex);
        if(work){
            work->func(work->arg);
            delete work;
        }
    }
    pthread_exit(0);
}
