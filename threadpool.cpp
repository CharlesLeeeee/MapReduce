#include "threadpool.h"
#include <iostream>

pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER; 
pthread_mutex_t mutex2 = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t cond_finish_work = PTHREAD_COND_INITIALIZER;
int threads_remaining = 0;  

ThreadPool_t *ThreadPool_create(int num){
    ThreadPool_t * tp = new ThreadPool_t;
    tp->tasks = new ThreadPool_work_queue_t;
    threads_remaining = num;
    for(size_t i=0;i<num;i++){
        pthread_t * thread = new pthread_t;
        tp->threads.push_back(thread);
    }
    return tp;
}

void ThreadPool_destroy(ThreadPool_t *tp){
    // pthread_mutex_lock(&mutex);
    // pthread_cond_wait(&cond_finish_work,&mutex);
    // pthread_mutex_unlock(&mutex);
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
    // if(threads_remaining > 0){
    //     pthread_create(tp->threads[tp->threads.size()-threads_remaining],NULL,(void *(*)(void*))(Thread_run),tp);
    //     threads_remaining--;
    // }
    return true;
}

ThreadPool_work_t *ThreadPool_get_work(ThreadPool_t *tp){
    if(!tp->tasks->works.empty()){
        ThreadPool_work_t * work = tp->tasks->works.front();
        tp->tasks->works.pop();
        tp->work_needed--;
        return work;
    }
    return NULL;
}

void * Thread_run(ThreadPool_t *tp){
    while(!tp->tasks->works.empty()){
        pthread_mutex_lock(&mutex);
        // if(!tp->tasks->works.empty()){ 
            ThreadPool_work_t * work = ThreadPool_get_work(tp);
            pthread_mutex_unlock(&mutex);
            if(work){
                work->func(work->arg);
                delete work;
            }
        //     pthread_mutex_unlock(&mutex);
        // }
        // else{
        //     // pthread_cond_broadcast(&cond_finish_work);
            // pthread_mutex_unlock(&mutex);
        //     pthread_exit(0);
        // }
        // pthread_mutex_lock(&mutex);
    }
    pthread_exit(0);
}
