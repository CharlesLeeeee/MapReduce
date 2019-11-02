#include "mapreduce.h"
#include <utility>
#include <bits/stdc++.h>
#include <map> 
#include <iostream>
#include <sys/stat.h>

std::map<std::string,std::queue<std::string> > * partitions;
int partition_size;
pthread_mutex_t * partition_mutex; 
Reducer reducer;

void * paritionProcessor(void * arg){
    int * arg_int = (int*) arg;
    MR_ProcessPartition(*arg_int);
    pthread_exit(0);
}

void MR_Run(int num_files, char *filenames[],
            Mapper map, int num_mappers,
            Reducer concate, int num_reducers){

    partitions = new std::map<std::string,std::queue<std::string> > [num_reducers];
    partition_mutex = new pthread_mutex_t [num_reducers];
    partition_size = num_reducers;
    std::vector<std::pair<off_t,char*> > sorted_files;
    for(int i=0;i<num_files;i++){
        struct stat buf;
        stat(filenames[i],&buf);
        sorted_files.push_back(std::make_pair(buf.st_size,filenames[i]));
    }
    std::sort(sorted_files.begin(),sorted_files.end());



    ThreadPool_t * tp = ThreadPool_create(num_mappers);
    tp->num_tasks = num_files;
    for(int i=0;i<num_files;i++){
        ThreadPool_add_work(tp,(thread_func_t)map, sorted_files[i].second);
    }
    ThreadPool_destroy(tp);


    reducer = concate;
    pthread_t reducer_threads[num_reducers];
    int num_thread[num_reducers];
    for(int i=0;i<num_reducers;i++){
        num_thread[i] = i;
        pthread_create(&reducer_threads[i],NULL,paritionProcessor,&num_thread[i]);
    }
    for(int i=0;i<num_reducers;i++){
        pthread_join(reducer_threads[i],NULL);
    }
    delete [] partitions;
    delete [] partition_mutex;
}

void MR_Emit(char *key, char *value){
    unsigned long assigned_part = MR_Partition(key,partition_size);
    pthread_mutex_lock(&partition_mutex[assigned_part]);
    partitions[assigned_part][std::string(key)].push(std::string(value));
    pthread_mutex_unlock(&partition_mutex[assigned_part]);
}


/*
* From the assignment 2 sheet
*/
unsigned long MR_Partition(char *key, int num_partitions){
    unsigned long hash = 5381;
    int c;
    while ((c = *key++) != '\0'){
        hash = hash * 33 + c;
    }
    return hash % num_partitions;
}

void MR_ProcessPartition(int partition_number){
    std::map<std::string,std::queue<std::string> >::iterator it;
    if(!partitions[partition_number].empty()){
        for(it=partitions[partition_number].begin();it!=partitions[partition_number].end();it++){
            reducer((char *)it->first.c_str(),partition_number);
        }
    }
}

char *MR_GetNext(char *key, int partition_number){
    if(!partitions[partition_number][std::string(key)].empty()){
        char * current = (char *)partitions[partition_number][std::string(key)].front().c_str();
        partitions[partition_number][std::string(key)].pop();
        return current;
    }
    return NULL;
}
