#include "mapreduce.h"
#include <unordered_map>
#include <utility>
#include <bits/stdc++.h>
#include <map> 
#include <iostream>
#include <unistd.h>
#include <sys/stat.h>

std::map<std::string,std::vector<std::string> > * partitions;
std::map<std::string,std::vector<std::string>::iterator> * iterators;
pthread_mutex_t * mutex_partition; 
int partition_size;
Reducer reducer;
pthread_mutex_t main_mutex = PTHREAD_MUTEX_INITIALIZER;


void MR_Run(int num_files, char *filenames[],
            Mapper map, int num_mappers,
            Reducer concate, int num_reducers){

    std::vector<std::pair<off_t,char*> > sorted_files;
    for(int i=0;i<num_files;i++){
        if(access(filenames[i],F_OK)==0){
            struct stat buf;
            stat(filenames[i],&buf);
            sorted_files.push_back(std::make_pair(buf.st_size,filenames[i]));
        }
    }
    std::sort(sorted_files.begin(),sorted_files.end());
    ThreadPool_t * tp = ThreadPool_create(num_mappers);
    tp->work_needed = num_files;
    for(int i=0;i<sorted_files.size();i++){
        ThreadPool_add_work(tp,(thread_func_t)map, sorted_files[i].second);
    }
    for(int i=0;i<num_mappers;i++){
        pthread_create(tp->threads[i],NULL,(void *(*)(void*))(Thread_run),tp);
    }
    ThreadPool_destroy(tp);



    partitions = new std::map<std::string,std::vector<std::string> > [num_reducers];
    iterators = new std::map<std::string,std::vector<std::string>::iterator> [num_reducers];
    mutex_partition = new pthread_mutex_t [num_reducers];
    partition_size = num_reducers;
    reducer = concate;
    // pthread_t reducethreads[num_reducers];
    
    int partition_nums [num_reducers];
    // for(int i=0;i<num_reducers;i++){
    //     partition_nums[i] = i;
    //     pthread_create(&reducethreads[i],NULL,(void *(*)(void*))MR_ProcessPartition,(void *)partition_nums[i]);
    // }
    // for(size_t i=0;i<num_reducers;i++){
    //     pthread_join(reducethreads[i],NULL);
    // }
    tp = ThreadPool_create(num_reducers);
    for(int i=0;i<num_reducers;i++){
        partition_nums[i] = i;
        ThreadPool_add_work(tp,(thread_func_t)MR_ProcessPartition, (void *)partition_nums[i]);
    }
    for(int i=0;i<num_reducers;i++){
        pthread_create(tp->threads[i],NULL,(void *(*)(void*))(Thread_run),(void*)tp);
    }
    ThreadPool_destroy(tp);


    delete [] partitions;
    delete [] iterators;
    delete [] mutex_partition;
}

void MR_Emit(char *key, char *value){
    unsigned long assigned_part = MR_Partition(key,partition_size);
    // pthread_mutex_lock(&mutex_partition[assigned_part]);
    pthread_mutex_lock(&main_mutex);
    partitions[assigned_part][std::string(key)].push_back(std::string(value));
    pthread_mutex_unlock(&main_mutex);
    // pthread_mutex_unlock(&mutex_partition[assigned_part]);
}

unsigned long MR_Partition(char *key, int num_partitions){
    unsigned long hash = 5381;
    int c;
    while ((c = *key++) != '\0'){
        hash = hash * 33 + c;
    }
    return hash % num_partitions;
}

void MR_ProcessPartition(int partition_number){
    std::map<std::string,std::vector<std::string> >::iterator it;
    if(!partitions[partition_number].empty()){
        for(it=partitions[partition_number].begin();it!=partitions[partition_number].end();it++){
            iterators[partition_number][it->first] = partitions[partition_number][it->first].begin();
            reducer((char *)it->first.c_str(),partition_number);
        }
    }
}

char *MR_GetNext(char *key, int partition_number){
    if(iterators[partition_number][std::string(key)] != partitions[partition_number][std::string(key)].end()){
        char * current = (char *)(*iterators[partition_number][std::string(key)]).c_str();
        iterators[partition_number][std::string(key)]++;
        return current;
    }
    return NULL;
}