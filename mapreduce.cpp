#include "mapreduce.h"
#include <unordered_map>
#include <utility>
#include <bits/stdc++.h>
#include <map> 
#include <iostream>
#include <sys/stat.h>

std::map<std::string,std::vector<std::string>> * partitions;
std::map<std::string,std::vector<std::string>::iterator> * iterators;
int partition_size;
pthread_mutex_t * mutex2; 
Reducer reducer;

void MR_Run(int num_files, char *filenames[],
            Mapper map, int num_mappers,
            Reducer concate, int num_reducers){

    partitions = new std::map<std::string,std::vector<std::string>> [num_reducers];
    iterators = new std::map<std::string,std::vector<std::string>::iterator> [num_reducers];
    mutex2 = new pthread_mutex_t [num_reducers];
    partition_size = num_reducers;
    //pthread_mutex_t main_mutex = 
    std::vector<std::pair<off_t,char*>> sorted_files;
    for(int i=0;i<num_files;i++){
        struct stat buf;
        stat(filenames[i],&buf);
        sorted_files.push_back(std::make_pair(buf.st_size,filenames[i]));
    }
    std::sort(sorted_files.begin(),sorted_files.end());



    std::cout<<"1"<<std::endl;
    ThreadPool_t * tp = ThreadPool_create(num_mappers);
    std::cout<<"1.1"<<std::endl;
    for(int i=0;i<num_files;i++){
        ThreadPool_add_work(tp,(thread_func_t)map, sorted_files[i].second);
    }
    std::cout<<"2"<<std::endl;
    // std::cout<<tp->threads.size()<<std::endl;
    for(int i=0;i<num_mappers;i++){
        pthread_create(tp->threads[i],NULL,(void *(*)(void*))(Thread_run),tp);
    }
    std::cout<<"3"<<std::endl;
    // for(int i=0;i<num_mappers;i++){
    //     pthread_join(*tp->threads[i],NULL);
    // }
    std::cout<<"4"<<std::endl;
    ThreadPool_destroy(tp);

    std::cout<<"5"<<std::endl;

    // tp = ThreadPool_create(num_reducers);
    reducer = concate;
    // for(int i=0;i<num_reducers;i++){
    //     ThreadPool_add_work(tp,(thread_func_t)MR_ProcessPartition, (void *)i);
    // }
    pthread_t reducer_threads[num_reducers];
    int num_thread[num_reducers];
    std::cout<<"6"<<std::endl;
    // for(int i=0;i<num_reducers;i++){
    //     pthread_create(tp->threads[i],NULL,(void *(*)(void*))(Thread_run),tp);
    // }
    for(int i=0;i<num_reducers;i++){
        num_thread[i] = i;
        pthread_create(&reducer_threads[i],NULL,(void *(*)(void*))MR_ProcessPartition,(void *)num_thread[i]);
    }
    std::cout<<"7"<<std::endl;
    // for(int i=0;i<num_reducers;i++){
    //     pthread_join(*tp->threads[i],NULL);
    // }
    for(int i=0;i<num_reducers;i++){
        pthread_join(reducer_threads[i],NULL);
    }
    std::cout<<"8"<<std::endl;
    // ThreadPool_destroy(tp);
    std::cout<<"9"<<std::endl;
    delete [] partitions;
    std::cout<<"10"<<std::endl;
    delete [] iterators;
    delete [] mutex2;
    std::cout<<"11"<<std::endl;
}

void MR_Emit(char *key, char *value){
    unsigned long assigned_part = MR_Partition(key,partition_size);
    pthread_mutex_lock(&mutex2[assigned_part]);
    partitions[assigned_part][std::string(key)].push_back(std::string(value));
    pthread_mutex_unlock(&mutex2[assigned_part]);
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
    std::map<std::string,std::vector<std::string>>::iterator it;
    if(!partitions[partition_number].empty()){
        for(it=partitions[partition_number].begin();it!=partitions[partition_number].end();it++){
            iterators[partition_number][it->first] = partitions[partition_number][it->first].begin();
            reducer((char *)it->first.c_str(),partition_number);
        }
    }
    pthread_exit(0);
}

char *MR_GetNext(char *key, int partition_number){
    if(iterators[partition_number][std::string(key)] != partitions[partition_number][std::string(key)].end()){
        char * current = (char *)(*iterators[partition_number][std::string(key)]).c_str();
        iterators[partition_number][std::string(key)]++;
        return current;
    }
    return NULL;
}