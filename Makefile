CC:=gcc
WARN:=-Wall 
LIB:=-lm -pthread -O3
CCOPTS:=-std=c99 -ggdb -D_GNU_SOURCE
TARGET:=distwc

all: threadpool.o mapreduce.o

%.o: %.c
	$(CC) $(WARN) $(CCOPTS) $< -c $(LIB)

clean-all:
	rm -rf *.o *.gch $(TARGET)

threadpool.o: threadpool.cpp threadpool.h
mapreduce.o: mapreduce.cpp mapreduce.h threadpool.h
