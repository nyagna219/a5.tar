#ifndef REL_OP_H
#define REL_OP_H
#pragma ones
#include "Pipe.h"
#include "DBFile.h"
#include "Record.h"
#include "Function.h"
#include "BigQ.h"
#include <pthread.h>
#include <iostream>
#include <cstdlib>
#include <sstream>
#include "Defs.h"
#include <vector>
using namespace std;
class RelationalOp {
	private:
	int page;
	public:
	void WaitUntilDone (){
		pthread_join(worker,NULL);
	}
	void Use_n_Pages (int n) {
		page=n;
	}
	pthread_t worker;
	static int create_join_thread(pthread_t *thread,void *(*start_routine)(void *),void * args);
};

class SelectFile : public RelationalOp { 
	static void* thread_work(void* arg);
	public:
	void Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal);
};

class SelectPipe : public RelationalOp {
	static void* thread_work(void* arg);
	public:
	void Run (Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal);
};
class Project : public RelationalOp { 
	static void* thread_work(void* arg);
	public:
	void Run (Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput);
};
class Join : public RelationalOp { 
	static void* thread_work(void* arg);
	public:
	void Run (Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal);
};
class DuplicateRemoval : public RelationalOp {
	static void* thread_work(void* arg);
	public:
	void Run (Pipe &inPipe, Pipe &outPipe, Schema &mySchema);
};
class Sum : public RelationalOp {
	static void* thread_work(void* arg);
	public:
	void Run (Pipe &inPipe, Pipe &outPipe, Function &computeMe);
};
class GroupBy : public RelationalOp {
	static void* thread_work(void* arg);
	public:
	void Run (Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe);
};
class WriteOut : public RelationalOp {
	static void* thread_work(void* arg);
	public:
	void Run (Pipe &inPipe, FILE *outFile, Schema &mySchema);
};
#endif