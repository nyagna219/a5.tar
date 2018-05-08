#ifndef BIGQ_H
#define BIGQ_H
#include <pthread.h>
#include <iostream>
#include <queue>
#include <algorithm>
#include "Pipe.h"
#include "File.h"
#include "Record.h"
#include <string>

class Page;
class File;


using namespace std;


class BigQ {
public:
	Pipe& in;
	Pipe& out;
	OrderMaker& so1;
	OrderMaker& so2;
	int runlen;
    static int countvar;
	Page *currpage;
	File *theFile;
	string filename;
	pthread_t worker;

	BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen);
	~BigQ ();

	static void *Working(void *biq);
private:
  static int cnt;
  
};

#endif