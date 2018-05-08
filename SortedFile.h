#ifndef SORTED_FILE_H_
#define SORTED_FILE_H_

#include <string>

#include "GenericDBFile.h"
#include "Pipe.h"
#include "BigQ.h"
#include <pthread.h>

class SortedFile: public GenericDBFile {
  string path;
public:
  SortedFile():pipe_in(NULL), pipe_out(NULL), bigqsamp(NULL) {}
  ~SortedFile() {}
  int Create (const char *f_path, fType f_type, void *startup);
  int Open (const char* fpath);
  int Close ();
  void Add (Record& addme);
  void Load (Schema& myschema, const char* loadpath);
  void MoveFirst();
  int GetNext (Record& fetchme);
  int GetNext (Record& fetchme, CNF& cnf, Record& literal);
  void orderarg (OrderMaker *om, int rl){
    order = om;
    runLength = rl;
    pipe_in = new Pipe(PIPE_BUFFER_SIZE), pipe_out = new Pipe(PIPE_BUFFER_SIZE);
    bigqsamp = new BigQ(*pipe_in, *pipe_out, *order, runLength);
    pthread_create(&pt,NULL,func,(void *)this);
  }

  static void* func(void* args){
    SortedFile *sortfil = (SortedFile *) args;
    Record rec;
    while(sortfil->pipe_out->Remove(&rec)){
          if(sortfil->pgwrite.Append(&rec) == 0){
			        sortfil->fl.AddPage(&sortfil->pgwrite,sortfil->fl.lastone());
              sortfil->pgwrite.EmptyItOut();
              sortfil->pgwrite.Append(&rec);
		        }
      }
  }

private:
  pthread_t pt;
  static const size_t PIPE_BUFFER_SIZE = 100;

  friend class DBFile;   
  Pipe *pipe_in, *pipe_out;
  BigQ *bigqsamp;

  int binarySearch(Record& fetchme, OrderMaker& queryorder, Record& literal, OrderMaker& cnforder, ComparisonEngine& cmp);
};

#endif
