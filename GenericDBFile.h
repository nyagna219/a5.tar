#ifndef GENERICDBFILE_H
#define GENERICDBFILE_H

#pragma once
#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include <string>
#include <iostream>

typedef enum {heap, sorted, tree} fType;
// stub DBFile header..replace it with your own DBFile.h 

struct SortInfo{
	OrderMaker *myorder;
	int runLength;
};

class GenericDBFile {
protected:
	File fl;
	Page pgread;
	Page pgwrite;
	off_t curr;
public:
    OrderMaker *order;
    int runLength;
	GenericDBFile(){	
		check = false;				//bool variable to check file is open or not
		curr = 0; 					//indicator of the current page
		order = new OrderMaker();

	}
	~GenericDBFile(){}
	bool check;
    friend class DBFile;
	virtual void writepg(){
		if(pgwrite.numrecord() != 0){
			fl.AddPage(&pgwrite,fl.lastone());
			pgwrite.EmptyItOut();
		};
	}
	virtual int Create (const char *fpath, fType file_type, void *startup)=0;
	virtual int Open (const char *fpath)=0;
	virtual int Close ()=0;

	virtual void Load (Schema &myschema, const char *loadpath)=0;

	virtual void MoveFirst ()=0;
	virtual void Add (Record &addme)=0;
	virtual int GetNext (Record &fetchme)=0;
	virtual int GetNext (Record &fetchme, CNF &cnf, Record &literal)=0;
    virtual void orderarg (OrderMaker *om, int rl){};
    virtual string metafName(const char* f_path) {
  			std::string p(f_path);
  			return string(p+".meta");
		}
};
#endif
