#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "Defs.h"
#include "HeapFile.h"
#include "SortedFile.h"
#include <fstream>
#include <iostream>

// stub file .. replace it with your own DBFile.cc
using namespace std;

DBFile::DBFile () {}

int DBFile::Create (const char *f_path, fType f_type, void *startup) {
	if(f_type==heap){
		 gbf = new HeapFile();
	}
	else if(f_type==sorted){
		        gbf = new SortedFile();
			typedef struct { OrderMaker* o; int l; } *pOrder;
  			pOrder po = (pOrder)startup;
			gbf->orderarg(po->o,po->l);
			
	}
	
	return gbf->Create(f_path, f_type, startup);
}

void DBFile::Load (Schema &f_schema, const char *loadpath) {
	if(gbf){
		gbf->Load(f_schema,loadpath);
	}
}

int DBFile::Open (const char *f_path) {
	ifstream ifile(string(f_path)+".meta");
	string val;
	getline(ifile,val);
	ifile.close();
	if(stoi(val)==0){
		gbf=new HeapFile();
		gbf->Open(f_path);
	  }
	else if(stoi(val)==1){
			   gbf=new SortedFile();
			   gbf->Open(f_path);
			   gbf->orderarg(gbf->order,gbf->runLength);
	}
}

void DBFile::MoveFirst () {
	gbf->MoveFirst();
}

int DBFile::Close () {
	return gbf->Close();
}

void DBFile::Add (Record &rec) {
	gbf->Add(rec);
}

int DBFile::GetNext (Record &fetchme) {
	return gbf->GetNext(fetchme);
}

int DBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
	return gbf->GetNext(fetchme, cnf, literal);
}
