#include "HeapFile.h"
#include <fstream>
#include <iostream>

// stub file .. replace it with your own DBFile.cc
using namespace std;

int HeapFile::Create (const char *f_path, fType f_type, void *startup) {
	if(check){	
		return 0;
	}
	
  int ftype;
  OrderMaker *ordermaker = new OrderMaker();
  ofstream output_fstream(metafName(f_path));
  if(output_fstream){
  output_fstream << ftype <<endl<<  *ordermaker << endl<<0;

	fl.Open(0,const_cast<char*>(f_path));			
	check = true;
	output_fstream.close();
	return 1;
}
else
return 0;
}

void HeapFile::Load (Schema &f_schema, const char *loadpath) {
	FILE *sample_file = fopen(loadpath,"r");
	if(check){									
	Record cur_record;
	while(cur_record.SuckNextRecord(&f_schema, sample_file)) 		
		Add(cur_record);
	}
}

int HeapFile::Open (const char *f_path) {
	int val=0;
	if(!check){
	fl.Open(1,const_cast<char*>(f_path));
	check = true;
	val =1;
	}
	return val;
	}
	

void HeapFile::MoveFirst () {
	writepg();
	pgread.EmptyItOut();
	fl.GetPage(&pgread,curr=0);	

}

int HeapFile::Close () {
	int val=0;
	if(check){
	writepg();
	fl.Close();
	check = false;
	val=1;
	}
	return val;
}

void HeapFile::Add (Record &rec) {
		if(pgwrite.Append(&rec) == 0){
			fl.AddPage(&pgwrite,fl.lastone());
			pgwrite.EmptyItOut();
			pgwrite.Append(&rec);
		}

}

int HeapFile::GetNext (Record &fetchme) {
	writepg();
	int val=1;
	while(!pgread.GetFirst(&fetchme)){
		if(++curr == fl.lastone())
		{
			val=0;
			break;
		}
		fl.GetPage(&pgread,curr);
	}
	return val;
}

int HeapFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
	ComparisonEngine comp;
	while(GetNext(fetchme)){
		if (comp.Compare (&fetchme, &literal, &cnf))
            return 1;
	}
	return 0;
}
