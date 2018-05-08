#include <fstream>
#include <stdio.h>    
#include "SortedFile.h"


int SortedFile::Open (const char* f_path) {
  int f_type;
  path = string(f_path); 
  ifstream input_fstream(metafName(f_path));
  int val=0;
  if(!input_fstream){
    return val;
  }
  input_fstream >> f_type >> *order >> runLength;
  input_fstream.close();
 
 if(!check){
	fl.Open(1,const_cast<char*>(f_path));
	check = true;
	val= 1;
  }
return val;
}

int SortedFile::Create(const char *f_path, fType f_type, void *startup) {
	typedef struct { OrderMaker* o; int l; } *pOrder;
	pOrder po = (pOrder)startup;
	order = po->o;
	runLength = po->l;
	if (check) {
		return 0;
	}
	path = string(f_path);
	fl.Open(0, const_cast<char*>(f_path));			
	check = true;
	return 1;
}


void SortedFile::Add (Record& addme) {
  pipe_in->Insert(&addme);
}

void SortedFile::Load (Schema& myschema, const char* loadpath) {
  FILE *f = fopen(loadpath,"r");
	if(check){									
	Record r;
	while(r.SuckNextRecord(&myschema, f)) 		
		Add(r);
	}
}
void SortedFile::MoveFirst () {
  	writepg();
	pgread.EmptyItOut();
	fl.GetPage(&pgread,curr=0);	
}

int SortedFile::GetNext (Record& fetchme) {
  writepg();
	while(!pgread.GetFirst(&fetchme)){
		if(++curr == fl.lastone())
			return 0;
		fl.GetPage(&pgread,curr);
	}
	return 1;
}

int SortedFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
int val=0; 
 OrderMaker queryorder, cnforder;
  OrderMaker::queryOrderMaker(*order, cnf, queryorder, cnforder);
  ComparisonEngine cmp;
  if (!binarySearch(fetchme, queryorder, literal, cnforder, cmp)) 
   return val;
  while(true) {
    if (cmp.Compare(&fetchme, &queryorder, &literal, &cnforder)) 
   	return val;
    if (cmp.Compare(&fetchme, &literal, &cnf)){ 
	 return 1;
     }
    if(!GetNext(fetchme))
		break;
  }
  return val;  
 };

int SortedFile::binarySearch(Record& fetchme, OrderMaker& queryorder, Record& literal, OrderMaker& cnforder, ComparisonEngine& cmp) {
  if (!GetNext(fetchme)) 
    return 0;
  int result = cmp.Compare(&fetchme, &queryorder, &literal, &cnforder);
  if (result > 0) 
    return 0;
  else if (result == 0)
   return 1;
  int low=curr, high=fl.lastone()-1, mid=(low+high)/2;
  for (; low<mid; mid=(low+high)/2) {
    fl.GetPage(&pgread, mid);
    if(!GetNext(fetchme)){
            return 0;
    } 
    result = cmp.Compare(&fetchme, &queryorder, &literal, &cnforder);
    if (result<0) low = mid;
    else if (result>0) high = mid-1;
    else high = mid;
  }
  fl.GetPage(&pgread, low);
  do {  
    if (!GetNext(fetchme)) return 0;
    result = cmp.Compare(&fetchme, &queryorder, &literal, &cnforder);
  } while (result<0);
  return result==0;
}

int SortedFile::Close() {
	pipe_in->ShutDown();
	pthread_join(pt, NULL);
	writepg();
	fl.Close();
	int ftype;
	ofstream output_fstream(metafName(path.c_str()));
	int val=0;
	if (output_fstream) {
	   output_fstream << fType::sorted << endl << *order << runLength;
           output_fstream.close();
	   val=1;
	}
	return val;
}

