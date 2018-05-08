#include "RelOp.h"

using namespace std;

struct Param4{
	DBFile *dbfile;
	Pipe *outPipe;
	Pipe *inputPipe1,*inputPipe2;
	CNF *cnf;
	Record *literal;
	Function *func;
	Schema *mySchema;
	int *flag;
	FILE *file;
	int inputcount;
	int outputcount;
	OrderMaker *orderMake;
};

void sum(Pipe *inputPipe1,Pipe *outputPipe, Function *func){
	Record rec;
	double result=0.0;
	while(inputPipe1->Remove(&rec)){
		int int_res=0;double dbl_res=0;
		func->Apply(rec,int_res,dbl_res);
		result += (int_res + dbl_res);
	}
	Attribute DA = {"double", Double};
	Schema sum_sch ("sum_sch", 1, &DA);
	stringstream ss;
	ss<<result<<"|";
	Record *rcd=new Record();
	rcd->ComposeRecord(&sum_sch, ss.str().c_str());
	outputPipe->Insert(rcd);
	outputPipe->ShutDown();
}

  
void SelectFile::Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal) {
	Param4 *args=static_cast<struct Param4 *>(malloc(sizeof(struct Param4)));
	args->dbfile=&inFile;
	args->outPipe=&outPipe;
	args->cnf=&selOp;
	args->literal=&literal;
	if(create_join_thread(&worker,thread_work,(void *)args)){
		std::cout<<"encountered  issue while creating the thread\n";
	}
}

void *SelectFile::thread_work(void *args){
  struct Param4 *arg = (struct Param4 *)(args);                     
  arg->dbfile->MoveFirst();
  Record next;
  int i=0;
  while(arg->dbfile->GetNext(next,*arg->cnf,*arg->literal))
		arg->outPipe->Insert(&next);
  arg->outPipe->ShutDown();
}

void SelectPipe::Run (Pipe &inputPipe1, Pipe &outPipe, CNF &selOp, Record &literal){
	Param4 *args=static_cast<struct Param4 *>(malloc(sizeof(struct Param4)));
	args->inputPipe1=&inputPipe1;
	args->outPipe=&outPipe;
	args->cnf=&selOp;
	args->literal=&literal;
	if(create_join_thread(&worker,thread_work,(void *)args)){
		std::cout<<"encountered  issue while creating the thread\n";
	}
}

void* SelectPipe::thread_work(void* args){
	struct Param4 *arg = (struct Param4 *)(args);     
	ComparisonEngine cmp;
	Record rec;
	while(arg->inputPipe1->Remove(&rec))
		if(cmp.Compare(&rec,arg->literal,arg->cnf))
			arg->outPipe->Insert(&rec);
	arg->outPipe->ShutDown();
}

void Sum::Run (Pipe &inputPipe1, Pipe &outPipe, Function &computeMe){
	Param4 *args=static_cast<struct Param4 *>(malloc(sizeof(struct Param4)));
	args->inputPipe1=&inputPipe1;
	args->outPipe=&outPipe;
	args->func=&computeMe;
	if(create_join_thread(&worker,thread_work,(void *)args)){
		std::cout<<"error creating thread\n";
	}
}

void *Sum::thread_work(void *args){
	struct Param4 *arg = (struct Param4 *)(args);   
	sum(arg->inputPipe1,arg->outPipe,arg->func);
}

void DuplicateRemoval::Run (Pipe &inputPipe1, Pipe &outPipe, Schema &mySchema) {
	Param4 *args=static_cast<struct Param4 *>(malloc(sizeof(struct Param4)));
	args->inputPipe1=&inputPipe1;
	args->outPipe=&outPipe;
	args->mySchema=&mySchema;
	if(create_join_thread(&worker,thread_work,(void *)args)){
		std::cout<<"error creating thread\n";
	}
 }


void* DuplicateRemoval::thread_work(void* args){
	struct Param4 *arg = (struct Param4 *)(args);  
 	OrderMaker sortOrder(arg->mySchema);
  	Pipe sorted(100);
  	BigQ biq(*arg->inputPipe1, sorted, sortOrder, RUNLEN);
  	Record cur, next;
  	ComparisonEngine cmp;
	int c=0;
  	if(sorted.Remove(&cur)) {
    	while(sorted.Remove(&next))
      		if(cmp.Compare(&cur, &next, &sortOrder)) {
        		arg->outPipe->Insert(&cur);
				std::cout<<c++<<endl;
        		cur.Consume(&next);
      		}
    	arg->outPipe->Insert(&cur);
  	}
  	arg->outPipe->ShutDown();
}

int RelationalOp::create_join_thread(pthread_t *thread,void *(*start_routine) (void *), void *arg){
  	int rc = pthread_create(thread, NULL, start_routine, arg);
	return rc;
}


void Project::Run (Pipe &inputPipe1, Pipe &outPipe, int *flag, int inputcount, int outputcount){
	Param4 *args=static_cast<struct Param4 *>(malloc(sizeof(struct Param4)));
	args->inputPipe1=&inputPipe1;
	args->outPipe=&outPipe;
	args->flag=flag;
	args->inputcount=inputcount;
	args->outputcount=outputcount;
	if(create_join_thread(&worker,thread_work,(void *)args)){
		std::cout<<"error creating thread\n";
	}
 }

 void* Project::thread_work(void* args){
	struct Param4 *arg = (struct Param4 *)(args);  
	Record tmpRcd;
	while(arg->inputPipe1->Remove(&tmpRcd)) {
		tmpRcd.Project(arg->flag, arg->outputcount, arg->inputcount);
		arg->outPipe->Insert(&tmpRcd);
	}
	arg->outPipe->ShutDown();
	return NULL;
 }

 void GroupBy::Run (Pipe &inputPipe1, Pipe &outPipe, OrderMaker &orderMake, Function &computeMe) {
	Param4 *args=static_cast<struct Param4 *>(malloc(sizeof(struct Param4)));
	args->inputPipe1 = &inputPipe1;
	args->outPipe = &outPipe;
	args->orderMake = &orderMake;
	args->func = &computeMe;
	create_join_thread(&worker,thread_work,(void *)args);
}

void* GroupBy::thread_work(void* args){
	struct Param4 *arg = (struct Param4 *)(args);  
	Pipe sortPipe(100);
	BigQ *bigQ = new BigQ(*(arg->inputPipe1), sortPipe, *(arg->orderMake), RUNLEN);

	int ir;  double dr;
	Type type;
	Attribute DA = {"double", Double};
	Attribute attr;
	attr.name = (char *)"sum";
	attr.myType = type;
	Schema *schema = new Schema ((char *)"dummy", 1, &attr);
	Attribute s_nationkey = {"s_nationkey", Int};
	Attribute outatt[] = {DA, s_nationkey};
    Schema out_sch("out_sch", 2, outatt);
	int numAttsToKeep = arg->orderMake->numAtts + 1;
	int *attsToKeep = new int[numAttsToKeep];
	attsToKeep[0] = 0; 
	for(int i = 1; i < numAttsToKeep; i++)
		attsToKeep[i] = arg->orderMake->whichAtts[i-1];

	ComparisonEngine cmp;
	Record *tmpRcd = new Record();
	if(sortPipe.Remove(tmpRcd)) {
		bool more = true;
		while(more) {
			more = false;
			type = arg->func->Apply(*tmpRcd, ir, dr);
			double sum=0;
			sum += (ir+dr);

			Record *r = new Record();
			Record *lastRcd = new Record;
			lastRcd->Copy(tmpRcd);
			while(sortPipe.Remove(r)) {
				if(cmp.Compare(lastRcd, r, arg->orderMake) == 0){ 
					type = arg->func->Apply(*r, ir, dr);
					sum += (ir+dr);
				} else {
					tmpRcd->Copy(r);
					more = true;
					break;
				}
			}
			ostringstream ss;
			ss <<sum <<"|";
			Record *sumRcd = new Record();
			sumRcd->ComposeRecord(schema, ss.str().c_str());

			Record *tuple = new Record;
			tuple->MergeRecords(sumRcd, lastRcd, 1, arg->orderMake->numAtts, attsToKeep,  numAttsToKeep, 1);
			arg->outPipe->Insert(tuple);
		}
	}
	arg->outPipe->ShutDown();
}
void WriteOut::Run (Pipe &inputPipe1, FILE *outFile, Schema &mySchema){
	Param4 *args=static_cast<struct Param4 *>(malloc(sizeof(struct Param4)));
	args->inputPipe1 = &inputPipe1;
	args->file = outFile;
	args->mySchema = &mySchema;
	if(create_join_thread(&worker,thread_work,(void *)args)){
		std::cout<<"error creating thread\n";
	}
}

void* WriteOut::thread_work(void* args){
	struct Param4 *arg = (struct Param4 *)(args); 
	Attribute *atts = arg->mySchema->GetAtts();
	int n = arg->mySchema->GetNumAtts();
	Record rec;
	int cnt=1;
	while(arg->inputPipe1->Remove(&rec)){
		fprintf(arg->file, "%d: ", cnt++);
		char *bits = rec.bits;
		for (int i = 0; i < n; i++) {
			fprintf(arg->file, "%s",atts[i].name);
			int pointer = ((int *) bits)[i + 1];
			fprintf(arg->file, "[");
			if (atts[i].myType == Int) {
				int *myInt = (int *) &(bits[pointer]);
				fprintf(arg->file, "%d",*myInt);
			} else if (atts[i].myType == Double) {
				double *myDouble = (double *) &(bits[pointer]);
				fprintf(arg->file, "%f", *myDouble);
			} else if (atts[i].myType == String) {
				char *myString = (char *) &(bits[pointer]);
				fprintf(arg->file, "%s", myString);
			}
			fprintf(arg->file, "]");
			if (i != n - 1) {
				fprintf(arg->file, ", ");
			}
		}
		fprintf(arg->file, "\n");
	}
}

void Join::Run (Pipe &inputPipe1L, Pipe &inputPipe1R, Pipe &outPipe, CNF &selOp, Record &literal){
	Param4 *args=static_cast<struct Param4 *>(malloc(sizeof(struct Param4)));
	args->inputPipe1 = &inputPipe1L;
	args->inputPipe2 = &inputPipe1R;
	args->cnf=&selOp;
	args->literal=&literal;
	args->outPipe=&outPipe;
	if(create_join_thread(&worker,thread_work,(void *)args)){
		std::cout<<"error creating thread\n";
	}

}

void* Join::thread_work(void* args){
	struct Param4 *arg = (struct Param4 *)(args); 
	OrderMaker orderL;
	OrderMaker orderR;
	vector<Record *> vectorL;
	vector<Record *> vectorR;
	arg->cnf->GetSortOrders(orderL, orderR);

			
	if(orderL.numAtts && orderR.numAtts && orderL.numAtts == orderR.numAtts) {
		Pipe pipeL(100), pipeR(100);
		BigQ *bigQL = new BigQ(*(arg->inputPipe1), pipeL, orderL, RUNLEN);
		BigQ *bigQR = new BigQ(*(arg->inputPipe2), pipeR, orderR, RUNLEN);

		
		
		Record *rcdL = new Record();
		Record *rcdR = new Record();
		ComparisonEngine cmp;

		if(pipeL.Remove(rcdL) && pipeR.Remove(rcdR)) {
			

			int leftAttr = ((int *) rcdL->bits)[1] / sizeof(int) -1;
			int rightAttr = ((int *) rcdR->bits)[1] / sizeof(int) -1;
			int totalAttr = leftAttr + rightAttr;
			int attrToKeep[totalAttr];
			for(int i = 0; i< leftAttr; i++)
				attrToKeep[i] = i;
			for(int i = 0; i< rightAttr; i++)
				attrToKeep[i+leftAttr] = i;
			int joinNum;

			bool leftOK=true, rightOK=true; 
	int num  =0;
			while(leftOK && rightOK) {
				leftOK=false; rightOK=false;
				int cmpRst = cmp.Compare(rcdL, &orderL, rcdR, &orderR);
				switch(cmpRst) {
				case 0: 
				{
	num ++;
					
					Record *rcd1 = new Record(); rcd1->Consume(rcdL);
					Record *rcd2 = new Record(); rcd2->Consume(rcdR);
					vectorL.push_back(rcd1);
					vectorR.push_back(rcd2);
					while(pipeL.Remove(rcdL)) {
						if(0 == cmp.Compare(rcdL, rcd1, &orderL)) { 
							Record *cLMe = new Record();
							cLMe->Consume(rcdL);
							vectorL.push_back(cLMe);
						} else {
							leftOK = true;
							break;
						}
					}
					
					while(pipeR.Remove(rcdR)) {
						if(0 == cmp.Compare(rcdR, rcd2, &orderR)) { 
	
							Record *cRMe = new Record();
							cRMe->Consume(rcdR);
							vectorR.push_back(cRMe);
						} else {
							rightOK = true;
	
							break;
						}
					}
					
					
					Record *lr = new Record(), *rr=new Record(), *jr = new Record();
						
					for(vector<Record *>::iterator itL = vectorL.begin(); itL!=vectorL.end(); itL++) {
						lr->Consume(*itL);
						for(vector<Record *>::iterator itR = vectorR.begin(); itR!=vectorR.end(); itR++) {
							
							if( 1 == cmp.Compare(lr, *itR, arg->literal, arg->cnf)) {
								joinNum++;
								rr->Copy(*itR);
								jr->MergeRecords(lr, rr, leftAttr, rightAttr, attrToKeep, leftAttr+rightAttr, leftAttr);
								arg->outPipe->Insert(jr);
							}
							
						}
						

					}
					for(vector<Record *>::iterator itL = vectorL.begin(); itL!=vectorL.end(); itL++) {
						if(*itL!=NULL)
							delete *itL;
					}
					vectorL.clear();
					for(vector<Record *>::iterator itL = vectorR.begin(); itL!=vectorR.end(); itL++) {
						if(*itL!=NULL)
							delete *itL;
					}
					vectorR.clear();
					break;
				}
				case 1: 
					leftOK = true;

					if(pipeR.Remove(rcdR))
						rightOK = true;
					break;
				case -1:
					rightOK = true;
					if(pipeL.Remove(rcdL))
						leftOK = true;
					break;
				}
			}
		
		}
	} else { 

			int n_pages = 10;

			Record *rcdL = new Record;
			Record *rcdR = new Record;
			Page pageR;
			DBFile dbFileL;
				fType ft = heap;
				dbFileL.Create((char*)"tmpL", ft, NULL);
				dbFileL.MoveFirst();

			int leftAttr, rightAttr, totalAttr, *attrToKeep;

			if(arg->inputPipe1->Remove(rcdL) && arg->inputPipe2->Remove(rcdR)) {
						
				leftAttr = ((int *) rcdL->bits)[1] / sizeof(int) -1;
				rightAttr = ((int *) rcdR->bits)[1] / sizeof(int) -1;
				totalAttr = leftAttr + rightAttr;
				attrToKeep = new int[totalAttr];
				for(int i = 0; i< leftAttr; i++)
					attrToKeep[i] = i;
				for(int i = 0; i< rightAttr; i++)
					attrToKeep[i+leftAttr] = i;


				do {
					dbFileL.Add(*rcdL);

				}while(arg->inputPipe1->Remove(rcdL));



				vector<Record *> vectorR;
				ComparisonEngine cmp;

				bool rMore = true;
				int joinNum =0;
				while(rMore) {
					Record *first = new Record();
					first->Copy(rcdR);
					pageR.Append(rcdR);
					vectorR.push_back(first);
					int rPages = 0;

					rMore = false;
					while(arg->inputPipe2->Remove(rcdR)) {
						
						Record *copyMe = new Record();
						copyMe->Copy(rcdR);
						if(!pageR.Append(rcdR)) {
							rPages += 1;
							if(rPages >= n_pages -1) {
								rMore = true;
								break;
							}
							else {
								pageR.EmptyItOut();
								pageR.Append(rcdR);
								vectorR.push_back(copyMe);
							}
						} else {
							vectorR.push_back(copyMe);
						}
					}
					
					dbFileL.MoveFirst(); 
					
					int fileRN = 0;
					while(dbFileL.GetNext(*rcdL)) {
						for(vector<Record*>::iterator it=vectorR.begin(); it!=vectorR.end(); it++) {
							if(1 == cmp.Compare(rcdL, *it, arg->literal, arg->cnf)) {
								
								joinNum++;
								Record *jr = new Record();
								Record *rr = new Record();
								rr->Copy(*it);
								jr->MergeRecords(rcdL, rr, leftAttr, rightAttr, attrToKeep, leftAttr+rightAttr, leftAttr);
								arg->outPipe->Insert(jr);
							}
						}
					}
					
					
					for(vector<Record *>::iterator itL = vectorR.begin(); itL!=vectorR.end(); itL++) {
						if(*itL!=NULL)
							delete *itL;
					}
					vectorR.clear();
				}
				
				dbFileL.Close();
			}
		}
	arg->outPipe->ShutDown();

 }