#include <string>
#include <cstdlib>
#include <sstream>
#include <stdio.h>
#include "BigQ.h"

using namespace std;

int BigQ::cnt= 0;


 BigQ :: BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen):
 in(in), out(out), so1(sortorder), runlen(runlen),so2(sortorder){
currpage = new Page();
	theFile = new File();
	std::ostringstream ss;
	ss << rand();

	string tmpfilestr = ss.str();

	string str = "never" + tmpfilestr;
	char* s = new char[str.size() + 1];
	copy(str.begin(), str.end(), s);
	s[str.size()] = '\0';
	theFile->Open(0, s);
	int thr = pthread_create(&worker, NULL, Working, (void *)this);
	if(thr){
		cout<<"Error creatin thread";
		exit(1);
	}
}

BigQ::~BigQ () {
	theFile->Close();
	delete currpage;
	delete theFile;
}

void * BigQ::Working(void *q){
	BigQ *bq = (BigQ *)q;
	auto ce = [&](Record *temp1, Record *temp2){
		ComparisonEngine cme;
		if(cme.Compare(temp1, temp2, &bq->so1) < 0)
			return true;
		else 
			return false;
};
	auto pqcomp = [&](pair<int, Record *> temp1, pair<int, Record *> temp2){
		ComparisonEngine cme;
		if(cme.Compare((temp1.second), (temp2.second), &bq->so2) < 0)
			return false;
		else 
			return true;
};
	Record temp;
	int curSize = 0;
	int curPage = 0;

	vector<Record *> record;
	vector<int> rhead;

	for(;(bq->in).Remove(&temp);){
		char *b = temp.GetBits();

		if(curSize + ((int *)b)[0] < (PAGE_SIZE) * bq->runlen){
			
			Record *put = new Record();
			put->Consume(&temp);
			record.push_back(put);
			curSize += ((int *)b)[0];
						
		}else{
			
			sort (record.begin(), record.end(), ce);

			rhead.push_back(curPage);

			for(auto it=record.begin();it!=record.end();){
				if(!(bq->currpage)->Append((*it))){
					(bq->theFile)->AddPage(bq->currpage,(bq->theFile)->lastone());
					(bq->currpage)->EmptyItOut();
					curPage++;
				}else
					it++;

			}
			if((bq->currpage)->numrecord() != 0){
				(bq->theFile)->AddPage(bq->currpage,(bq->theFile)->lastone());
				(bq->currpage)->EmptyItOut();
				curPage++;
			}
	
		
			for(auto x:record)
				delete x;
			
			record.erase(record.begin(),record.end());
			Record *put = new Record();
			put->Consume(&temp);
			record.push_back(put);
			curSize = ((int *) b)[0];
		}

	}

	if(!record.empty()){
		
			sort (record.begin(), record.end(), ce);

			rhead.push_back(curPage);

			auto it=record.begin();
			while(it!=record.end()){

				if(!(bq->currpage)->Append((*it))){
					(bq->theFile)->AddPage(bq->currpage,(bq->theFile)->lastone());
					(bq->currpage)->EmptyItOut();
					curPage++;
				}else
					it++;

			}
			if((bq->currpage)->numrecord() != 0){
				(bq->theFile)->AddPage(bq->currpage,(bq->theFile)->lastone());
				(bq->currpage)->EmptyItOut();
				curPage++;
			}	

			
			for(auto x:record)
				delete x;

			record.clear();
			
	}
	rhead.push_back(curPage);


	
	
 	priority_queue<pair<int, Record*>, vector<pair<int, Record*> >, decltype (pqcomp)> PQueue(pqcomp);
 	vector<int> runcur(rhead);
 	vector<Page *> runpagelist;

 	for(int i=0; i<rhead.size()-1; i++){
 		Page *temp_P = new Page();
 		(bq->theFile)->GetPage(temp_P, rhead[i]);
 		Record *temp_R = new Record();
 		temp_P->GetFirst(temp_R);

 		PQueue.push(make_pair(i,temp_R));
 		runpagelist.push_back(temp_P);
 	}
 	while(!PQueue.empty()){
 		
 		int temp_I = PQueue.top().first;
 		Record *temp_R = PQueue.top().second;
 		PQueue.pop();
 		(bq->out).Insert(temp_R);

 		
 		if(!runpagelist[temp_I]->GetFirst(temp_R)){
 			if(++runcur[temp_I]<rhead[temp_I+1]){
				runpagelist[temp_I]->EmptyItOut();
 				(bq->theFile)->GetPage(runpagelist[temp_I], runcur[temp_I]);
 				runpagelist[temp_I]->GetFirst(temp_R);
 				PQueue.push(make_pair(temp_I,temp_R));
 			}
 		}else{
 			PQueue.push(make_pair(temp_I,temp_R));
 		}
		

 	}
	for(int i=0; i<runpagelist.size(); i++){
		delete runpagelist[i];
	}
	
    
	(bq->out).ShutDown ();
}