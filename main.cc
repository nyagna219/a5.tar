#include <iostream>
#include <fstream>
#include "Defs.h"
#include "Optimizer.h"
#include "QueryPlan.h"
#include "ParseTree.h"
#include <time.h>
#include "test.h"

using namespace std; 
extern "C" {
	int yyparse(void);
	int yyfuncparse(void); 
}

extern struct CreateTable *createTable;
extern struct InsertFile *insertFile;
extern char *dropTableName;
extern char *setOutPut;

extern struct FuncOperator *finalFunction;
extern struct TableList *tables;
extern struct AndList *boolean; 
extern struct NameList *groupingAtts; 
extern struct NameList *attsToSelect; 
extern int distinctAtts; 
extern int distinctFunc;  

extern int quit;

int main() {
	setup();
	while(1){
	yyparse();
	QueryPlan *queryPlan = new QueryPlan(tpch_dir, dbfile_dir,catalog_path);
	//
	if(quit) {
		return 0;
	}
	//retrive the output target from output_path

	if(createTable){
		if(queryPlan->ExecuteCreateTable(createTable)) {
			cout <<"Created table"<<createTable->tableName<<endl;
		}
	}else if(insertFile) {
		if(queryPlan->ExecuteInsertFile(insertFile)) {
			cout <<"Loaded file "<<insertFile->fileName<<" into " <<insertFile->tableName<<endl;
		}
	} else if(dropTableName) {
		if(queryPlan->ExecuteDropTable(dropTableName)) {
			cout <<"Dropped dbfile"<<dropTableName<<endl;
		}
	} else if(setOutPut) {
		queryPlan->output = setOutPut;
		ofstream f(string(dbfile_dir)+"database");
		f << setOutPut;
		f.close();
		cout <<"Setted output to "<<setOutPut<<endl;
	} else if(tables){ // query
	//now we have all the info in the above data structure
		Statistics *s = new Statistics();
		s->LoadAllStatistics();
		Optimizer optimizer(tpch_dir, dbfile_dir,catalog_path, finalFunction, tables, boolean, groupingAtts,
					attsToSelect, distinctAtts, distinctFunc, s);
		queryPlan =	optimizer.OptimizedQueryPlan();
		if(queryPlan == NULL) {
			cerr <<"Error: Building of query plan failed. Exiting with error code 0."<<endl;
			exit(0);
		}
		//queryPlan->PrintInOrder();
		time_t t1;
		time(&t1);
		queryPlan->ExecuteQueryPlan();
		time_t t2;
		time(&t2);
		cout <<"Execution took "<<difftime(t2, t1)<<" seconds!"<<endl;
		}
	}
	return 1;
	
}