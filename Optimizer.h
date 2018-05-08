#ifndef OPTIMIZER_H_
#define OPTIMIZER_H_

#include <map>
#include <vector>
#include <string>
#include <string.h>
#include <iostream>
#include "ParseTree.h"
#include "Statistics.h"
#include "QueryPlan.h"
#include "Record.h"
#include <stdio.h>
#include "Schema.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "Function.h"

class Optimizer {
private:
	char* tpch_dir;
	char* dbfile_dir;
	char* catalog_path;
	struct FuncOperator * finalFunction;
	struct TableList * tables;   
	struct AndList *andlist;  
	struct NameList * gatts; 
	struct NameList * satts; 
	int datts; 
	int dfunc;  

	Statistics *statistics;
	void GetJoinsAndSelects(vector<AndList*> &joins, vector<AndList*> &selects,
				vector<AndList*> &selAboveJoin);

	map<string, AndList*> OptimizeSelectAndApply(vector<AndList*> selects);
	vector<AndList*> OptimizeJoinOrder(vector<AndList*> joins);
	Function *GenerateFunc(Schema *schema);
	OrderMaker *GenerateOM(Schema *schema);
public:
	Optimizer();
	Optimizer(
			char* tpch_dir,
			char* dbfile_dir,
			char* catalog_path,
			struct FuncOperator *finalFunction,
			struct TableList *tab,
			struct AndList * boolean,
			struct NameList * nmlist,
	        struct NameList * pattlist,
	        int dattnum, int dfuncnum, Statistics *s);

	QueryPlan * OptimizedQueryPlan();
	virtual ~Optimizer();
};

#endif /* OPTIMIZER_H_ */
