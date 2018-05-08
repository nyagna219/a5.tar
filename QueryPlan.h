#ifndef QUERYPLAN_H_
#define QUERYPLAN_H_
#include "RelOp.h"
#include "Pipe.h"
#include <map>
#include <string.h>
#include "ParseTree.h"
#include "DBFile.h"
#include <iostream>
#include <fstream>
#include <vector>

#define PIPE_SIZE 100
using namespace std;
enum QueryNodeType {SELECTF, SELECTP, PROJECT, JOIN, SUM, GROUP_BY, DISTINCT, WRITEOUT};
class QueryPlanNode {
public: 
	QueryNodeType opType;
	QueryPlanNode *pnode;
	QueryPlanNode *lnode;
	QueryPlanNode *rnode;
	int lpid;
	int rpid;
	int opid;
	FILE *outFile;
	string dbfilePath;
	CNF *cnf;
	Record *lrec;
	Schema *outschema;
	Function *function;
	OrderMaker *omake;
	int *knum;
	int inpatt, outatt;
	QueryPlanNode();
	~QueryPlanNode();
};
class QueryPlan {
public:
	QueryPlan(char*,char*,char*);
	virtual ~QueryPlan();
	char* tpch_dir; 
	char *dbfile_dir;
	char  *catalog_path;
	QueryPlanNode *root;
	int noofpip;
	map<int, Pipe*> pipes;
	vector<RelationalOp *> operators;
	int noofdb;
	DBFile *dbs[10];
	char* output;
	void PrintNode(QueryPlanNode *);
	void PrintInOrder();
	void ExecuteNode(QueryPlanNode *);
	int ExecuteQueryPlan();
	int ExecuteCreateTable(CreateTable*);
	int ExecuteInsertFile(InsertFile*);
	int ExecuteDropTable(char *);
};
#endif
