#include "QueryPlan.h"

QueryPlanNode::QueryPlanNode() {
	this->pnode = NULL;
	this->lnode = NULL;
	this->rnode = NULL;
	this->cnf = new CNF;
	this->lrec = new Record;
}
QueryPlanNode::~QueryPlanNode(){
	delete cnf;
	delete lrec;
}
QueryPlan::QueryPlan(char* tpch_dir, char *dbfile_dir, char *catalog_path){
	this->tpch_dir = tpch_dir;
	this->dbfile_dir = dbfile_dir;
	this->catalog_path = catalog_path;
	this->output = output;
}
QueryPlan::~QueryPlan() {
}
void QueryPlan::PrintInOrder() {
	PrintNode(root);
}
void QueryPlan::PrintNode(QueryPlanNode *node) {
	if(node->lnode)
		PrintNode(node->lnode);
	switch(node->opType) {
	case PROJECT:
	
		cout <<"Project Operation"<<endl;
		cout <<"Input Pipe:	"<<node->lpid<<endl;
		cout <<"Output Pipe: "<<node->opid<<endl;
		cout <<"Output Schema: " <<endl;
		node->outschema->Print();
		cout <<"Attributes to keep: "<<endl;
		cout <<"\t";
		for(int i=0;i<node->outatt;i++) {
			cout <<node->knum[i] <<", ";
		}
		cout <<endl;
		cout <<"\n";
		break;
	case JOIN:
		cout <<"Join Operation"<<endl;
		cout <<"Left Input Pipe: "<<node->lpid<<endl;
		cout <<"Right Input Pipe: "<<node->rpid<<endl;
		cout <<"Output Pipe: "<<node->opid<<endl;
		cout <<"Output Schema: " <<endl;
			node->outschema->Print();
		cout <<"Select CNF: " <<endl;
		cout <<"\t"; node->cnf->Print();
		cout <<"\n\n";
		break;
	case DISTINCT:
		cout <<"Duplicate Removal Operation"<<endl;
		cout <<"Input Pipe:	"<<node->lpid<<endl;
		cout <<"Output Pipe: "<<node->opid<<endl;
		cout <<"Output Schema: " <<endl;
		node->outschema->Print();
		cout <<"\n";
		break;
	case SELECTF:
		
		cout <<"SelectFromFile Operation"<<endl;
		cout <<"Input File:	"<<node->dbfilePath<<endl;
		cout <<"Output Pipe: "<<node->opid<<endl;
		cout <<"Output Schema: " <<endl;
			node->outschema->Print();
		cout <<"Select CNF: " <<endl;
		cout <<"\t"; node->cnf->Print();
		cout <<"\n\n";
		break;
	case SELECTP:
	
		cout <<"SelectFromPipe Operation"<<endl;
		cout <<"Input Pipe:	"<<node->lpid<<endl;
		cout <<"Output Pipe: "<<node->opid<<endl;
		cout <<"Output Schema: " <<endl;
			node->outschema->Print();
		cout <<"Select CNF: " <<endl;
		cout <<"\t"; node->cnf->Print();
		cout <<"\n\n";
		break;
	case SUM:
		cout <<"Sum Operation"<<endl;
		cout <<"Input Pipe:	"<<node->lpid<<endl;
		cout <<"Output Pipe: "<<node->opid<<endl;
		cout <<"Output Schema: " <<endl;
		node->outschema->Print();
		cout <<"Sum Function: " <<endl;
		node->function->Print();
		cout <<endl;
		cout <<"\n";
		break;
	case GROUP_BY:
		cout <<"GroupBy Operation"<<endl;
		cout <<"Input Pipe:	"<<node->lpid<<endl;
		cout <<"Output Pipe: "<<node->opid<<endl;
		cout <<"Output Schema: " <<endl;
		node->outschema->Print();
		cout <<"Group By OrderMaker: " <<endl;
		node->omake->Print();
		cout <<endl;
		cout <<"Group By Function: " <<endl;
		node->function->Print();
		cout <<endl;
		cout <<"\n";
		break;
	case WRITEOUT:
		cout <<"Write Out"<<endl;
		cout <<"Input Pipe:	"<<node->lpid<<endl;
		cout <<"Output Schema: " <<endl;
		node->outschema->Print();
		cout <<"\n";
		break;
	default:
		break;
	}

	if(node->rnode)
		PrintNode(node->rnode);
}
int QueryPlan::ExecuteCreateTable(CreateTable *createTable) {
	DBFile *db = new DBFile;
	char dbpath[100];
	sprintf(dbpath, "%s%s.bin", dbfile_dir, createTable->tableName);
	SortInfo *info = new SortInfo;
	OrderMaker *om = new OrderMaker;
	if(createTable && createTable->type == SORTED) {
		NameList *sortAtt = createTable->sortAttrList;
		while(sortAtt) {
			AttrList *atts = createTable->attrList;
			int i=0;
			while(atts) {
				if(strcmp(sortAtt->name, atts->attr->attrName)){
					om->whichAtts[om->numAtts] = i;
					om->whichTypes[om->numAtts] = (Type) atts->attr->type;
					om->numAtts++;
					break;
				}
				i++;
				atts = atts->next;
			}
			sortAtt = sortAtt->next;
		}
		info->myorder = om;
		info->runLength = RUNLEN;
		db->Create(dbpath, sorted, (void*)info);
	} else
		db->Create(dbpath, heap, NULL );
	db->Close();
	return 1;
}
int QueryPlan::ExecuteInsertFile(InsertFile *insertFile) {
	if(!insertFile)
		return 0;
	DBFile dbfile;
	char dbpath[100];
	sprintf(dbpath, "%s%s.bin", dbfile_dir, insertFile->tableName);
	dbfile.Open(dbpath);
	char fpath[100];
	sprintf(fpath, "%s%s", tpch_dir, insertFile->fileName);
	cout <<"loading " <<fpath<<endl;
	Schema schema((char*)catalog_path, insertFile->tableName);
	dbfile.Load(schema, fpath);
	dbfile.Close();
	return 1;
}
int QueryPlan::ExecuteDropTable(char *dropTable) {
	if(!dropTable)
		return 0;
	char dbpath[100];
	sprintf(dbpath, "%s%s.bin", dbfile_dir, dropTable);
	remove(dbpath);
	sprintf(dbpath, "%s.header", dbpath);
	remove(dbpath);
	return 1;
}
void QueryPlan::ExecuteNode(QueryPlanNode *node) {
	if(!node)
		return;
	if(node->lnode)
		ExecuteNode(node->lnode);

	if(node->rnode)
		ExecuteNode(node->rnode);
	switch(node->opType) {
	case SELECTF:
	{
		cout <<"execute selectfrom file: " <<node->dbfilePath<<endl;
		SelectFile *selectFile = new SelectFile();
		Pipe *sfOutPipe = new Pipe(PIPE_SIZE);
		this->pipes[node->opid] = sfOutPipe;
		dbs[this->noofdb] = new DBFile;
		dbs[this->noofdb]->Open((char*)node->dbfilePath.c_str());
		dbs[this->noofdb]->MoveFirst();
		selectFile->Run(*(dbs[this->noofdb++]), *sfOutPipe, *(node->cnf), *(node->lrec));
		break;
	}
	case SELECTP:
	{
		SelectPipe *selectPipe = new SelectPipe();
		selectPipe->Use_n_Pages(RUNLEN);
		Pipe *spOutPipe = new Pipe(PIPE_SIZE);
		this->pipes[node->opid] = spOutPipe;
		Pipe *splPipe = this->pipes[node->lpid];
		selectPipe->Run(*splPipe, *spOutPipe, *(node->cnf), *(node->lrec));
		break;
	}
	case PROJECT:
	{
		cout <<"Project running"<<endl;
		Project *project = new Project;
		Pipe *pOutPipe = new Pipe(PIPE_SIZE);
		this->pipes[node->opid] = pOutPipe;
		Pipe *plPipe = this->pipes[node->lpid];
		project->Run(*plPipe, *pOutPipe, node->knum, node->inpatt, node->outatt);
		break;
	}
	case JOIN:
	{
		cout <<"Join running"<<endl;
		Join *join = new Join;
		Pipe *jOutPipe = new Pipe(PIPE_SIZE);
		this->pipes[node->opid] = jOutPipe;
		Pipe *jlPipe = this->pipes[node->lpid];
		Pipe *jrPipe = this->pipes[node->rpid];
		join->Run(*jlPipe, *jrPipe, *jOutPipe, *(node->cnf), *(node->lrec));
		break;
	}
	case SUM:
	{
		cout <<" Sum running"<<endl;
		Sum *sum = new Sum;
		Pipe *sOutPipe = new Pipe(PIPE_SIZE);
		this->pipes[node->opid] = sOutPipe;
		Pipe *slPipe = this->pipes[node->lpid];
		sum->Run(*slPipe, *sOutPipe, *(node->function));
		break;
	}
	case GROUP_BY:
	{
		cout <<"Group BY running"<<endl;
		GroupBy *groupBy = new GroupBy;
		Pipe *gbOutPipe = new Pipe(PIPE_SIZE);
		this->pipes[node->opid] = gbOutPipe;
		Pipe *gblPipe = this->pipes[node->lpid];
		groupBy->Run(*gblPipe, *gbOutPipe, *(node->omake), *(node->function));
		break;
	}
	case DISTINCT:
	{
		cout <<"Distinct running"<<endl;
		DuplicateRemoval *dr = new DuplicateRemoval;
		Pipe *drOutPipe = new Pipe(PIPE_SIZE);
		this->pipes[node->opid] = drOutPipe;
		Pipe *drlPipe = this->pipes[node->lpid];
		dr->Run(*drlPipe, *drOutPipe, *(node->lnode->outschema));
		break;
	}
	case WRITEOUT:
	{
		cout <<" writeout running"<<endl;
		WriteOut *wo = new WriteOut;
		Pipe *wlPipe = this->pipes[node->lpid];
		wo->Run(*wlPipe, node->outFile, *(node->outschema));
		this->operators.push_back(wo);
		break;
	}
	default:
		break;
	}
}
int QueryPlan::ExecuteQueryPlan() {
	string output;
	ifstream f("database");
	f >> output;
	if( output ==  "NONE") { 
		cout<<0<<endl;
		this->PrintInOrder();
	} else {
		QueryPlanNode *writeOut = new QueryPlanNode;
		writeOut->opType = WRITEOUT;
		writeOut->lnode = this->root;
		writeOut->lpid = writeOut->lnode->opid;
		writeOut->outschema = writeOut->lnode->outschema;
		if(output == "STDOUT") {
			writeOut->outFile = stdout;
		} else {
			FILE *fp = fopen("Yagna", "w");
			writeOut->outFile = fp;
		}
		this->ExecuteNode(writeOut);
		for(vector<RelationalOp *>::iterator roIt=this->operators.begin(); roIt!=this->operators.end();roIt++){
			RelationalOp *op = *roIt;
			op->WaitUntilDone();
		}
	}
	return 1;
}

