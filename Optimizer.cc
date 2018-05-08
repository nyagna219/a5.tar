
#include "Optimizer.h"

Optimizer::Optimizer() {
	
}

Optimizer::Optimizer(
			char* tpch_dir,
			char* dbfile_dir,
			char* catalog_path,
			struct FuncOperator *finalFunction,
			struct TableList *tables,
			struct AndList * boolean,
			struct NameList * pGrpAtts,
	        struct NameList * pAttsToSelect,
	        int distinct_atts, int distinct_func,
	        Statistics *s) {
	this->tpch_dir = tpch_dir;
	this->dbfile_dir = dbfile_dir;
	this->catalog_path = catalog_path;	
	this->finalFunction = finalFunction;
	this->tables = tables;
	this->andlist = boolean;
	this->gatts = pGrpAtts;
	this->satts = pAttsToSelect;
	this->datts = distinct_atts;
	this->dfunc = distinct_func;
	this->statistics = s;
}

void Optimizer::GetJoinsAndSelects(vector<AndList*> &joins, vector<AndList*> &selects,
	vector<AndList*> &selAboveJoin) {
	OrList *orListVar;
	AndList *andListVar = this->andlist;
	for(;andListVar;) {
		orListVar = andListVar->left;
		if(orListVar==NULL) {
			cerr <<"Error: Problem encountered in CNF AndList"<<endl;
			return;
		}
		if(orListVar->left->code == EQUALS && orListVar->left->left->code == NAME
					&& orListVar->left->right->code == NAME){ 
			AndList *andNewVar = new AndList();
			andNewVar->left= orListVar;
			andNewVar->rightAnd = NULL;
			joins.push_back(andNewVar);
		} else {
			if(!orListVar->rightOr) {  

				AndList *andNewVar = new AndList();
				andNewVar->left= orListVar;
				andNewVar->rightAnd = NULL;
				selects.push_back(andNewVar);
			} else { 
				vector<string> requiredTables;
				OrList *olp = orListVar;
				while(orListVar != NULL){
					Operand *op = orListVar->left->left;
					if(op->code != NAME){
						op = orListVar->left->right;
					}
					string rel;
					if(this->statistics->ParseRelation(string(op->value), rel) ==0) {
						cerr <<"Error: Problem encountered in parsing relations"<<endl;
						return;
					}
					if(requiredTables.size() == 0){
						requiredTables.push_back(rel);
					}
					else if(rel.compare(requiredTables[0]) != 0){
							requiredTables.push_back(rel);
					}

					orListVar = orListVar->rightOr;
				}

				if(requiredTables.size() > 1){
					AndList *andNewVar = new AndList();
					andNewVar->left= olp;
					andNewVar->rightAnd = NULL;
					selAboveJoin.push_back(andNewVar);
				}
				else{
					AndList *andNewVar = new AndList();
					andNewVar->left= olp;
					andNewVar->rightAnd = NULL;
					selects.push_back(andNewVar);
				}
			}
		}
		andListVar = andListVar->rightAnd;
	}
}

map<string, AndList*> Optimizer::OptimizeSelectAndApply(vector<AndList*> selects) {
	map<string, AndList*> selectors;
	for(auto iter=selects.begin(); iter!=selects.end();iter++) {
		AndList *andListVar = *iter;
		Operand *op = andListVar->left->left->left;
		if(op->code != NAME)
			op = andListVar->left->left->right;
		string rel;
		this->statistics->ParseRelation(string(op->value), rel);
		auto mapiter=selectors.begin();
		for(; mapiter!=selectors.end(); mapiter++) {
			if(mapiter->first.compare(rel) == 0) {
				AndList *endAnd = mapiter->second;
				while(endAnd->rightAnd!=NULL)
					endAnd = endAnd->rightAnd;
				endAnd->rightAnd = andListVar;
				break;
			}
		}
		if(mapiter == selectors.end())
			selectors.insert(make_pair(rel, andListVar));
	}
	return selectors;
}

vector<AndList*> Optimizer::OptimizeJoinOrder(vector<AndList*> joins) {
	vector<AndList*> sortedAndListVec;
	sortedAndListVec.reserve(joins.size());
	if(joins.size() <=1 ) {
		if(joins.size()==1)
			sortedAndListVec.push_back(joins[0]);
	} else {
		int size = joins.size(),i=0;
		while(i<size) {
			int joinNum = 2;
			double smallest = 0.0;
			string left_rel, right_rel;
			AndList *selectAndList;
			int selectPos = -1,j=0;
			while(j<joins.size()){
				string rel1, rel2;
				AndList *andListVal = joins[j];
				Operand *l = andListVal->left->left->left;
				Operand *r = andListVal->left->left->right;
				this->statistics->ParseRelation(string(l->value), rel1);
				this->statistics->ParseRelation(string(r->value), rel2);
				char *estrels[] = {(char*)rel1.c_str(), (char*)rel2.c_str()};
				double cost = this->statistics->Estimate(andListVal,estrels, 2);
				if(selectPos == -1 || cost < smallest) {
					smallest = cost;
					left_rel = rel1;
					right_rel = rel2;
					selectAndList = andListVal;
					selectPos = j;
				}
				j++;
			}
			sortedAndListVec.push_back(selectAndList);
			char *aplyrels[] = {(char*) left_rel.c_str(), (char*)right_rel.c_str()};
			this->statistics->Apply(selectAndList, aplyrels, 2);
			joins.erase(joins.begin()+selectPos);
			i++;
		}
		return sortedAndListVec;
	}
}

OrderMaker *Optimizer::GenerateOM(Schema *schema) {
	OrderMaker *orderObj = new OrderMaker();
	NameList *name = this->gatts;
	while(name) {
		orderObj->whichTypes[orderObj->numAtts] = schema->FindType(name->name);
		orderObj->whichAtts[orderObj->numAtts] = schema->Find(name->name);
		orderObj->numAtts++;
		name=name->next;
	}
	return orderObj;
}
Function *Optimizer::GenerateFunc(Schema *schema) {
	Function *functionObj = new Function();
	functionObj->GrowFromParseTree(this->finalFunction, *schema);
	return functionObj;
}


QueryPlan * Optimizer::OptimizedQueryPlan() {
	TableList *listOfTables = tables;
	while(listOfTables) {
		if(listOfTables->aliasAs) {
			this->statistics->CopyRel(listOfTables->tableName, listOfTables->aliasAs);
		}
		listOfTables = listOfTables->next;
	}
	vector<AndList*> joins;
	vector<AndList*> selects, selAboveJoin;
	
	GetJoinsAndSelects(joins, selects, selAboveJoin);
	map<string, AndList*> selectors = this->OptimizeSelectAndApply(selects);
	vector<AndList*> optimisedOrderedJoins = this->OptimizeJoinOrder(joins);
	QueryPlan *queryPlan = new QueryPlan(tpch_dir, dbfile_dir,catalog_path);
	
	map<string, QueryPlanNode *> mapForselect; 

	for(TableList *table=this->tables; table!=NULL; table=table->next) {
		QueryPlanNode *selectFile = new QueryPlanNode;
		selectFile->opType = SELECTF;
		char name[100];
		sprintf(name, "%s%s.bin", dbfile_dir, table->tableName);

		selectFile->dbfilePath = string(name);
		selectFile->opid = queryPlan->noofpip++;

		selectFile->outschema = new Schema(&catalog_path[0u], table->tableName);

		string relName(table->tableName);
		if(table->aliasAs) { 
			selectFile->outschema->AdjustSchemaWithAlias(table->aliasAs);
			relName = string(table->aliasAs);
		}
		auto iter = selectors.begin();
		for(; iter!=selectors.end(); iter++) {
			if(relName.compare(iter->first)==0)
				break;
		}
		AndList *andListVal;
		if(iter==selectors.end())
			andListVal = NULL;
		else
			andListVal = iter->second;
		selectFile->cnf->GrowFromParseTree(andListVal, selectFile->outschema, *(selectFile->lrec));

		mapForselect.insert(make_pair(relName, selectFile));
	}
	map<string, QueryPlanNode *> cJoinsMap;
	QueryPlanNode *forJoin = NULL;
	if(optimisedOrderedJoins.size()>0) {
		for(auto joinIter=optimisedOrderedJoins.begin(); joinIter!=optimisedOrderedJoins.end(); joinIter++) {
			AndList *andListVar = *joinIter;
			Operand *lAttrib = andListVar->left->left->left;
			string lRelation; this->statistics->ParseRelation(string(lAttrib->value), lRelation);

			Operand *rAttrib = andListVar->left->left->right;
			string rRelation; this->statistics->ParseRelation(string(rAttrib->value), rRelation);

			forJoin = new QueryPlanNode;
			forJoin->opType = JOIN;

			QueryPlanNode *lUpMost = cJoinsMap[lRelation];
			QueryPlanNode *rUpMost = cJoinsMap[rRelation];
			if(!lUpMost && !rUpMost) { 
				forJoin->lnode = mapForselect[lRelation];
				forJoin->rnode = mapForselect[rRelation];
				forJoin->outschema = new Schema(forJoin->lnode->outschema, forJoin->rnode->outschema);
			} else if(lUpMost) { 
				while(lUpMost->pnode )
					lUpMost = lUpMost->pnode;
				forJoin->lnode = lUpMost; lUpMost->pnode = forJoin;
				forJoin->rnode = mapForselect[rRelation];
			} else if(rUpMost) { 
				while(rUpMost->pnode)
					rUpMost = rUpMost->pnode;
				forJoin->lnode = rUpMost; rUpMost->pnode = forJoin;
				forJoin->rnode = mapForselect[lRelation];
			} else { 
				while(lUpMost->pnode )
					lUpMost = lUpMost->pnode;
				while(rUpMost->pnode)
					rUpMost = rUpMost->pnode;
				forJoin->lnode = lUpMost;
				lUpMost->pnode = forJoin;
				forJoin->rnode  = rUpMost;
				rUpMost->pnode = forJoin;
			}

			cJoinsMap[lRelation] = forJoin;
			cJoinsMap[rRelation] = forJoin;
			forJoin->lpid = forJoin->lnode->opid;
			forJoin->rpid = forJoin->rnode->opid;
			forJoin->outschema = new Schema(forJoin->lnode->outschema, forJoin->rnode->outschema);
			forJoin->opid = queryPlan->noofpip++;

			forJoin->cnf->GrowFromParseTree(andListVar, forJoin->lnode->outschema, forJoin->rnode->outschema, *(forJoin->lrec));
		}
	}
	
	QueryPlanNode *selectAbvJoin = NULL;
	if(selAboveJoin.size() > 0 ) {
		selectAbvJoin = new QueryPlanNode;
		selectAbvJoin->opType = SELECTP;
		if(forJoin==NULL) {
			selectAbvJoin->lnode = mapForselect.begin()->second;
		} else {
			selectAbvJoin->lnode = forJoin;
		}
		selectAbvJoin->lpid = selectAbvJoin->lnode->opid;
		selectAbvJoin->opid = queryPlan->noofpip++;
		selectAbvJoin->outschema = selectAbvJoin->lnode->outschema;
		AndList *andListVal = *(selAboveJoin.begin());
		for(vector<AndList*>::iterator iter=selAboveJoin.begin(); iter!= selAboveJoin.end(); iter++) {
			if(iter!=selAboveJoin.begin()) {
				andListVal->rightAnd = *iter;
			}
		}
		selectAbvJoin->cnf->GrowFromParseTree(andListVal, selectAbvJoin->outschema, *(selectAbvJoin->lrec));
	}

	QueryPlanNode *forGroupBy = NULL;
	if(this->gatts) {
		forGroupBy = new QueryPlanNode;
		forGroupBy->opType = GROUP_BY;
		if(selectAbvJoin) {
			forGroupBy ->lnode = selectAbvJoin;
		} else if(forJoin) {
			forGroupBy->lnode = forJoin;
		} else {
			forGroupBy->lnode = mapForselect.begin()->second;
		}
		forGroupBy->lpid = forGroupBy->lnode->opid;
		forGroupBy->opid = queryPlan->noofpip++;

		forGroupBy->omake = this->GenerateOM(forGroupBy->lnode->outschema);
		forGroupBy->function = this->GenerateFunc(forGroupBy->lnode->outschema);

		Attribute *attr = new Attribute[1];
		attr[0].name = (char *)"forSum";
		attr[0].myType = Double;
		Schema *sumSchema = new Schema ((char *)"dummy", 1, attr);

		NameList *attribName = this->gatts;
		int numGroupAttrs = 0;
		while(attribName) {
			numGroupAttrs ++;
			attribName = attribName->next;
		}
		if(numGroupAttrs == 0) {
			forGroupBy->outschema = sumSchema;
		} else {
			Attribute *attrs = new Attribute[numGroupAttrs];
			int i = 0;
			attribName = this->gatts;
			while(attribName) {
				attrs[i].name = strdup(attribName->name);
				attrs[i++].myType = forGroupBy->lnode->outschema->FindType(attribName->name);
				attribName = attribName->next;
			}
			Schema *outSchema = new Schema((char *)"dummy", numGroupAttrs, attrs);
			forGroupBy->outschema = new Schema(sumSchema, outSchema);
		}
	}

	QueryPlanNode *forSum = NULL;
	if(forGroupBy == NULL && this->finalFunction!=NULL) {

		forSum = new QueryPlanNode;
		forSum->opType = SUM;
		if(selectAbvJoin)
			forSum->lnode = selectAbvJoin;
		else if(forJoin)
			forSum->lnode = forJoin;
		else
			forSum->lnode = mapForselect.begin()->second;
		forSum->lpid = forSum->lnode->opid;
		forSum->opid = queryPlan->noofpip++;
		forSum->function = this->GenerateFunc(forSum->lnode->outschema);

		Attribute *attr = new Attribute[1];
		attr[0].name = (char *)"forSum";
		attr[0].myType = Double;

		forSum->outschema = new Schema ((char *)"dummy", 1, attr);
	}

	QueryPlanNode *forProject = new QueryPlanNode;
	forProject->opType = PROJECT;
	int outputNum = 0;
	NameList *name = this->satts;
	Attribute *opAttribs;
	while(name) {  
		name = name->next;
		outputNum++;
	}
	int ithAttr = 0;
	if(forGroupBy) {
			forProject->lnode = forGroupBy;
			outputNum++;
			forProject->knum = new int[outputNum];
			forProject->knum[0] = forGroupBy->outschema->Find((char *)"forSum");
			opAttribs = new Attribute[outputNum+1];
			opAttribs[0].name = (char *)"forSum";
			opAttribs[0].myType = Double;
			ithAttr = 1;
	}else if(forSum) { 
		forProject->lnode = forSum;
		outputNum++;
		forProject->knum = new int[outputNum];
		forProject->knum[0] = forSum->outschema->Find((char *) "forSum");
		opAttribs = new Attribute[outputNum];
		opAttribs[0].name = (char*)"forSum";
		opAttribs[0].myType = Double;
		ithAttr = 1;
	}else if(forJoin) {
		forProject->lnode = forJoin;
		if(outputNum == 0) {
			cerr <<"Error: select was not assigned any attributes"<<endl;
			return NULL;
		}
		forProject->knum = new int[outputNum];
		opAttribs = new Attribute[outputNum];
	} else {
		forProject->lnode = mapForselect.begin()->second;
		if(outputNum == 0) {
			cerr <<"Error: select was not assigned any attributes"<<endl;
			return NULL;
		}
		forProject->knum = new int[outputNum];
		opAttribs = new Attribute[outputNum];
	}
	name = this->satts;
	while(name) {
		forProject->knum[ithAttr] = forProject->lnode->outschema->Find(name->name);
		opAttribs[ithAttr].name = name->name;
		opAttribs[ithAttr].myType = forProject->lnode->outschema->FindType(name->name);
		ithAttr++;
		name = name->next;
	}
	forProject->inpatt = forProject->lnode->outschema->GetNumAtts();
	forProject->outatt = outputNum;
	forProject->lpid = forProject->lnode->opid;
	forProject->opid = queryPlan->noofpip++;
	forProject->outschema = new Schema((char*)"dummy", outputNum, opAttribs);

	queryPlan->root = forProject;

	
	QueryPlanNode *distinct = NULL;
	if(this->datts) { 

		distinct = new QueryPlanNode;
		distinct->opType = DISTINCT;
		distinct->lnode = forProject;
		distinct->lpid = distinct->lnode->opid;
		distinct->outschema = distinct->lnode->outschema;
		distinct->opid = queryPlan->noofpip++;
		queryPlan->root = distinct;
	}

	return queryPlan;
}
Optimizer::~Optimizer() {	
}

