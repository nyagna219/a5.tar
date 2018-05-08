#ifndef STATISTICS_
#define STATISTICS_
#include "ParseTree.h"
#include <vector>
#include <string>
#include <unordered_map>
#include <fstream>
#include <iostream>
#include <unordered_set>
#include <sstream>
#include <map>
using namespace std;
struct structure1{
	int numTuples;
	map<string,double> attSet;
	structure1(){};
	structure1(int numAttr):numTuples(numAttr){}
	void AddRel(char *attrName,int numDistincts)
	{
		attSet[string(attrName)]=numDistincts;
	}
	void operator= (const structure1& copy){
		this->numTuples=copy.numTuples;
		this->attSet=copy.attSet;
	}
	void AddAtt(string Name,int val){
		attSet[Name]=val;
	}
	friend std::ostream& operator<<(std::ostream& os, const structure1& relation);
  	friend std::istream& operator>>(std::istream& is, structure1& Rel);
};
class Statistics
{
	unordered_map<string,structure1> relation;
	unordered_map<string,double> estimate;
	std::unordered_set<string> split(string s){
		stringstream ss(s);
		string line;
		unordered_set<string> res;
		while(getline(ss,line,'#')){
			res.insert(line);
		}
		return res;
	}
	bool checkRel(char *relName[],int numJoin){
		unordered_set<string> st;
		for(auto i=0;i<numJoin;i++)
			st.insert(string(relName[i]));
		for(auto i=0;i<numJoin;i++){
			string rel(relName[i]);
			for(auto iter:estimate){
				auto check=split(iter.first);
				if(check.count(rel)){
					for(auto i:check){
						if(!st.count(i)){
							return false;
						}
					}
				}
			}
		}
		return true;
	}
	bool checkAtt(struct Operand *left, double &t, char **relNames, int numToJoin){
	string operand(left->value);
	if(left->code == 4){
			bool found = false;
			for(int i=0; i<numToJoin; i++){
				string relname(relNames[i]);				
				if(relation[relname].attSet.count(operand)){
						found = true;
						t = relation[relname].attSet[operand]!=-1?relation[relname].attSet[operand]:t;
						return true;
				}
			}
			if(!found){
						cout<<operand<<" not found!!"<<endl;
						return false;
			}
	}
	return true;
}
public:
	Statistics();
	Statistics(Statistics &copyMe);
	~Statistics();

	int ParseRelation(string name, string &rel);

	void LoadAllStatistics();
	void AddRel(char *relName, int numTuples);
	void AddAtt(char *relName, char *attName, int numDistincts);
	void CopyRel(char *oldName, char *newName);
	
	void Read(char *fromWhere);
	void Write(char *fromWhere);

	void  Apply(struct AndList *parseTree, char *relNames[], int numToJoin);
	double Estimate(struct AndList *parseTree, char **relNames, int numToJoin);
	friend std::ostream& operator<<(std::ostream& os, const Statistics& stat);
  	friend std::istream& operator>>(std::istream& is, Statistics& stat);
};

#endif
