#ifndef HEAP_FILE_H_
#define HEAP_FILE_H_

#include "GenericDBFile.h"

class HeapFile: public GenericDBFile {
public:
	int Create (const char *fpath, fType file_type, void *startup);
	int Open (const char *fpath);
	int Close ();
    string path;
	void Load (Schema &myschema, const char *loadpath);

	void MoveFirst ();
	void Add (Record &addme);
	int GetNext (Record &fetchme);
	virtual int GetNext (Record &fetchme, CNF &cnf, Record &literal);
};

#endif
