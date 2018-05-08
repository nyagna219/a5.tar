
CC = g++ -std=c++17 -O2 -w

tag = -i

ifdef linux
tag = -n
endif

a1-test.out: HeapFile.o SortedFile.o Record.o Comparison.o ComparisonEngine.o Schema.o Pipe.o BigQ.o File.o DBFile.o RelOp.o Function.o y.tab.o yyfunc.tab.o lex.yy.o lex.yyfunc.o a1-test.o
	$(CC) -o a1-test.out HeapFile.o SortedFile.o Record.o Comparison.o ComparisonEngine.o Schema.o Pipe.o BigQ.o File.o DBFile.o RelOp.o Function.o y.tab.o yyfunc.tab.o lex.yy.o lex.yyfunc.o a1-test.o -lfl -lpthread

main:  Pipe.o BigQ.o HeapFile.o  SortedFile.o Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o RelOp.o Function.o y.tab.o yyfunc.tab.o lex.yy.o lex.yyfunc.o Statistics.o Optimizer.o QueryPlan.o main.o
	$(CC) -o main.out Pipe.o BigQ.o HeapFile.o  SortedFile.o Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o RelOp.o Function.o y.tab.o yyfunc.tab.o lex.yy.o lex.yyfunc.o Statistics.o Optimizer.o QueryPlan.o main.o -lfl -lpthread
	
a1-test.o: a1-test.cc
	$(CC) -g -c a1-test.cc
	
main.o : main.cc
	$(CC) -g -c main.cc
	
Statistics.o: Statistics.cc
	$(CC) -g -c Statistics.cc

QueryPlan.o: QueryPlan.cc
	$(CC) -g -c QueryPlan.cc

Optimizer.o: Optimizer.cc
	$(CC) -g -c Optimizer.cc

Comparison.o: Comparison.cc
	$(CC) -g -c Comparison.cc
	
ComparisonEngine.o: ComparisonEngine.cc
	$(CC) -g -c ComparisonEngine.cc
	
HeapFile.o: HeapFile.cc
	$(CC) -g -c HeapFile.cc

SortedFile.o: SortedFile.cc
	$(CC) -g -c SortedFile.cc
	
DBFile.o: DBFile.cc
	$(CC) -g -c DBFile.cc

File.o: File.cc
	$(CC) -g -c File.cc

Record.o: Record.cc
	$(CC) -g -c Record.cc

Pipe.o: Pipe.cc
	$(CC) -g -c Pipe.cc

BigQ.o: BigQ.cc
	$(CC) -g -c BigQ.cc

Schema.o: Schema.cc
	$(CC) -g -c Schema.cc

y.tab.o: Parser.y
	yacc -d Parser.y
	sed $(tag) y.tab.c -e "s/  __attribute__ ((__unused__))$$/# ifndef __cplusplus\n  __attribute__ ((__unused__));\n# endif/" 
	g++ -c y.tab.c

yyfunc.tab.o: ParserFunc.y
	yacc -p "yyfunc" -b "yyfunc" -d ParserFunc.y
	#sed $(tag) yyfunc.tab.c -e "s/  _attribute_ ((_unused))$$/# ifndef __cplusplus\n  __attribute_ ((_unused_));\n# endif/" 
	g++ -c yyfunc.tab.c

lex.yy.o: Lexer.l
	lex  Lexer.l
	gcc  -c lex.yy.c

lex.yyfunc.o: LexerFunc.l
	lex -Pyyfunc LexerFunc.l
	gcc  -c lex.yyfunc.c

clean: 
	rm -f *.o
	rm -f *.out
	rm -f lex.yy.*
	rm -f y.tab.*
	rm -f yyfunc.tab.*
	rm -f lex.yyfunc*
