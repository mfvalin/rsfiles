CC = gcc

NP = 4

OPT = -O2
OPTC = -O2

test1: clean
	$(CC) ${OPTC} -c -I. rsf.c && mpicc ${OPT} -I. -DTEST1 test_rsf.c rsf.o -o test1.Abs && ./test1.Abs

test2: clean
	mpicc ${OPT} -I. -DTEST2 test_rsf.c  -o test2.Abs && mpirun -n ${NP} ./test2.Abs

clean:
	rm -f demo*.rsf demo.dat  test1.Abs
