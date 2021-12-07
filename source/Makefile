CC = gcc
FC = gfortran

NP = 4

OPT  = -O2
OPTC = -O2
OPTF = -O2

librsf.a: rsf.c rsf_mod.F90 rsf.h
	${CC} ${OPTC} -c -I. rsf.c
	${FC} ${OPTF} -c -I. rsf_mod.F90 
	ar rcv librsf.a rsf.o rsf_mod.o

test1.Abs: clean librsf.a test_rsf.c
# 	$(CC) ${OPTC} -c -I. rsf.c && mpicc ${OPT} -I. -DTEST1 test_rsf.c rsf.o -o test1.Abs && ./test1.Abs
	${CC} ${OPT} -I. -DTEST1 test_rsf.c -L. -lrsf -o test1.Abs && ./test1.Abs 2>&1 | tee test1.lst

testf.Abs:	clean librsf.a test_rsf.F90
	${FC} ${OPTF} -I. test_rsf.F90 -L. -lrsf -o testf.Abs && ./testf.Abs 2>&1 | tee testf.lst

test2.Abs: clean test_rsf.c
	mpicc ${OPT} -I. -DTEST2 test_rsf.c  -o test2.Abs && mpirun -n ${NP} ./test2.Abs

clean:
	rm -f demo*.rsf demo.dat  *.Abs *.o *.mod a.out *.a
