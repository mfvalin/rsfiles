CC = gcc
FC=gfortran

NP = 4

OPT  = -O2
OPTC = -O2
OPTF =

lib:	rsf.c
	${CC} ${OPTC} -c -I. rsf.c
	ar rcv librsf.a rsf.o

test1.Abs: clean lib
# 	$(CC) ${OPTC} -c -I. rsf.c && mpicc ${OPT} -I. -DTEST1 test_rsf.c rsf.o -o test1.Abs && ./test1.Abs
	${CC} ${OPT} -I. -DTEST1 test_rsf.c -L. -lrsf -o test1.Abs && ./test1.Abs 2>&1 | tee test1.lst

testf.Abs:	clean lib rsf_mod.F90 test_rsf.F90
	${FC} ${OPTF} -I. rsf_mod.F90 test_rsf.F90 -L. -lrsf -o testf.Abs && ./testf.Abs 2>&1 | tee testf.lst

test2.Abs: clean
	mpicc ${OPT} -I. -DTEST2 test_rsf.c  -o test2.Abs && mpirun -n ${NP} ./test2.Abs

clean:
	rm -f demo*.rsf demo.dat  *.Abs *.o *.mod a.out
