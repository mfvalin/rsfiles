# make CC=mpicc TESTC=test7
# make TESTC=rsf_dump
# rm -f demo0.rsf tagada.txt  ; r.mpich_run ./test7.exe demo0.rsf icc.txt -- -n 2
# rm -f demo7.rsf ; r.mpich_run ./test7s.exe  demo7.rsf 12 -- -n 3


CC = mpicc
CFLAGS =
FC = mpif90
FCFLAGS =

TESTC := test1
DEFINE := $(shell echo ${TESTC} | tr a-z A-Z )

NP = 4

OPT  = -O2
OPTC = -O2 $(CFLAGS)
OPTF = -O2 $(FCFLAGS)

$(TESTC).exe : clean librsf.a test_rsf.c
	${CC} ${OPT} -I. -D$(DEFINE) test_rsf.c -L. -lrsf -o  $(TESTC).exe

test1.Abs: clean librsf.a test_rsf.c
# 	$(CC) ${OPTC} -c -I. rsf.c && mpicc ${OPT} -I. -DTEST1 test_rsf.c rsf.o -o test1.Abs && ./test1.Abs
	${CC} ${OPT} -I. -DTEST1 test_rsf.c -L. -lrsf -o test1.Abs && ./test1.Abs 2>&1 | tee test1.lst
# 	echo Target = -D$(subst test,TESTC,$(TESTC))
# 	echo DEFINE = -D$(DEFINE)

testf.Abs:	clean librsf.a test_rsf.F90
	${FC} ${OPTF} -I. test_rsf.F90 -L. -lrsf -o testf.Abs && ./testf.Abs 2>&1 | tee testf.lst

test2.Abs: clean librsf.a test_rsf.c
	mpicc ${OPT} -I. -DTEST2 test_rsf.c  -o test2.Abs && mpirun -n ${NP} ./test2.Abs

librsf.a: rsf.c rsf_mod.F90 rsf.h bad.rsf
	${CC} ${OPTC} -c -I. rsf.c
	${FC} ${OPTF} -c -I. rsf_mod.F90 
	ar rcv librsf.a rsf.o rsf_mod.o

clean:
	rm -f demo*.rsf demo.dat  test*.Abs test*.exe *.o *.mod a.out *.a

bad.rsf:
	touch bad.rsf
	chmod 000 bad.rsf
