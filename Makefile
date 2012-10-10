CPPFLAGS=-O2 -std=gnu99 -pthread -Wall -pedantic

benchmark: benchmark.o
	$(LINK.c) -o benchmark benchmark.o -L/usr/lib/fio -Wl,-rpath,/usr/lib/fio -lvsldpexp -lvsl -ldl -lrt

benchmark.o: benchmark.c
	$(COMPILE.c) benchmark.c

clean:
	$(RM) benchmark benchmark.o source.tar

dist:
	tar cf source.tar benchmark.c Makefile
