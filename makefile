.PHONY: all compile configure clean

CC=gcc
Py=python

srcdir=sparkseqreducer
stretcherdir=${srcdir}/stretcher
hdirs=Headers Sources pcre
cdirs=Sources pcre

includes=$(foreach d,${hdirs},-I ${d}/)
sources=$(foreach d,${cdirs},${d}/*.c)

all: compile configure

compile:
	@echo "Installing..."
	cd ${stretcherdir} && \
	$(CC) -w -c -fPIC ${includes} ${sources} stretcher.c -lm && \
	$(CC) -shared -fPIC -o libstretcher.so *.o

configure:
	@echo "Configure"
	$(Py) $(srcdir)/configure.py

clean:
	@echo "Cleaning up files..."
	cd ${stretcherdir} && \
	rm *.o