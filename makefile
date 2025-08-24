
all: macosx

macosx:
	clang -undefined dynamic_lookup --shared -Wall -DUSE_RDTSC -g -O2 \
		-o luaprofilec.so \
		imap.c profile.c

linux:
	gcc -shared -fPIC -Wall -g -O2 -DUSE_RDTSC \
		-o luaprofilec.so \
		imap.c profile.c

clean:
	rm -rf luaprofilec.so

.PHONY : all clean macosx linux