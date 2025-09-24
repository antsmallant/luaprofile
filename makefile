.PHONY : all clean macosx linux

all: linux

macosx:
	clang -undefined dynamic_lookup --shared -Wall -DUSE_RDTSC -g -O2 \
		-o luaprofilec.so \
		imap.c profile.c

linux:
	gcc -shared -fPIC -Wall -g -O2 -Ibuild/lua-5.4.8/src \
		-o luaprofilec.so \
		imap.c profile.c icallpath.c

clean:
	rm -rf luaprofilec.so