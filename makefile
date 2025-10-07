.PHONY : all clean linux

all: linux

linux:
	gcc -shared -fPIC -Wall -g -O2 \
		-Ibuild/lua-5.4.8/src \
		-o luaprofilec.so \
		imap.c smap.c profile.c icallpath.c

clean:
	rm -rf luaprofilec.so