LUALIB=-I/usr/local/include -L/usr/local/bin -llua52
SOCKETLIB=-lws2_32

.PHONY: all win linux

all : 
	@echo Please do \'make PLATFORM\' where PLATFORM is one of these:
	@echo win linux

win: mongo.dll

linux: mongo.so

mongo.dll : lua-mongo.c lua-socket.c
	gcc --shared -Wall -g $^ -o$@ $(LUALIB) $(SOCKETLIB)

mongo.so : lua-mongo.c lua-socket.c
	gcc --shared -Wall -fPIC -g $^ -o$@ 

clean:
	rm -f mongo.dll mongo.so
