#include <lua.h>
#include <lauxlib.h>

#include <errno.h>

#if defined(_WIN32) || defined(_WIN64)

#define _WIN32_WINNT 0x0501

#include <winsock2.h>
#include <ws2tcpip.h>

static void	
init_winsock() {
	WSADATA wsaData;
	WSAStartup(MAKEWORD(2,2), &wsaData);
}

#define close(fd) closesocket(fd)

#else

#include <netdb.h>
#include <unistd.h>
#define INVALID_SOCKET (-1)

static void	
init_winsock() {
}

#endif

#ifndef MSG_WAITALL
#define MSG_WAITALL 0
#endif

#define LOCALBUFFER 8192

static int
lopen(lua_State *L) {
	const char * host = luaL_checkstring(L,1);
	int port = luaL_checkinteger(L,2);

	char port_str[32];
	int status;

	struct addrinfo ai_hints;
	struct addrinfo *ai_list = NULL;
	struct addrinfo *ai_ptr = NULL;

	sprintf( port_str, "%d", port );

	memset( &ai_hints, 0, sizeof( ai_hints ) );
	ai_hints.ai_family = AF_UNSPEC;
	ai_hints.ai_socktype = SOCK_STREAM;
	ai_hints.ai_protocol = IPPROTO_TCP;

	status = getaddrinfo( host, port_str, &ai_hints, &ai_list );
	if ( status != 0 ) {
		return 0;
	}
	int sock=INVALID_SOCKET;
	for	( ai_ptr = ai_list;	ai_ptr != NULL;	ai_ptr = ai_ptr->ai_next ) {
		sock = socket( ai_ptr->ai_family, ai_ptr->ai_socktype, ai_ptr->ai_protocol );
		if ( sock == INVALID_SOCKET	) {
			continue;
		}
		
		status = connect( sock,	ai_ptr->ai_addr, ai_ptr->ai_addrlen	);
		if ( status	!= 0 ) {
			close(sock);
			sock = INVALID_SOCKET;
			continue;
		}
		break;
	}

	freeaddrinfo( ai_list );

	if (sock != INVALID_SOCKET) {
		lua_pushinteger(L, sock);
		return 1;
	}

	return 0;
}

static int
lclose(lua_State *L) {
	int sock = luaL_checkinteger(L, 1);
	close(sock);

	return 0;
}

static int
lread(lua_State *L) {
	int fd = luaL_checkinteger(L,1);
	int sz = luaL_checkinteger(L,2);
	char tmp[LOCALBUFFER];
	void * buffer = tmp;
	if (sz > LOCALBUFFER) {
		buffer = lua_newuserdata(L, sz);
	}
  char * ptr = buffer;
	for (;;) {
		int bytes = recv(fd, ptr, sz, MSG_WAITALL);
		if (bytes < 0) {
			switch (errno) {
			case EAGAIN:
			case EINTR:
				continue;
			}
			return 0;
		}
		if (bytes == 0) {
			return 0;
		}
		if (bytes < sz) {
			ptr += bytes;
			sz -= bytes;
			return 0;
		}
		lua_pushlstring(L, buffer, sz);
		return 1;
	}
}

static int
lwrite(lua_State *L) {
	int sock = luaL_checkinteger(L,1);
	size_t sz = 0;
	const char *buffer = luaL_checklstring(L, 2, &sz);
	for (;;) {
		int err = send(sock, buffer, sz, 0);
		if (err < 0) {
			switch (errno) {
			case EAGAIN:
			case EINTR:
				continue;
			}
		}
		if (err != sz) {
			lua_pushboolean(L,0);
			return 1;
		}
		break;
	}
	lua_pushboolean(L,1);
	return 1;
}

int
luaopen_mongo_socket(lua_State *L) {
	init_winsock();
	luaL_checkversion(L);
	luaL_Reg l[] ={
		{ "open", lopen },
		{ "close", lclose },
		{ "read", lread },
		{ "write", lwrite },
		{ NULL, NULL },
	};

	luaL_newlib(L,l);

	return 1;
}
