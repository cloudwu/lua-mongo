#include <lua.h>
#include <lauxlib.h>

#include <errno.h>

#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

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

#include<netdb.h>

static void	
init_winsock() {
}

#endif

#define OP_REPLY 1
#define OP_MSG	1000
#define OP_UPDATE 2001
#define OP_INSERT 2002
#define OP_QUERY 2004
#define OP_GET_MORE	2005
#define OP_DELETE 2006
#define OP_KILL_CURSORS	2007

#define REPLY_CURSORNOTFOUND 1
#define REPLY_QUERYFAILURE 2
#define REPLY_AWAITCAPABLE 8	// ignore because mongo 1.6+ always set it

#define DEFAULT_CAP 128

struct connection {
	int sock;
	int id;
};

struct response {
	int flags;
	int32_t cursor_id[2];
	int starting_from;
	int number;
};

struct buffer {
	int size;
	int cap;
	uint8_t * ptr;
	uint8_t buffer[DEFAULT_CAP];
};

static inline uint32_t
to_little_endian(uint32_t v) {
	union {
		uint32_t v;
		uint8_t b[4];
	} u;
	u.v = v;
	return u.b[0] | u.b[1] << 8 | u.b[2] << 16 | u.b[3] << 24;
}

typedef void * document;

static inline uint32_t
get_length(document buffer) {
	union {
		uint32_t v;
		uint8_t b[4];
	} u;
	memcpy(&u.v, buffer, 4);
	return u.b[0] | u.b[1] << 8 | u.b[2] << 16 | u.b[3] << 24;
}

static inline void
buffer_destroy(struct buffer *b) {
	if (b->ptr != b->buffer) {
		free(b->ptr);
	}
}

static inline void
buffer_create(struct buffer *b) {
	b->size = 0;
	b->cap = DEFAULT_CAP;
	b->ptr = b->buffer;
}

static inline void
buffer_reserve(struct buffer *b, int sz) {
	if (b->size + sz <= b->cap)
		return;
	do {
		b->cap *= 2;
	} while (b->cap <= b->size + sz);

	if (b->ptr == b->buffer) {
		b->ptr = malloc(b->cap);
		memcpy(b->ptr, b->buffer, b->size);
	} else {
		b->ptr = realloc(b->ptr, b->cap);
	}
}

static inline void
write_int32(struct buffer *b, int32_t v) {
	uint32_t uv = (uint32_t)v;
	buffer_reserve(b,4);
	b->ptr[b->size++] = uv & 0xff;
	b->ptr[b->size++] = (uv >> 8)&0xff;
	b->ptr[b->size++] = (uv >> 16)&0xff;
	b->ptr[b->size++] = (uv >> 24)&0xff;
}

static inline void
write_bytes(struct buffer *b, void * buf, int sz) {
	buffer_reserve(b,sz);
	memcpy(b->ptr + b->size, buf, sz);
	b->size += sz;
}

static void
write_string(struct buffer *b, const char *key, size_t sz) {
	buffer_reserve(b,sz+1);
	memcpy(b->ptr + b->size, key, sz);
	b->ptr[b->size+sz] = '\0';
	b->size+=sz+1;
}

static inline int
reserve_length(struct buffer *b) {
	int sz = b->size;
	buffer_reserve(b,4);
	b->size +=4;
	return sz;
}

static inline void
write_length(struct buffer *b, int32_t v, int off) {
	uint32_t uv = (uint32_t)v;
	b->ptr[off++] = uv & 0xff;
	b->ptr[off++] = (uv >> 8)&0xff;
	b->ptr[off++] = (uv >> 16)&0xff;
	b->ptr[off++] = (uv >> 24)&0xff;
}

static int
block_send(int sock, const void * buffer, int sz) {
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
			return -1;
		}
		break;
	}
	return 0;
}

static int
block_recv(struct connection *conn, void * buffer, int sz) {
	for (;;) {
		int err = recv(conn->sock, buffer, sz, 0);
		if (err < 0) {
			switch (errno) {
			case EAGAIN:
			case EINTR:
				continue;
			}
		}
		if (err != sz) {
			close(conn->sock);
			conn->sock = INVALID_SOCKET;
			return -1;
		}
		break;
	}
	return 0;
}

// 1 connection
// 2 integer flags
// 3 string collection name
// 4 integer skip
// 5 integer return number
// 6 document query
// 7 document selector (optional)
// return integer request_id
static int
op_query(lua_State *L) {
	struct connection * conn = lua_touserdata(L,1);
	if (conn == NULL || conn->sock == INVALID_SOCKET) {
		return luaL_error(L, "Not a invalid socket");
	}
	document query = lua_touserdata(L,6);
	if (query == NULL) {
		return luaL_error(L, "require query document");
	}
	document selector = lua_touserdata(L,7);
	int flags = luaL_checkinteger(L, 2);
	size_t nsz = 0;
	const char *name = luaL_checklstring(L,3,&nsz);
	int skip = luaL_checkinteger(L, 4);
	int number = luaL_checkinteger(L, 5);

	struct buffer buf;
	buffer_create(&buf);

	int len = reserve_length(&buf);
	write_int32(&buf, ++conn->id);
	write_int32(&buf, 0);
	write_int32(&buf, OP_QUERY);
	write_int32(&buf, flags);
	write_string(&buf, name, nsz);
	write_int32(&buf, skip);
	write_int32(&buf, number);

	int32_t query_len = get_length(query);
	int total = buf.size + query_len;
	int32_t selector_len = 0;
	if (selector) {
		selector_len = get_length(selector);
		total += selector_len;
	}

	write_length(&buf, total, len);

	if (block_send(conn->sock, buf.ptr, buf.size)) {
		goto _error;
	}

	if (block_send(conn->sock, query, query_len)) {
		goto _error;
	}
	if (selector) {
		if (block_send(conn->sock, selector, selector_len)) {
			goto _error;
		}
	}

	lua_pushinteger(L, conn->id);
	buffer_destroy(&buf);

	return 1;

_error:
	buffer_destroy(&buf);
	return 0;
}

// 1 connection
// 2 result document table
// return number
//	response data
//  document first
//  integer startfrom

static int
op_reply(lua_State *L) {
	struct {
		int32_t length; // total message size, including this
		int32_t request_id; // identifier for this message
		int32_t response_id; // requestID from the original request
							// (used in reponses from db)
		int32_t opcode; // request type 
		int32_t flags;
		int32_t cursor_id[2];
		int32_t starting;
		int32_t number;
	} reply;

	struct connection * conn = lua_touserdata(L, 1);
	if (conn == NULL || conn->sock == INVALID_SOCKET) {
		luaL_error(L, "Invalid socket");
	}

	if (block_recv(conn, &reply, sizeof(reply))) {
		return 0;
	}
	int length = to_little_endian(reply.length);
	int id = to_little_endian(reply.response_id);
	lua_pushinteger(L, id);
	if (length <= sizeof(reply)) {
		return 1;
	}

	int flags = to_little_endian(reply.flags);
	if (flags & REPLY_QUERYFAILURE) {
		lua_pushnil(L);
		int sz = length - sizeof(reply);
		void * err = lua_newuserdata(L, sz);
		if (block_recv(conn, err, sz)) {
			return 0;
		}
		return 3;
	}

	struct response * rep = lua_newuserdata(L, length);
	rep->flags = flags;
	rep->cursor_id[0] = reply.cursor_id[0];
	rep->cursor_id[1] = reply.cursor_id[1];
	rep->starting_from = to_little_endian(reply.starting);
	rep->number = to_little_endian(reply.number);
	int sz = length - sizeof(reply);
	uint8_t * doc = (uint8_t *)(rep+1);
	if (block_recv(conn, doc, sz)) {
		return 0;
	}
	lua_pushlightuserdata(L, doc);
	if (lua_istable(L,2)) {
		int i = 1;
		while (sz > 4) {
			lua_pushlightuserdata(L, doc);
			lua_rawseti(L, 2, i);

			int32_t doc_len = get_length(doc);

			doc += doc_len;
			sz -= doc_len;

			++i;
		}
		if (i != rep->number + 1) {
			return luaL_error(L, "Invalid reply message, need %d document", rep->number);
		}
		int c = lua_rawlen(L, 2);
		for (;i<=c;i++) {
			lua_pushnil(L);
			lua_rawseti(L, 2, i);
		}
	}
	lua_pushinteger(L, rep->starting_from);
	return 4;
}

/*
	1 connection
	2 response
 */
static int
op_kill(lua_State *L) {
	struct connection * conn = lua_touserdata(L,1);
	struct response * rep = lua_touserdata(L,2);
	if (conn == NULL || rep == NULL || conn->sock == INVALID_SOCKET) {
		luaL_error(L, "Invalid socket");
	}
	struct buffer buf;
	buffer_create(&buf);

	int len = reserve_length(&buf);
	write_int32(&buf, 0);
	write_int32(&buf, 0);
	write_int32(&buf, OP_KILL_CURSORS);

	write_int32(&buf, 0);
	write_int32(&buf, 1);
	write_bytes(&buf, rep->cursor_id, 8);

	write_length(&buf, buf.size, len);

	block_send(conn->sock, buf.ptr, buf.size);

	buffer_destroy(&buf);
	return 0;
}

/*
	1 connection
	2 string collection
	3 integer single remove
	4 document selector
 */
static int
op_delete(lua_State *L) {
	struct connection * conn = lua_touserdata(L,1);
	document selector  = lua_touserdata(L,4);
	if (conn == NULL || selector == NULL || conn->sock == INVALID_SOCKET) {
		luaL_error(L, "Invalid param");
	}
	size_t sz = 0;
	const char * name = luaL_checklstring(L,2,&sz);

	struct buffer buf;
	buffer_create(&buf);
	int len = reserve_length(&buf);
	write_int32(&buf, 0);
	write_int32(&buf, 0);
	write_int32(&buf, OP_DELETE);
	write_int32(&buf, 0);
	write_string(&buf, name, sz);
	write_int32(&buf, lua_tointeger(L,3));

	int32_t selector_len = get_length(selector);
	int total = buf.size + selector_len;
	write_length(&buf, total, len);

	if (block_send(conn->sock, buf.ptr, buf.size)) {
		goto _ret;
	}

	if (block_send(conn->sock, selector, selector_len)) {
		goto _ret;
	}
_ret:
	buffer_destroy(&buf);

	return 0;
}

/*
	1 connection
	2 string collection
	3 integer number
	4 response (cursor id)

	return request id
 */
static int
op_get_more(lua_State *L) {
	struct connection * conn = lua_touserdata(L,1);
	struct response * rep = lua_touserdata(L,4);
	if (conn == NULL || rep == NULL || conn->sock == INVALID_SOCKET) {
		luaL_error(L, "Invalid socket");
	}
	size_t sz = 0;
	const char * name = luaL_checklstring(L,2,&sz);
	int number = luaL_checkinteger(L, 3);

	struct buffer buf;
	buffer_create(&buf);
	int len = reserve_length(&buf);
	write_int32(&buf, ++conn->id);
	write_int32(&buf, 0);
	write_int32(&buf, OP_GET_MORE);
	write_int32(&buf, 0);
	write_string(&buf, name, sz);
	write_int32(&buf, number);
	write_bytes(&buf, rep->cursor_id, 8);
	write_length(&buf, buf.size, len);

	block_send(conn->sock, buf.ptr, buf.size);

	buffer_destroy(&buf);

	lua_pushinteger(L, conn->id);

	return 1;
}

static int
closeconn(lua_State *L) {
	struct connection * conn = luaL_checkudata(L, 1, "mongo");
	if (conn->sock != INVALID_SOCKET) {
		close(conn->sock);
		conn->sock = INVALID_SOCKET;
	}
	return 0;
}

// 1 connection
// 2 string collection
// 3 integer flags
// 4 document selector
// 5 document update
static int
op_update(lua_State *L) {
	struct connection * conn = lua_touserdata(L,1);
	document selector  = lua_touserdata(L,4);
	document update = lua_touserdata(L,5);
	if (conn == NULL || selector == NULL || update == NULL || conn->sock == INVALID_SOCKET) {
		luaL_error(L, "Invalid param");
	}
	size_t sz = 0;
	const char * name = luaL_checklstring(L,2,&sz);

	struct buffer buf;
	buffer_create(&buf);
	int len = reserve_length(&buf);
	write_int32(&buf, 0);
	write_int32(&buf, 0);
	write_int32(&buf, OP_UPDATE);
	write_int32(&buf, 0);
	write_string(&buf, name, sz);
	write_int32(&buf, lua_tointeger(L,3));

	int32_t selector_len = get_length(selector);
	int32_t update_len = get_length(update);

	int total = buf.size + selector_len + update_len;
	write_length(&buf, total, len);

	if (block_send(conn->sock, buf.ptr, buf.size)) {
		goto _ret;
	}

	if (block_send(conn->sock, selector, selector_len)) {
		goto _ret;
	}
	if (block_send(conn->sock, update, update_len)) {
		goto _ret;
	}
_ret:
	buffer_destroy(&buf);

	return 0;
}

static int
document_length(lua_State *L) {
	if (lua_isuserdata(L, 4)) {
		document doc = lua_touserdata(L,4);
		return get_length(doc);
	}
	if (lua_istable(L,4)) {
		int total = 0;
		int s = lua_rawlen(L,4);
		int i;
		for (i=1;i<=s;i++) {
			lua_rawgeti(L, 4, i);
			document doc = lua_touserdata(L,-1);
			if (doc == NULL) {
				lua_pop(L,1);
				return luaL_error(L, "Invalid document at %d", i);
			} else {
				total += get_length(doc);
				lua_pop(L,1);
			}
		}
		return total;
	}
	return luaL_error(L, "Insert need documents");
}

// 1 connection
// 2 integer flags
// 3 string collection
// 4 documents

static int
op_insert(lua_State *L) {
	struct connection * conn = lua_touserdata(L,1);
	if (conn == NULL || conn->sock == INVALID_SOCKET) {
		luaL_error(L, "Invalid sock");
	}
	size_t sz = 0;
	const char * name = luaL_checklstring(L,3,&sz);
	int dsz = document_length(L);
	
	struct buffer buf;
	buffer_create(&buf);
	int len = reserve_length(&buf);
	write_int32(&buf, 0);
	write_int32(&buf, 0);
	write_int32(&buf, OP_INSERT);
	write_int32(&buf, lua_tointeger(L,2));
	write_string(&buf, name, sz);

	int total = buf.size + dsz;
	write_length(&buf, total, len);

	if (block_send(conn->sock, buf.ptr, buf.size)) {
		goto _ret;
	}

	if (lua_isuserdata(L,4)) {
		document doc = lua_touserdata(L,4);
		if (block_send(conn->sock, doc, get_length(doc))) {
			goto _ret;
		}
	} else {
		int s = lua_rawlen(L, 4);
		int i;
		for (i=1;i<=s;i++) {
			lua_rawgeti(L,4,i);
			document doc = lua_touserdata(L,4);
			if (block_send(conn->sock, doc, get_length(doc))) {
				goto _ret;
			}
			lua_pop(L,1);
		}
	}
_ret:
	buffer_destroy(&buf);

	return 0;
}

static int
newconn(lua_State *L, int sock) {
	struct connection * conn = lua_newuserdata(L, sizeof(*conn));
	conn->sock = sock;
	conn->id = 0;
	if (luaL_newmetatable(L, "mongo")) {
		luaL_Reg l[] ={
			{ "close", closeconn },
			{ "query", op_query },
			{ "reply", op_reply },
			{ "kill", op_kill },
			{ "delete", op_delete },
			{ "more", op_get_more },
			{ "update", op_update },
			{ "insert", op_insert },
			{ NULL, NULL },
		};
		luaL_newlib(L,l);
		lua_setfield(L, -2, "__index");
		lua_pushcfunction(L, closeconn);
		lua_setfield(L, -2, "__gc");
	}
	lua_setmetatable(L, -2);
	return 1;
}

static int
lconn(lua_State *L) {
	const char * host = luaL_checkstring(L,1);
	int port = luaL_optinteger(L,2,27017);

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
		return newconn(L, sock);
	}

	return 0;
}

int
luaopen_mongo_conn(lua_State *L) {
	init_winsock();
	luaL_checkversion(L);
	lua_pushcfunction(L, lconn);
	return 1;
}
