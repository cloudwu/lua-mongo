local bson = require "bson"
local conn = require "mongo.conn"
local rawget = rawget
local assert = assert

local bson_encode = bson.encode
local bson_decode = bson.decode
local empty_bson = bson_encode {}

local mongo = {}
mongo.null = assert(bson.null)
mongo.maxkey = assert(bson.maxkey)
mongo.minkey = assert(bson.minkey)
mongo.type = assert(bson.type)

local mongo_cursor = {}
local cursor_meta = {
	__index = mongo_cursor,
}

local mongo_client = {}

local client_meta = {
	__index = function(self, key)
		return rawget(mongo_client, key) or self:getDB(key)
	end,
	__tostring = function (self)
		local port_string
		if self.port then
			port_string = ":" .. tostring(self.port)
		else
			port_string = ""
		end

		return "[mongo client : " .. self.host .. port_string .."]"
	end,
}

local mongo_db = {}

local db_meta = {
	__index = function (self, key)
		return rawget(mongo_db, key) or self:getCollection(key)
	end,
	__tostring = function (self)
		return "[mongo db : " .. self.name .. "]"
	end
}

local mongo_collection = {}
local collection_meta = {
	__index = function(self, key)
		return rawget(mongo_collection, key) or self:getCollection(key)
	end ,
	__tostring = function (self)
		return "[mongo collection : " .. self.full_name .. "]"
	end
}

function mongo.client( obj )
	obj.sock = assert(conn(obj.host, obj.port),"Connect failed")
	return setmetatable(obj, client_meta)
end

function mongo_client:getDB(dbname)
	local db = {
		connection = self,
		__sock = self.sock,
		name = dbname,
		full_name = dbname,
		database = db,
		__cmd = dbname .. "." .. "$cmd",
	}

	return setmetatable(db, db_meta)
end

function mongo_client:disconnect()
	self.__sock:close()
end

function mongo_client:runCommand(cmd)
	if not self.admin then
		self.admin = self:getDB "admin"
	end
	return self.admin:runCommand(cmd)
end

function mongo_db:runCommand(cmd)
	local request_id = self.__sock:query(0, self.__cmd, 0, 1, bson_encode(cmd))
	local reply_id, data, doc = self.__sock:reply()
	assert(request_id == reply_id, "Reply from mongod error")
	return bson_decode(doc)
end

function mongo_db:getCollection(collection)
	local col = {
		connection = self.connection,
		__sock = self.__sock,
		name = collection,
		full_name = self.full_name .. "." .. collection,
		database = self.database,
	}
	self[collection] = setmetatable(col, collection_meta)
	return col
end

mongo_collection.getCollection = mongo_db.getCollection

function mongo_collection:insert(doc)
	-- flags support 1: ContinueOnError
	if doc._id == nil then
		doc._id = bson.objectid()
	end
	self.__sock:insert(0, self.full_name, bson_encode(doc))
end

function mongo_collection:batch_insert(docs)
	for i=1,#docs do
		if docs[i]._id == nil then
			docs[i]._id = bson.objectid()
		end
		docs[i] = bson_encode(docs[i])
	end
	self.__sock:insert(self.full_name, 0, docs)
end

function mongo_collection:update(selector,update,upsert,multi)
	local flags = (upsert and 1 or 0) + (multi and 2 or 0)
	self.__sock:update(self.full_name, flags, bson_encode(selector), bson_encode(update))
end

function mongo_collection:delete(selector, single)
	self.__sock:delete(self.full_name, single, bson_encode(selector))
end

function mongo_collection:findOne(query, selector)
	local request_id = self.__sock:query(0, self.full_name, 0, 1, query and bson_encode(query) or empty_bson, selector and bson_encode(selector))
	local reply_id, data, doc = self.__sock:reply()
	assert(request_id == reply_id, "Reply from mongod error")
	return bson_decode(doc)
end

function mongo_collection:find(query, selector)
	return setmetatable( {
		__collection = self,
		__query = query and bson_encode(query) or empty_bson,
		__selector = selector and bson_encode(selector),
		__ptr = nil,
		__data = nil,
		__document = {},
		__flags = 0,
	} , cursor_meta)
end

function mongo_cursor:hasNext()
	if self.__ptr == nil then
		if self.__document == nil then
			return false
		end
		local sock = self.__collection.__sock
		local request_id
		if self.__data == nil then
			request_id = sock:query(self.__flags, self.__collection.full_name,0,0,self.__query,self.__selector)
		else
			request_id = sock:more(self.__collection.full_name,0,self.__data)
		end

		local reply_id, data, doc = sock:reply(self.__document)
		assert(request_id == reply_id, "Reply from mongod error")
		if data == nil then
			if doc then
				local err = bson_decode(doc)
				error(err["$err"])
			else
				self.__document = nil
				self.__data = nil
				return false
			end
		end

		self.__data = data
		self.__ptr = 1
	end

	return true
end

function mongo_cursor:next()
	if self.__ptr == nil then
		error "Call hasNext first"
	end
	local r = bson_decode(self.__document[self.__ptr])
	self.__ptr = self.__ptr + 1
	if self.__ptr > #self.__document then
		self.__ptr = nil
	end

	return r
end

function mongo_cursor:close()
	-- todo: warning hasNext after close
	if self.__data then
		self.__collection.__sock:kill(self.__data)
	end
end

return mongo