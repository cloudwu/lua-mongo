mongo = require "mongo"

db = mongo.client { host = "localhost" }

local r = db:runCommand "listDatabases"

for k,v in ipairs(r.databases) do
	print(v.name)
end


local loc = db:getDB "hello"
local c = loc.system.namespaces:find()

while c:hasNext() do
	local r = c:next()
	print(r.name)
end

print "==============="

db.hello.world:insert {}
local r = db:runCommand ("getLastError",1,"w",1)
print(r.ok)

local c = db.hello.world:find()

while c:hasNext() do
	local r = c:next()
	print(mongo.type(r._id))
end
