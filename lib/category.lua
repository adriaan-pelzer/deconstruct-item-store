local function doRedisCall ( command, args )
    local cmd={}
    table.insert(cmd,command)
    for i = 1, #args, 1 do
        table.insert(cmd,args[i])
    end
    return redis.call(unpack(cmd))
end

local function zCatStore ( command, dest, srcs )
    local args={}
    table.insert(args,dest)
    table.insert(args,#srcs)
    for i = 1, #srcs, 1 do
        table.insert(args,srcs[i])
    end
    table.insert(args,'AGGREGATE')
    table.insert(args,'MAX')
    return doRedisCall(command,args);
end

local function zInterStore ( dest, srcs )
    return zCatStore('zinterstore',dest,srcs)
end

local function zUnionStore ( dest, srcs )
    return zCatStore('zunionstore',dest,srcs)
end

local function zRevRangeByScore ( key, max, min, offset, count )
    local cnt=count
    if cnt == 0 then
        cnt = 100
    end
    local args={}
    table.insert(args,key)
    table.insert(args,max)
    table.insert(args,min)
    table.insert(args,'WITHSCORES')
    table.insert(args,'LIMIT')
    table.insert(args,offset)
    table.insert(args,cnt)
    return doRedisCall('zrevrangebyscore',args)
end

local inter={}
local del={}
local offset=tonumber(ARGV[1])
local count=tonumber(ARGV[2])
local stop=count+offset
local before=tonumber(ARGV[3])
local after=tonumber(ARGV[4])

local interKey = 'tmp'
table.insert(del,interKey)
for i = 5, #ARGV, 1 do
    local union={}
    local len=0
    local unionKey='tmp-'..i
    table.insert(del,unionKey)
    for key in string.gmatch(ARGV[i], '([^,]+)') do
        table.insert(union,key)
    end
    zUnionStore(unionKey,union)
    table.insert(inter,unionKey)
end
zInterStore(interKey,inter)
local result=zRevRangeBySCore('tmp',before,after,offset,count)
doRedisCall('del',del)
return result
