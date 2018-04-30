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

local function zRevRangeByScore ( key, max, min )
    local args={}
    table.insert(args,key)
    table.insert(args,max)
    table.insert(args,min)
    table.insert(args,'WITHSCORES')
    return doRedisCall('zrevrangebyscore',args)
end

local function geoRadius ( key, lng, lat, radius, units )
    local args={}
    table.insert(args,key)
    table.insert(args,lng)
    table.insert(args,lat)
    table.insert(args,radius)
    table.insert(args,units)
    table.insert(args,'WITHDIST')
    table.insert(args,'ASC')
    return doRedisCall('georadius',args)
end

local inter={}
local interKey='tmp'
local del={}
local before=tonumber(ARGV[6])
local after=tonumber(ARGV[7])
local geoResult=geoRadius(ARGV[1],ARGV[2],ARGV[3],ARGV[4],ARGV[5])

table.insert(del,interKey)

for i = 8, #ARGV, 1 do
    local union={}
    local unionKey='tmp-'..i
    table.insert(del,unionKey)
    for key in string.gmatch(ARGV[i], '([^,]+)') do
        table.insert(union,key)
    end
    zUnionStore(unionKey,union)
    table.insert(inter,unionKey)
end
zInterStore(interKey,inter)
local interResult=zRevRangeByScore(interKey,before,after)
doRedisCall('del',del)

local result=0
for i = 1, #geoResult, 1 do
    for j = 1, #interResult/2, 1 do
        if interResult[j*2-1] == geoResult[i][1] then
            table.remove(interResult, j*2-1)
            table.remove(interResult, j*2)
            result=result+1
            break;
        end
    end
end

return result
