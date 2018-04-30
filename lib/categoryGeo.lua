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
local del={}
local geo={}
local offset=tonumber(ARGV[6])
local count=tonumber(ARGV[7])
local stop=count+offset
local before=tonumber(ARGV[8])
local after=tonumber(ARGV[9])
local geoResult=geoRadius(ARGV[1],ARGV[2],ARGV[3],ARGV[4],ARGV[5])
local interKey='tmp'
table.insert(del,interKey)

for i = 10, #ARGV, 1 do
    local union={}
    local unionKey='tmp-'..i
    table.insert(union,unionKey)
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

local tmp={}
for i = 1, #geoResult, 1 do
    for j = 1, #interResult/2, 1 do
        if interResult[j*2-1] == geoResult[i][1] then
            table.insert(tmp,{geoResult[i][1],interResult[j*2],geoResult[i][2]})
            table.remove(interResult,j*2-1);
            table.remove(interResult,j*2);
            break;
        end
    end
end

if count == 0 or stop > #tmp then
    stop = #tmp
end

local result={}
for i = offset + 1, stop, 1 do
    for j = 1, 3, 1 do
        table.insert(result,tmp[i][j])
    end
end

return result
