const R = require ( 'ramda' );
const fs = require ( 'fs' );
const redis = require ( 'redis' );
const crypto = require ( 'crypto' );
const path = require ( 'path' );

const getRedisClient = stub => {
    const sha = string => crypto.createHash ( 'sha1' ).update ( string, 'utf8' ).digest ( 'hex' );

    const client = stub || redis.createClient ( {
        port: process.env['REDIS_PORT'] || 6379,
        host: process.env['REDIS_HOST'] || 'localhost'
    } );

    const wrapCallback = callback => {
        return ( error, result ) => {
            if ( error ) {
                return callback ( error.message || error );
            }

            return callback ( null, result );
        };
    };

    const supportedMethods = [
        'zrange', 'zrevrange', 'zrangebyscore', 'zscore', 'zinterstore', 'zunionstore', 'zadd', 'zrevrangebyscore', 'zrem', 'zremrangebyrank', 'zremrangebyscore', 'zcard',
        'lpush', 'rpush', 'linsert', 'lrange', 'lindex', 'lset', 'llen',
        'srandmember', 'sismember', 'smembers', 'sadd', 'srem', 'scard',
        'georadius', 'geoadd',
        'pfadd', 'pfcount',
        'exists', 'get', 'set', 'del'
    ];

    const wrapRedisMethod = methodName => {
        return [ methodName, ( parms, callback ) => {
            return client[methodName] ( parms, wrapCallback ( callback ) );
        } ];
    };

    const luaScriptBodies = R.map (
        fileName => ( { body: fs.readFileSync ( path.resolve ( __dirname, fileName ) ).toString ( 'utf8' ), fileName } ),
        R.filter ( fileName => fileName.match ( /^.*\.lua$/ ), fs.readdirSync ( path.resolve ( __dirname ) ) )
    );

    const luaScripts = R.fromPairs ( R.map ( file => ( [ file.fileName.replace ( '.lua', '' ), { script: file.body, sha: sha ( file.body ) } ] ), luaScriptBodies ) );

    const runLua = ( { scriptName, keys, args }, callback ) => {
        if ( ! luaScripts[scriptName] || ! luaScripts[scriptName].script ) {
            return callback ( `there's no lua script named '${scriptName}'` );
        }
        const redisCmd = luaScripts[scriptName].sha ? 'evalsha' : 'script';
        const redisArgs = luaScripts[scriptName].sha ? R.reduce ( R.concat, [], [
            [ luaScripts[scriptName].sha, keys.length ],
            keys,
            args
        ] ) : [ 'LOAD', luaScripts[scriptName].script ];

        return client[redisCmd] ( redisArgs, ( error, result ) => {
            if ( error && luaScripts[scriptName].sha ) {
                if ( JSON.stringify ( error ).match ( /^.*NOSCRIPT.*$/ ) ) {
                    luaScripts[scriptName].sha = undefined;
                    return runLua ( { scriptName, keys, args }, callback );
                }
                return callback ( error );
            }

            if ( error ) {
                return callback ( error );
            }

            if ( ! luaScripts[scriptName].sha ) {
                luaScripts[scriptName].sha = result
                return runLua ( { scriptName, keys, args }, callback );
            }

            return callback ( null, result );
        } );
    };

    return {
        ...R.fromPairs ( R.map ( wrapRedisMethod, supportedMethods ) ),
        runLua,
        quit: () => {
            return client.quit ();
        }
    };
};

module.exports = getRedisClient;
