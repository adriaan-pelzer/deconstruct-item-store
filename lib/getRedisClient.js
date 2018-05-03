const R = require ( 'ramda' );
const fs = require ( 'fs' );
const redis = require ( 'redis' );
const crypto = require ( 'crypto' );
const path = require ( 'path' );

const getRedisConfigs = env => {
    const write_host = env.PRIMARY_REDIS_HOST || env.REDIS_HOST || 'localhost';
    const write_port = env.PRIMARY_REDIS_PORT || env.REDIS_PORT || 6379;
    const read_hosts = env.READ_REDIS_HOSTS ? R.map ( R.trim, env.READ_REDIS_HOSTS.split ( ',' ) ) : [ write_host ];
    const read_ports = env.READ_REDIS_PORTS ? R.map ( R.trim, env.READ_REDIS_PORTS.split ( ',' ) ) : [ write_port ];
    const chosen_read_index = Math.floor ( Math.random () * read_hosts.length );

    if ( read_hosts.length !== read_ports.length ) {
        throw new Error ( `environment variable READ_REDIS_HOSTS specifies ${read_hosts.length > read_ports.length ? 'more' : 'fewer'} entries than READ_REDIS_PORTS` );
    }

    return { write: { host: write_host, port: write_port }, read: { host: read_hosts[chosen_read_index], port: read_ports[chosen_read_index] } };
};

const getRedisClient = stub => {
    const redisConfigs = getRedisConfigs ( process.env );
    const clients = {
        write: stub || redis.createClient ( redisConfigs.write ),
        read: stub || redis.createClient ( redisConfigs.read )
    };

    const wrapCallback = callback => {
        return ( error, result ) => {
            if ( error ) {
                return callback ( error.message || error );
            }

            return callback ( null, result );
        };
    };

    const writeMethods = [
        'zadd', 'zrem', 'zremrangebyrank', 'zremrangebyscore',
        'lpush', 'rpush', 'linsert', 'lset',
        'sadd', 'srem',
        'geoadd',
        'pfadd',
        'set', 'del'
    ];

    const readMethods = [
        'zrange', 'zrevrange', 'zrangebyscore', 'zscore', 'zinterstore', 'zunionstore', 'zrevrangebyscore',
        'lrange', 'lindex', 'llen',
        'georadius',
        'pfcount',
        'exists', 'get', 'delTmp'
    ];

    const supportedMethods = R.concat ( writeMethods, readMethods );

    const wrapRedisMethod = R.curry ( ( client, methodName ) => {
        const method = methodName === 'delTmp' ? 'del' : methodName;
        return [ methodName, ( parms, callback ) => {
            return client[method] ( parms, wrapCallback ( callback ) );
        } ];
    } );

    /*const luaScriptBodies = R.map (
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
    };*/

    return {
        ...R.fromPairs ( R.map ( wrapRedisMethod ( clients.read ), readMethods ) ),
        ...R.fromPairs ( R.map ( wrapRedisMethod ( clients.write ), writeMethods ) ),
        quit: () => {
            clients.read.quit ();
            clients.write.quit ();
        }
    };
};

module.exports = getRedisClient;
