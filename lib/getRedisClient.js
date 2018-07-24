const R = require ( 'ramda' );
const fs = require ( 'fs' );
const redis = require ( 'redis' );
const crypto = require ( 'crypto' );
const path = require ( 'path' );

const getRedisConfigs = env => {
    if ( env.NOREPL ) {
        return { read: {
            host: env.REDIS_HOST || 'localhost',
            port: env.REDIS_PORT || 6379
        }, write: {
            host: env.REDIS_HOST || 'localhost',
            port: env.REDIS_PORT || 6379
        } };
    }
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
        'zrange', 'zrevrange', 'zrangebyscore', 'zscore', 'zinterstore', 'zunionstore', 'zrevrangebyscore', 'zcard',
        'lrange', 'lindex', 'llen',
        'smembers', 'scard', 'sismember', 'srandmember',
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

    const returnDisabledError = R.curry ( ( client, methodName ) => {
        return [ methodName, ( parms, callback ) => {
            return callback ( {
                code: 503,
                message: 'Writing has temporarily been disabled due to system maintenance. This should be rectified in the next few minutes'
            } );
        } ];
    } );

    const writeWrapper = process.env.REDIS_RDONLY ? returnDisabledError : wrapRedisMethod;

    return {
        ...R.fromPairs ( R.map ( wrapRedisMethod ( clients.read ), readMethods ) ),
        ...R.fromPairs ( R.map ( writeWrapper ( clients.write ), writeMethods ) ),
        quit: () => {
            clients.read.quit ();
            clients.write.quit ();
        }
    };
};

module.exports = getRedisClient;
