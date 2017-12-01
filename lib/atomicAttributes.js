const H = require ( 'highland' );
const R = require ( 'ramda' );

module.exports = redisClient => {
    const oneOrMany = R.compose ( R.flatten, R.of );

    const getKey = ( id, attributePath ) => {
        return [ id, ...R.flatten ( [ attributePath ] ).join ( '.' ).split ( '.' ) ].join ( '-' );
    };

    const zParmsFromMembers = members => R.reduce ( ( parms, { score, member } ) => ( [ ...parms, score, member ] ), [], oneOrMany ( members ) );
    const zRemoveParmsFromMembers = members => R.map ( member => {
        if ( R.type ( member ) === 'Object' && member.member ) {
            return member.member;
        }

        return member;
    }, oneOrMany ( members ) );
    const zMembersFromParms = R.compose ( R.map ( R.fromPairs ), R.map ( R.zip ( [ 'score', 'member' ] ) ), R.map ( pair => {
        return [ JSON.parse ( pair[1] ), pair[0] ];
    } ), R.splitEvery ( 2 ) );

    const redisZget = ( command, parms, callback ) => {
        return H.wrapCallback ( R.bind ( redisClient[command], redisClient ) )( parms )
            .map ( zMembersFromParms )
            .toCallback ( callback );
    };

    const validateList = R.curry ( ( type, members ) => {
        if ( R.type ( members ) === 'Array' ) {
            return R.isNil ( R.find ( member => {
                return atomicAttributes[type].validate ( member ) === false;
            }, members ) );
        }

        return atomicAttributes[type].validateSingle ( members );
    } );

    const atomicAttributes = {
        move: ( { parentItemId, attributePath, oldParentItemId }, callback ) => {
            return redisClient.rename ( [ getKey ( parentItemId, attributePath ), getKey ( oldParentItemId, attributePath ) ], callback );
        },
        list: {
            validate: validateList ( 'list' ),
            validateSingle: member => {
                return R.type ( member ) === 'String';
            },
            create: ( { parentItemId, attributePath, members }, callback ) => {
                return H.wrapCallback ( atomicAttributes.list.delete )( { parentItemId, attributePath } )
                    .flatMap ( () => {
                        return H.wrapCallback ( atomicAttributes.list.addEnd )( { parentItemId, attributePath, members } );
                    } )
                    .toCallback ( callback );
            },
            addStart: ( { parentItemId, attributePath, members }, callback ) => {
                return redisClient.lpush ( [ getKey ( parentItemId, attributePath ), ...oneOrMany ( members ) ], callback );
            },
            addEnd: ( { parentItemId, attributePath, members }, callback ) => {
                return redisClient.rpush ( [ getKey ( parentItemId, attributePath ), ...oneOrMany ( members ) ], callback );
            },
            insertBefore: ( { parentItemId, attributePath, member, pivotMember }, callback ) => {
                return redisClient.linsert ( [ getKey ( parentItemId, attributePath ), 'BEFORE', pivotMember, member ], callback );
            },
            insertAfter: ( { parentItemId, attributePath, member, pivotMember }, callback ) => {
                return redisClient.linsert ( [ getKey ( parentItemId, attributePath ), 'AFTER', pivotMember, member ], callback );
            },
            setIndex: ( { parentItemId, attributePath, member, index }, callback ) => {
                return redisClient.lset ( [ getKey ( parentItemId, attributePath ), index, member ], callback );
            },
            getIndex: ( { parentItemId, attributePath, index }, callback ) => {
                return redisClient.lindex ( [ getKey ( parentItemId, attributePath ), index ], callback );
            },
            card: ( { parentItemId, attributePath }, callback ) => {
                return redisClient.llen ( [ getKey ( parentItemId, attributePath ) ], callback );
            },
            get: ( { parentItemId, attributePath, start, stop }, callback ) => {
                return redisClient.lrange ( [ getKey ( parentItemId, attributePath ), start, stop ], callback );
            },
            getAll: ( { parentItemId, attributePath }, callback ) => {
                return H.wrapCallback ( R.bind ( redisClient.exists, redisClient ) )( [ getKey ( parentItemId, attributePath ) ] )
                    .flatMap ( exists => {
                        if ( exists ) {
                            return H.wrapCallback ( atomicAttributes.list.get )( { parentItemId, attributePath, start: 0, stop: -1 } )
                        }
                        return H ( [ null ] );
                    } )
                    .toCallback ( callback );
            },
            delete: ( { parentItemId, attributePath }, callback ) => {
                return redisClient.del ( [ getKey ( parentItemId, attributePath ) ], callback );
            }
        },
        set: {
            validate: validateList ( 'set' ),
            validateSingle: member => {
                return R.type ( member ) === 'String';
            },
            create: ( { parentItemId, attributePath, members }, callback ) => {
                return H.wrapCallback ( atomicAttributes.set.delete )( { parentItemId, attributePath } )
                    .flatMap ( () => {
                        return H.wrapCallback ( atomicAttributes.set.add )( { parentItemId, attributePath, members } );
                    } )
                    .toCallback ( callback );
            },
            add: ( { parentItemId, attributePath, members }, callback ) => {
                return redisClient.sadd ( [ getKey ( parentItemId, attributePath ), ...oneOrMany ( members ) ], callback );
            },
            remove: ( { parentItemId, attributePath, members }, callback ) => {
                return redisClient.srem ( [ getKey ( parentItemId, attributePath ), ...oneOrMany ( members ) ], callback );
            },
            card: ( { parentItemId, attributePath }, callback ) => {
                return redisClient.scard ( [ getKey ( parentItemId, attributePath ) ], callback );
            },
            getRandom: ( { parentItemId, attributePath, count }, callback ) => {
                return redisClient.srandmember ( [ getKey ( parentItemId, attributePath ), count || 1 ], callback );
            },
            get: ( { parentItemId, attributePath }, callback ) => {
                return redisClient.smembers ( [ getKey ( parentItemId, attributePath ) ], callback );
            },
            has: ( { parentItemId, attributePath, member }, callback ) => {
                return redisClient.sismember ( [ getKey ( parentItemId, attributePath ), member ], callback );
            },
            getAll: ( { parentItemId, attributePath }, callback ) => {
                return H.wrapCallback ( R.bind ( redisClient.exists, redisClient ) )( [ getKey ( parentItemId, attributePath ) ] )
                    .flatMap ( exists => {
                        if ( exists ) {
                            return H.wrapCallback ( atomicAttributes.set.get )( { parentItemId, attributePath } )
                        }
                        return H ( [ null ] );
                    } )
                    .toCallback ( callback );
            },
            delete: ( { parentItemId, attributePath }, callback ) => {
                return redisClient.del ( [ getKey ( parentItemId, attributePath ) ], callback );
            }
        },
        zset: {
            validate: validateList ( 'zset' ),
            validateSingle: member => {
                return R.type ( member ) === 'Object' && R.type ( member.score ) === 'Number' && R.type ( member.member ) === 'String';
            },
            create: ( { parentItemId, attributePath, members }, callback ) => {
                return H.wrapCallback ( atomicAttributes.zset.delete )( { parentItemId, attributePath } )
                    .flatMap ( () => {
                        return H.wrapCallback ( atomicAttributes.zset.add )( { parentItemId, attributePath, members } );
                    } )
                    .toCallback ( callback );
            },
            add: ( { parentItemId, attributePath, members }, callback ) => {
                return redisClient.zadd ( [ getKey ( parentItemId, attributePath ), ...zParmsFromMembers ( members ) ], callback );
            },
            remove: ( { parentItemId, attributePath, members }, callback ) => {
                return redisClient.zrem ( [ getKey ( parentItemId, attributePath ), ...zRemoveParmsFromMembers ( members ) ], callback );
            },
            removeRange: ( { parentItemId, attributePath, start, stop }, callback ) => {
                return redisClient.zremrangebyrank ( [ getKey ( parentItemId, attributePath ), start, stop ], callback );
            },
            removeRangeByScore: ( { parentItemId, attributePath, start, stop }, callback ) => {
                return redisClient.zremrangebyscore ( [ getKey ( parentItemId, attributePath ), start, stop ], callback );
            },
            card: ( { parentItemId, attributePath }, callback ) => {
                return redisClient.zcard ( [ getKey ( parentItemId, attributePath ) ], callback );
            },
            getRange: ( { parentItemId, attributePath, start, stop }, callback ) => {
                return redisZget ( 'zrange', [ getKey ( parentItemId, attributePath ), start, stop, 'WITHSCORES' ], callback );
            },
            getRevRange: ( { parentItemId, attributePath, start, stop }, callback ) => {
                return redisZget ( 'zrevrange', [ getKey ( parentItemId, attributePath ), start, stop, 'WITHSCORES' ], callback );
            },
            getRangeByScore: ( { parentItemId, attributePath, start, stop }, callback ) => {
                return redisZget ( 'zrangebyscore', [ getKey ( parentItemId, attributePath ), start, stop, 'WITHSCORES' ], callback );
            },
            getRevRangeByScore: ( { parentItemId, attributePath, start, stop }, callback ) => {
                return redisZget ( 'zrevrangebyscore', [ getKey ( parentItemId, attributePath ), start, stop, 'WITHSCORES' ], callback );
            },
            getAll: ( { parentItemId, attributePath }, callback ) => {
                return H.wrapCallback ( R.bind ( redisClient.exists, redisClient ) )( [ getKey ( parentItemId, attributePath ) ] )
                    .flatMap ( exists => {
                        if ( exists ) {
                            return H.wrapCallback ( atomicAttributes.zset.getRange )( { parentItemId, attributePath, start: 0, stop: -1 } )
                        }
                        return H ( [ null ] );
                    } )
                    .toCallback ( callback );
            },
            delete: ( { parentItemId, attributePath }, callback ) => {
                return redisClient.del ( [ getKey ( parentItemId, attributePath ) ], callback );
            }
        },
        hll: {
            validate: validateList ( 'hll' ),
            validateSingle: member => {
                return R.type ( member ) === 'String';
            },
            create: ( { parentItemId, attributePath, members }, callback ) => {
                return H.wrapCallback ( atomicAttributes.hll.delete )( { parentItemId, attributePath } )
                    .flatMap ( () => {
                        return H.wrapCallback ( atomicAttributes.hll.add )( { parentItemId, attributePath, members } );
                    } )
                    .toCallback ( callback );
            },
            add: ( { parentItemId, attributePath, members }, callback ) => {
                return redisClient.pfadd ( [ getKey ( parentItemId, attributePath ), ...oneOrMany ( members ) ], callback );
            },
            card: ( { parentItemId, attributePath }, callback ) => {
                return redisClient.pfcount ( [ getKey ( parentItemId, attributePath ) ], callback );
            },
            getAll: ( { parentItemId, attributePath }, callback ) => {
                return H.wrapCallback ( R.bind ( redisClient.exists, redisClient ) )( [ getKey ( parentItemId, attributePath ) ] )
                    .flatMap ( exists => {
                        if ( exists ) {
                            return H.wrapCallback ( atomicAttributes.hll.card )( { parentItemId, attributePath, start: 0, stop: -1 } )
                        }
                        return H ( [ null ] );
                    } )
                    .toCallback ( callback );
            },
            delete: ( { parentItemId, attributePath }, callback ) => {
                return redisClient.del ( [ getKey ( parentItemId, attributePath ) ], callback );
            }
        }
    };

    return atomicAttributes;
};

if ( ! module.parent ) {
    const redisClient = require ( 'redis' ).createClient ();
    const AA = module.exports ( redisClient );
    const parentItemId = 'item-id';
    const attributePath = 'something.or.other';

    const printResult = result => {
        if ( R.type ( result ) === 'Object' || R.type ( result ) === 'Array' ) {
            return JSON.stringify ( result );
        }

        return result;
    };

    const checkResult = ( command, expected ) => H.wrapCallback ( ( result, callback ) => {
        if ( ! R.equals ( result, expected ) ) {
            return callback ( `${command} result is ${printResult ( result )}, not ${printResult ( expected )}` );
        }

        return callback ( null, result );
    } );

    const checkResultSet = ( command, expected ) => H.wrapCallback ( ( result, callback ) => {
        const equals = ( a, b ) => {
            if ( R.type ( a ) === 'Array' ) {
                return R.isEmpty ( R.difference ( a, b ) );
            }

            return R.equals ( a, b );
        };

        if ( ! equals ( result, expected ) ) {
            return callback ( `${command} result is ${printResult ( result )}, not ${printResult ( expected )}` );
        }

        return callback ( null, result );
    } );

    const test = ( type, method, parms, expected ) => {
        const check = type === 'set' ? checkResultSet : checkResult;
        return H ( [ `testing ${type}.${method} with ${printResult ( parms )}, expecting ${printResult ( expected )} ...` ] )
            .doto ( console.log )
            .flatMap ( () => {
                return H.wrapCallback ( AA[type][method] )( { parentItemId, attributePath, ...parms } )
                    .flatMap ( check ( `${type}.${method}`, expected ) );
            } );
    };

    return test ( 'list', 'create', { members: [ 'first', 'second' ] }, 2 )
        .flatMap ( R.always ( test ( 'list', 'create', { members: [ 'first', 'second', 'third' ] }, 3 ) ) )
        .flatMap ( R.always ( test ( 'list', 'get', { start: 0, stop: -1 }, [ 'first', 'second', 'third' ] ) ) )
        .flatMap ( R.always ( test ( 'list', 'get', { start: 0, stop: 1 }, [ 'first', 'second' ] ) ) )
        .flatMap ( R.always ( test ( 'list', 'get', { start: 0, stop: 0 }, [ 'first' ] ) ) )
        .flatMap ( R.always ( test ( 'list', 'get', { start: 2, stop: 2 }, [ 'third' ] ) ) )
        .flatMap ( R.always ( test ( 'list', 'getIndex', { index: 2 }, 'third' ) ) )
        .flatMap ( R.always ( test ( 'list', 'getIndex', { index: 0 }, 'first' ) ) )
        .flatMap ( R.always ( test ( 'list', 'addStart', { members: 'zeroth' }, 4 ) ) )
        .flatMap ( R.always ( test ( 'list', 'addEnd', { members: 'fourth' }, 5 ) ) )
        .flatMap ( R.always ( test ( 'list', 'get', { start: 0, stop: -1 }, [ 'zeroth', 'first', 'second', 'third', 'fourth' ] ) ) )
        .flatMap ( R.always ( test ( 'list', 'getIndex', { index: 2 }, 'second' ) ) )
        .flatMap ( R.always ( test ( 'list', 'getIndex', { index: 0 }, 'zeroth' ) ) )
        .flatMap ( R.always ( test ( 'list', 'insertBefore', { member: 'halfth', pivotMember:'first' }, 6 ) ) )
        .flatMap ( R.always ( test ( 'list', 'insertAfter', { member: 'oneAndAHalfth', pivotMember: 'first' }, 7 ) ) )
        .flatMap ( R.always ( test ( 'list', 'get', { start: 0, stop: -1 }, [ 'zeroth', 'halfth', 'first', 'oneAndAHalfth', 'second', 'third', 'fourth' ] ) ) )
        .flatMap ( R.always ( test ( 'list', 'setIndex', { member: 'quarter', index: 1 }, 'OK' ) ) )
        .flatMap ( R.always ( test ( 'list', 'getIndex', { index: 1 }, 'quarter' ) ) )
        .flatMap ( R.always ( test ( 'list', 'card', {}, 7 ) ) )
        .flatMap ( R.always ( test ( 'list', 'delete', {}, 1 ) ) )
        .flatMap ( R.always ( test ( 'list', 'card', {}, 0 ) ) )
        .flatMap ( R.always ( test ( 'list', 'get', { start: 0, stop: -1 }, [] ) ) )

        .flatMap ( R.always ( test ( 'set', 'create', { members: [ 'first', 'second', 'third' ] }, 3 ) ) )
        .flatMap ( R.always ( test ( 'set', 'create', { members: [ 'first', 'first', 'second', 'third' ] }, 3 ) ) )
        .flatMap ( R.always ( test ( 'set', 'get', {}, [ 'first', 'second', 'third' ] ) ) )
        .flatMap ( R.always ( test ( 'set', 'has', { member: 'first' }, 1 ) ) )
        .flatMap ( R.always ( test ( 'set', 'has', { member: 'second' }, 1 ) ) )
        .flatMap ( R.always ( test ( 'set', 'has', { member: 'third' }, 1 ) ) )
        .flatMap ( R.always ( test ( 'set', 'has', { member: 'fourth' }, 0 ) ) )
        .flatMap ( R.always ( test ( 'set', 'card', {}, 3 ) ) )
        .flatMap ( R.always ( test ( 'set', 'add', { members: [ 'fourth', 'fifth' ] }, 2 ) ) )
        .flatMap ( R.always ( test ( 'set', 'get', {}, [ 'first', 'second', 'third', 'fourth', 'fifth' ] ) ) )
        .flatMap ( R.always ( test ( 'set', 'card', {}, 5 ) ) )
        .flatMap ( R.always ( test ( 'set', 'remove', { members: [ 'first', 'third' ] }, 2 ) ) )
        .flatMap ( R.always ( test ( 'set', 'get', {}, [ 'second', 'fourth', 'fifth' ] ) ) )
        .flatMap ( R.always ( test ( 'set', 'card', {}, 3 ) ) )
        .flatMap ( R.always ( test ( 'set', 'delete', {}, 1 ) ) )
        .flatMap ( R.always ( test ( 'set', 'card', {}, 0 ) ) )
        .flatMap ( R.always ( test ( 'set', 'get', {}, [] ) ) )

        .flatMap ( R.always ( test ( 'zset', 'create', { members: [ { score: 0, member: 'first' }, { score: 1, member: 'second' }, { score: 2, member: 'third' } ] }, 3 ) ) )
        .flatMap ( R.always ( test ( 'zset', 'create', { members: [ { score: 0, member: 'first' }, { score: 1, member: 'first' }, { score: 2, member: 'second' }, { score: 3, member: 'third' } ] }, 3 ) ) )
        .flatMap ( R.always ( test ( 'zset', 'getRange', { start: 0, stop: -1 }, [ { score: 1, member: 'first' }, { score: 2, member: 'second' }, { score: 3, member: 'third' } ] ) ) )
        .flatMap ( R.always ( test ( 'zset', 'getRevRange', { start:  0, stop: -1 }, R.reverse ( [ { score: 1, member: 'first' }, { score: 2, member: 'second' }, { score: 3, member: 'third' } ] ) ) ) )
        .flatMap ( R.always ( test ( 'zset', 'getRange', { start: 0, stop: 1 }, [ { score: 1, member: 'first' }, { score: 2, member: 'second' } ] ) ) )
        .flatMap ( R.always ( test ( 'zset', 'getRange', { start: 1, stop: 2 }, [ { score: 2, member: 'second' }, { score: 3, member: 'third' } ] ) ) )
        .flatMap ( R.always ( test ( 'zset', 'getRevRange', { start: 0, stop: 1 }, [ { score: 3, member: 'third' }, { score: 2, member: 'second' } ] ) ) )
        .flatMap ( R.always ( test ( 'zset', 'getRangeByScore', { start: -Infinity, stop: Infinity }, [ { score: 1, member: 'first' }, { score: 2, member: 'second' }, { score: 3, member: 'third' } ] ) ) )
        .flatMap ( R.always ( test ( 'zset', 'getRangeByScore', { start: 1, stop: 2 }, [ { score: 1, member: 'first' }, { score: 2, member: 'second' } ] ) ) )
        .flatMap ( R.always ( test ( 'zset', 'getRangeByScore', { start: '(1', stop: 2 }, [ { score: 2, member: 'second' } ] ) ) )
        .flatMap ( R.always ( test ( 'zset', 'getRangeByScore', { start: 2, stop: Infinity }, [ { score: 2, member: 'second' }, { score: 3, member: 'third' } ] ) ) )
        .flatMap ( R.always ( test ( 'zset', 'getRevRangeByScore', { start: Infinity, stop: -Infinity }, R.reverse ( [ { score: 1, member: 'first' }, { score: 2, member: 'second' }, { score: 3, member: 'third' } ] ) ) ) )
        .flatMap ( R.always ( test ( 'zset', 'getRevRangeByScore', { start: 2, stop: 1 }, R.reverse ( [ { score: 1, member: 'first' }, { score: 2, member: 'second' } ] ) ) ) )
        .flatMap ( R.always ( test ( 'zset', 'getRevRangeByScore', { start: '(2', stop: 1 }, R.reverse ( [ { score: 1, member: 'first' } ] ) ) ) )
        .flatMap ( R.always ( test ( 'zset', 'delete', {}, 1 ) ) )
        .flatMap ( R.always ( test ( 'zset', 'card', {}, 0 ) ) )
        .flatMap ( R.always ( test ( 'zset', 'getRange', { start: 0, stop: -1 }, [] ) ) )

        .flatMap ( R.always ( test ( 'hll', 'create', { members: [ 'first', 'second', 'third' ] }, 1 ) ) )
        .flatMap ( R.always ( test ( 'hll', 'card', {}, 3 ) ) )
        .flatMap ( R.always ( test ( 'hll', 'create', { members: [ 'first', 'first', 'second', 'third' ] }, 1 ) ) )
        .flatMap ( R.always ( test ( 'hll', 'card', {}, 3 ) ) )
        .flatMap ( R.always ( test ( 'hll', 'add', { members: [ 'fourth' ] }, 1 ) ) )
        .flatMap ( R.always ( test ( 'hll', 'card', {}, 4 ) ) )
        .flatMap ( R.always ( test ( 'hll', 'add', { members: [ 'fifth', 'sixth' ] }, 1 ) ) )
        .flatMap ( R.always ( test ( 'hll', 'card', {}, 6 ) ) )
        .flatMap ( R.always ( test ( 'hll', 'delete', {}, 1 ) ) )
        .flatMap ( R.always ( test ( 'hll', 'card', {}, 0 ) ) )

        .stopOnError ( error => {
            console.error ( 'ERROR', error );
            redisClient.quit ();
        } )
        .each ( result => {
            redisClient.quit ();
            console.log ( result );
        } );
}
