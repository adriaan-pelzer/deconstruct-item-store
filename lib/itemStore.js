const R = require ( 'ramda' );
const H = require ( 'highland' );
const aws = require ( 'aws-sdk' );
const redis = require ( 'redis' );
const uuid = require ( 'uuid' ), generateId = uuid.v4;
const md5 = require ( 'md5' );

const configs = [];

const log = R.compose ( console.log, R.partialRight ( JSON.stringify, [ null, 4 ] ) );

const queryObject = require ( './queryObject.js' );
const atomicAttributes = require ( './atomicAttributes.js' );

const getDynamoClient = stub => {
    return stub || new aws.DynamoDB.DocumentClient ( { region: 'eu-west-1' } );
};

const getRedisClient = stub => {
    const client = stub || redis.createClient ( {
        port: process.env['REDIS_PORT'] || R.path ( [ 'redis', 'port' ], R.head ( configs ) ) || 6379,
        host: process.env['REDIS_HOST'] || R.path ( [ 'redis', 'host' ], R.head ( configs ) ) || 'localhost'
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

    return { ...R.fromPairs ( R.map ( wrapRedisMethod, supportedMethods ) ), quit: () => {
        return client.quit ();
    } };
};

const clients = {
    uuid: null,
    redis: null,
    dynamo: null
};

const getClients = ( stubs, call ) => {
    if ( clients.uuid === null ) {
        clients.uuid = generateId ();
    }

    if ( clients.redis === null ) {
        clients.redis = getRedisClient ( stubs && stubs.redis );
    }

    if ( clients.dynamo === null ) {
        clients.dynamo = getDynamoClient ( stubs && stubs.dynamo );
    }

    return clients;
};

const generateItemSortedSetNames = encapsulatedItem => {
    return R.reduce ( ( listNames, key ) => ( R.concat ( listNames, R.map ( value => {
        return [ 'items', encapsulatedItem.type, key, value.toString () ].join ( '-' );
    }, R.flatten ( R.of ( encapsulatedItem[key] ) ) ) ) ), [ [ 'items', encapsulatedItem.type ].join ( '-' ) ], R.filter ( key => {
        return R.contains ( R.type ( encapsulatedItem[key] ), [ 'String', 'Array', 'Number', 'Boolean' ] );
    }, R.keys ( R.omit ( [ 'id', 'type', 'publishedTime', 'lastModifiedTime', 'lat', 'lng', 'item', 'previousId' ], encapsulatedItem ) ) ) );
};

const geoSetName = type => [ 'items', type, 'geo' ].join ( '-' );
const removedSetName = type => [ 'items', type, 'removed' ].join ( '-' );

const getAttributePaths = ( typeType, type ) => {
    const config = R.head ( configs );

    const commands = {
        atomic: 'reject',
        default: 'filter'
    };

    return R[commands[typeType] || 'filter'] ( path => {
        return R.isNil ( path.type );
    }, R.concat ( R.path ( [ 'typeConfigs', type, 'metadataPaths' ], config ) || [], config.automaticMetadataPaths || [] ) );
};

const encapsulateItem = ( type, id, prevItem, item ) => {
    const config = R.head ( configs );
    const time = new Date ().valueOf ();
    const automaticMetadataPaths = config.automaticMetadataPaths || [];
    const mergedItem = R.merge ( R.fromPairs ( R.reject ( R.isNil, R.map ( pathObj => {
        if ( R.path ( pathObj.path, prevItem ) ) {
            return [ R.last ( pathObj.path ), R.path ( pathObj.path, prevItem ) ];
        }
        return null;
    }, automaticMetadataPaths ) ) ), item );

    const metadataPaths = getAttributePaths ( 'default', type );

    const pathAttrTxFuncs = {
        boolean: value => {
            return !!value;
        },
        string: value => {
            return value.toString ();
        },
        int: value => {
            return parseInt ( value, 10 );
        },
        float: value => {
            return parseFloat ( value );
        },
        md5Hash: value => {
            return md5 ( value );
        },
        toLowerCase: value => {
            return value.toString ().toLowerCase ();
        },
        uriLastPathComp: value => {
            return R.last ( value.split ( '/' ) );
        }
    };

    return R.reduce ( R.merge, {}, [
        R.reduce ( ( encapsulatedItem, pathObj ) => {
            const path = pathObj.path, pathAttrs = pathObj.attrs || [];

            if ( R.isNil ( R.path ( path, mergedItem ) ) ) {
                return encapsulatedItem;
            }

            return R.merge ( encapsulatedItem, R.fromPairs ( [ [ R.last ( path ), R.reduce ( ( value, key ) => {
                if ( ! R.has ( key, pathAttrTxFuncs ) || R.type ( pathAttrTxFuncs[key] ) !== 'Function' ) {
                    return value;
                }

                if ( R.type ( value ) === 'Array' ) {
                    return R.map ( pathAttrTxFuncs[key], value );
                }

                return pathAttrTxFuncs[key] ( value );
            }, R.path ( path, mergedItem ), pathAttrs ) ] ] ) );
        }, {
            id: id,
            iid: id,
            publishedTime: R.path ( [ 'publishedTime' ], prevItem ) || time,
            lastModifiedTime: time,
            type: type
        }, metadataPaths ),
        { item: mergedItem },
        prevItem ? {
            iid: prevItem.iid || prevItem.id,
            previous_id: prevItem.id
        } : {}
    ] );
};

const listItem = redisClient => {
    const config = R.head ( configs );
    const atomicHelper = atomicAttributes ( redisClient );
    const listItemHelper = ( redisMethodname, encapsulatedItem, parms, callback ) => {
        const itemSortedSets = ( redisMethodname === 'geoadd' ) ? [ geoSetName ( encapsulatedItem.type ) ] : generateItemSortedSetNames ( encapsulatedItem );

        return H ( itemSortedSets )
            .map ( key => R.concat ( [ key ], parms ) )
            .flatMap ( H.wrapCallback ( R.bind ( redisClient[redisMethodname], redisClient ) ) )
            .collect ()
            .map ( R.zip ( itemSortedSets ) )
            .map ( R.fromPairs )
            .toCallback ( callback );
    };

    const validateLongLat = encapsulatedItem => {
        if ( ! encapsulatedItem.lng || ! encapsulatedItem.lat ) {
            return false;
        }

        if ( parseFloat ( encapsulatedItem.lng ) > 180 || parseFloat ( encapsulatedItem.lng ) < -180 ) {
            return false;
        }

        if ( parseFloat ( encapsulatedItem.lat ) > 85.05112878 || parseFloat ( encapsulatedItem.lat ) < -85.05112878 ) {
            return false;
        }

        return true;
    };

    return {
        atomicAdd: ( encapsulatedItem, callback ) => {
            return H ( getAttributePaths ( 'atomic', encapsulatedItem.type ) )
                .filter ( atomicAttributePath => R.not ( R.isNil ( R.path ( [ 'item', ...atomicAttributePath.path ], encapsulatedItem ) ) ) )
                .map ( atomicAttributePath => {
                    const redisKey = [ encapsulatedItem.iid, ...atomicAttributePath.path ].join ( '-' );

                    return H.wrapCallback ( atomicHelper[atomicAttributePath.type].create )( {
                        parentItemId: encapsulatedItem.iid,
                        attributePath: atomicAttributePath.path,
                        members: R.path ( [ 'item', ...atomicAttributePath.path ], encapsulatedItem )
                    } )
                        .map ( result => ( [ redisKey, result ] ) );
                } )
                .parallel ( 100 )
                .collect ()
                .map ( R.fromPairs )
                .toCallback ( callback );
        },
        atomicRemove: ( encapsulatedItem, callback ) => {
            return H ( getAttributePaths ( 'atomic', encapsulatedItem.type ) )
                .filter ( atomicAttributePath => R.not ( R.isNil ( R.path ( [ 'item', ...atomicAttributePath.path ], encapsulatedItem ) ) ) )
                .map ( atomicAttributePath => {
                    const redisKey = [ encapsulatedItem.iid, ...atomicAttributePath.path ].join ( '-' );

                    return H.wrapCallback ( atomicHelper[atomicAttributePath.type].delete )( {
                        parentItemId: encapsulatedItem.iid,
                        attributePath: atomicAttributePath.path
                    } )
                        .map ( result => ( [ redisKey, result ] ) );
                } )
                .parallel ( 100 )
                .collect ()
                .map ( R.fromPairs )
                .toCallback ( callback );
        },
        atomicGet: ( encapsulatedItem, callback ) => {
            return H ( R.map ( atomicAttributePath => {
                return H.wrapCallback ( atomicHelper[atomicAttributePath.type].getAll )( {
                    parentItemId: encapsulatedItem.iid,
                    attributePath: atomicAttributePath.path
                } )
                    .flatMap ( result => {
                        if ( result === null ) {
                            return H ( [ null ] );
                        }

                        if ( R.contains ( 'dehydrated', atomicAttributePath.attrs || [] ) ) {
                            return H.wrapCallback ( atomicHelper[atomicAttributePath.type].card )( {
                                parentItemId: encapsulatedItem.iid,
                                attributePath: atomicAttributePath.path
                            } )
                                .map ( card => ( {
                                    type: atomicAttributePath.type,
                                    attrs: atomicAttributePath.attrs,
                                    cardinality: card
                                } ) );
                        }

                        return H ( [ result ] );
                    } )
                    .map ( result => ( [ atomicAttributePath.path, result ] ) );
            }, getAttributePaths ( 'atomic', encapsulatedItem.type ) ) )
                .parallel ( 100 )
                .reject ( result => result[1] === null )
                .reduce ( {}, ( item, [ path, result ] ) => {
                    return R.assocPath ( path, result, item );
                } )
                .toCallback ( callback );
        },
        atomicOp: ( dehydratedItem, attributePath, command, parms, callback ) => {
            const atomicAttributePath = R.find ( atomicAttributePath => {
                return atomicAttributePath.path.join ( '.' ) === R.flatten ( [ attributePath ] ).join ( '.' );
            }, getAttributePaths ( 'atomic', dehydratedItem.type ) );

            if ( ! atomicAttributePath ) {
                return callback ( {
                    code: 404,
                    message: `Type ${dehydratedItem.type} does not have an atomic attribute called ${R.flatten ( [ attributePath ] ).join ( '.' )}`
                } );
            }

            return atomicHelper[atomicAttributePath.type][command] ( parms, callback );
        },
        iidGet: ( itemType, itemId, callback ) => {
            return H.wrapCallback ( R.bind ( redisClient.get, redisClient ) )( [ [ itemId, 'iid' ].join ( '-' ) ] )
                .flatMap ( iid => {
                    if ( R.isNil ( iid ) ) {
                        return H.wrapCallback ( getItem )( null, itemType, itemId )
                            .errors ( ( error, push ) => {
                                if ( error.code === 404 ) {
                                    return push ( null, { iid: itemId } )
                                }
                                return push ( error );
                            } )
                            .map ( item => item.iid || item.id );
                    }

                    return H ( [ iid ] );
                } )
                .toCallback ( callback );
        },
        list: ( encapsulatedItem, callback ) => {
            return H ( [
                H.wrapCallback ( listItemHelper )( 'zadd', encapsulatedItem, [ encapsulatedItem.lastModifiedTime, encapsulatedItem.id ] ),
                H.wrapCallback ( R.bind ( redisClient.set, redisClient ) )( [
                    [ encapsulatedItem.id, 'iid' ].join ( '-' ),
                    encapsulatedItem.iid || encapsulatedItem.id
                ] )
            ] )
                .parallel ( 2 )
                .collect ()
                .map ( R.head )
                .toCallback ( callback );
        },
        deList: ( encapsulatedItem, callback ) => {
            return H ( [
                H.wrapCallback ( listItemHelper )( 'zrem', encapsulatedItem, [ encapsulatedItem.id ] ),
                H.wrapCallback ( R.bind ( redisClient.del, redisClient ) )( [
                    [ encapsulatedItem.id, 'iid' ].join ( '-' )
                ] )
            ] )
                .parallel ( 2 )
                .collect ()
                .map ( R.head )
                .toCallback ( callback );
        },
        geoList: ( encapsulatedItem, callback ) => {
            if ( validateLongLat ( encapsulatedItem ) ) {
                return listItemHelper ( 'geoadd', encapsulatedItem, [ parseFloat ( encapsulatedItem.lng ), parseFloat ( encapsulatedItem.lat ), encapsulatedItem.id ], callback );
            }
            return callback ( null, R.fromPairs ( [ [ geoSetName ( encapsulatedItem.type ), 0 ] ] ) );
        },
        geoDeList: ( encapsulatedItem, callback ) => {
            if ( validateLongLat ( encapsulatedItem ) ) {
                return redisClient.zrem ( [ geoSetName ( encapsulatedItem.type ), encapsulatedItem.id ], ( error, result ) => {
                    return callback ( error, R.fromPairs ( [ [ geoSetName ( encapsulatedItem.type ), result ] ] ) );
                } );
            }
            return callback ( null, R.fromPairs ( [ [ geoSetName ( encapsulatedItem.type ), 0 ] ] ) );
        },
        remove: ( encapsulatedItem, callback ) => {
            return redisClient.zadd ( [ removedSetName ( encapsulatedItem.type ), encapsulatedItem.lastModifiedTime, encapsulatedItem.id ], ( error, result ) => {
                return callback ( error, R.fromPairs ( [ [ removedSetName ( encapsulatedItem.type ), result ] ] ) );
            } );
        }
    };
};

const storeItem = dynamoClient => {
    const storeItemHelper = ( dynamoMethodname, parms, callback ) => {
        return dynamoClient[dynamoMethodname] ( R.merge ( {
            TableName: process.env['DYNAMO_TABLE'] || R.path ( [ 'dynamo', 'table' ], R.head ( configs ) ) || 'items'
        }, parms ), ( error, data ) => {
            if ( error ) {
                const code = error.statusCode || error.code || 500;
                const message = error.message || error;
                const E = {
                    code: code >= 300 ? code : 500,
                    message: message
                };

                log ( E );
                return callback ( E );
            }

            return callback ( null, {
                put: 1,
                delete: 1,
                get: data
            }[dynamoMethodname] );
        } );
    };

    const makeEmptyStringsNull = obj => {
        if ( R.type ( obj ) === 'Array' ) {
            return R.map ( makeEmptyStringsNull, obj );
        }

        if ( R.type ( obj ) === 'Object' ) {
            return R.fromPairs ( R.map ( pair => {
                return [ pair[0], makeEmptyStringsNull ( pair[1] ) ];
            }, R.toPairs ( obj ) ) );
        }

        if ( R.type ( obj ) === 'String' && R.isEmpty ( obj ) ) {
            return null;
        }

        return obj;
    };

    return {
        store: ( encapsulatedItem, callback ) => {
            return storeItemHelper ( 'put', { Item: R.merge ( { HashKey: encapsulatedItem.id }, makeEmptyStringsNull ( encapsulatedItem ) ) }, callback );
        },
        remove: ( encapsulatedItem, callback ) => {
            return storeItemHelper ( 'delete', { Key: { HashKey: encapsulatedItem.id } }, callback );
        },
        get: ( hashKey, callback ) => {
            return storeItemHelper ( 'get', { Key: { HashKey: hashKey } }, callback );
        }
    };
};

const saveItemRaw = ( stubs, encapsulatedItem, callback ) => {
    const clients = getClients ( stubs, 'saveItemRaw' );
    const strippedEncapsulatedItem = R.reduce ( ( encapsulatedItem, atomicAttributePath ) => {
        return R.dissocPath ( [ 'item', ...atomicAttributePath.path ], encapsulatedItem );
    }, encapsulatedItem, getAttributePaths ( 'atomic', encapsulatedItem.type ) );

    return H ( [
        H.wrapCallback ( listItem ( clients.redis ).atomicAdd )( encapsulatedItem ),
        H.wrapCallback ( listItem ( clients.redis ).list )( strippedEncapsulatedItem ),
        H.wrapCallback ( listItem ( clients.redis ).geoList )( strippedEncapsulatedItem ),
        H.wrapCallback ( storeItem ( clients.dynamo ).store )( strippedEncapsulatedItem )
    ] )
        .parallel ( 4 )
        .collect ()
        .map ( R.zip ( [ 'atomic', 'list', 'geolist', 'store' ] ) )
        .map ( R.fromPairs )
        .map ( R.merge ( {
            id: encapsulatedItem.id,
            iid: encapsulatedItem.iid,
            type: encapsulatedItem.type
        } ) )
        .stopOnError ( ( error, push ) => {
            return push ( null, { error } );
        } )
        .flatMap ( response => {
            if ( response.error ) {
                return H ( [
                    H.wrapCallback ( listItem ( clients.redis ).atomicRemove )( encapsulatedItem ),
                    H.wrapCallback ( listItem ( clients.redis ).deList )( encapsulatedItem ),
                    H.wrapCallback ( listItem ( clients.redis ).geoDeList )( encapsulatedItem )
                ] )
                    .parallel ( 3 )
                    .collect ()
                    .stopOnerror ( ( error, push ) => {
                        return push ( null, null );
                    } )
                    .flatMap ( H.wrapCallback ( ( notUsed, callback ) => {
                        return callback ( response.error );
                    } ) );
            }

            return H ( [ response ] );
        } )
        .toCallback ( callback );
};

const saveItem = ( stubs, type, id, prevItem, item, callback ) => {
    return saveItemRaw ( stubs, encapsulateItem ( type, id, prevItem, item ), callback );
};

const deleteItem = ( stubs, type, id, callback ) => {
    const clients = getClients ( stubs, 'deleteItem' );

    return H.wrapCallback ( getItem )( stubs, type, id )
        .flatMap ( H.wrapCallback ( ( encapsulatedItem, callback ) => {
            if ( encapsulatedItem.type !== type ) {
                return callback ( { code: 404, message: `There is such an item, but not of type ${type}; its type is ${encapsulatedItem.type}` } );
            }

            return callback ( null, encapsulatedItem );
        } ) )
        .flatMap ( encapsulatedItem => {
            return H ( [
                H.wrapCallback ( listItem ( clients.redis ).atomicRemove )( encapsulatedItem ),
                H.wrapCallback ( listItem ( clients.redis ).deList )( encapsulatedItem ),
                H.wrapCallback ( listItem ( clients.redis ).geoDeList )( encapsulatedItem )
            ] );
        } )
        .parallel ( 3 )
        .collect ()
        .map ( R.zip ( [ 'atomic', 'list', 'geolist' ] ) )
        .map ( R.fromPairs )
        .toCallback ( callback );
};

const getItem = ( stubs, type, id, callback ) => {
    const clients = getClients ( stubs, 'getItem' );
    const redisClient = clients.redis;

    const redisStream = redisMethodName => {
        return H.wrapCallback ( R.bind ( redisClient[redisMethodName], redisClient ) );
    };

    return redisStream ( 'zscore' )( [ `items-${type}`, id ] )
        .flatMap ( score => {
            return H.wrapCallback ( storeItem ( clients.dynamo ).get )( id )
                .pluck ( 'Item' )
                .map ( R.omit ( [ 'HashKey' ] ) )
                .flatMap ( H.wrapCallback ( ( encapsulatedItem, callback ) => {
                    if ( R.isNil ( encapsulatedItem.type ) ) {
                        return callback ( { code: 404, message: `There is no such item` } );
                    }

                    if ( encapsulatedItem.type !== type ) {
                        return callback ( { code: 404, message: `We can find such an item, but of type '${encapsulatedItem.type}', not '${type}'` } );
                    }

                    if ( R.isNil ( score ) ) {
                        return callback ( { code: 404, message: `There was such an item, but it's been deleted` } );
                    }

                    if ( encapsulatedItem.type !== type ) {
                        return callback ( { code: 404, message: `There is such an item, but not of type ${type}; its type is ${encapsulatedItem.type}` } );
                    }

                    return callback ( null, encapsulatedItem );
                } ) );
        } )
        .flatMap ( item => {
            return H.wrapCallback ( listItem ( redisClient ).atomicGet )( item )
                .map ( atomicItem => ( {
                    ...item,
                    item: { ...item.item, ...atomicItem }
                } ) );
        } )
        .toCallback ( callback );
};

const generateItemSet = ( clients, type, query ) => {
    const redisClient = clients.redis;
    const sortedSetNames = queryObject.queryObjToSortedSetNames ( type, queryObject.queryToObj ( query ) );

    const redisStream = redisMethodName => {
        return H.wrapCallback ( R.bind ( redisClient[redisMethodName], redisClient ) );
    };

    const redisStoreStream = ( redisMethodName, destination, keys ) => {
        if ( keys.length === 1 ) {
            return H ( [ [ keys[0], 'unknown' ] ] );
        }

        return redisStream ( redisMethodName )( R.reduce ( R.concat, [], [
            [ destination, keys.length ],
            keys,
            [ 'AGGREGATE', 'MAX' ]
        ] ) )
            .map ( result => {
                return [ destination, result ];
            } );
    };

    const mainStreams = R.map ( setNamePair => {
        return redisStoreStream ( 'zunionstore', setNamePair[0], setNamePair[1] );
    }, R.toPairs ( sortedSetNames ) );

    return H ( mainStreams )
        .parallel ( mainStreams.length )
        .collect ()
        .flatMap ( unionResults => {
            const destination = [ 'tmp', generateId () ].join ( '-' );

            if ( R.isEmpty ( unionResults ) ) {
                return H ( [ [ [ [ 'items', type ].join ( '-' ) ] ] ] );
            }

            return redisStoreStream ( 'zinterstore', destination, R.map ( R.prop ( 0 ), unionResults ) )
                .flatMap ( interResult => {
                    return H ( R.map ( key => {
                        return redisStream ( 'del' )( [ key ] );
                    }, R.filter ( key => key.match ( /^tmp-/ ), R.map ( R.head, unionResults ) ) ) )
                        .parallel ( 10 )
                        .collect ()
                        .map ( R.always ( R.head ( interResult ) ) );
                } );
        } );
};

const getItemCount = ( stubs, type, query, callback ) => {
    const clients = getClients ( stubs, 'getItemCount' );
    const redisClient = clients.redis;

    const redisStream = redisMethodName => {
        return H.wrapCallback ( R.bind ( redisClient[redisMethodName], redisClient ) );
    };

    const valiDate = ( def, dateString ) => {
        const parsedDate = new Date ( ( R.type ( dateString ) === 'String' && ( dateString.match ( /\d{10}/ ) || dateString.match ( /\d{13}/ ) ) ) ? parseInt ( dateString, 10 ) : dateString );

        if ( parsedDate.toString () === 'Invalid Date' ) {
            return def;
        }

        return parsedDate.valueOf ();
    };

    const isGeoQuery = R.path ( [ 'lat' ], query ) && R.path ( [ 'lng' ], query ) && R.path ( [ 'radius' ], query );

    return generateItemSet ( clients, type, query )
        .flatMap ( itemSet => {
            return redisStream ( 'zrevrangebyscore' )( [
                itemSet,
                valiDate ( '+inf', R.path ( [ 'before' ], query ) ),
                valiDate ( '-inf', R.path ( [ 'after' ], query ) )
            ] )
                .flatMap ( ids => {
                    if ( itemSet.match ( /^tmp-/ ) ) {
                        return redisStream ( 'del' )( [ itemSet ] )
                            .map ( R.always ( ids ) );
                    }

                    return H ( [ ids ] );
                } );
        } )
        .flatMap ( ids => {
            if ( isGeoQuery ) {
                return redisStream ( 'georadius' )( [
                    geoSetName ( type ),
                    parseFloat ( query.lng ),
                    parseFloat ( query.lat ),
                    parseInt ( query.radius, 10 ),
                    query.units || 'mi',
                    'WITHDIST',
                    'ASC'
                ] )
                    .sequence ()
                    .map ( R.head )
                    .filter ( geoId => R.contains ( geoId, ids ) )
                    .collect ()
                    .map ( R.length );
            }

            return H ( [ ids.length ] );
        } )
        .toCallback ( callback );
};

const getItemsDehydratedWithDistance = ( clients, type, query ) => {
    const redisClient = clients.redis;

    const redisStream = redisMethodName => {
        return H.wrapCallback ( R.bind ( redisClient[redisMethodName], redisClient ) );
    };

    const valiDate = ( def, dateString ) => {
        const parsedDate = new Date ( ( R.type ( dateString ) === 'String' && ( dateString.match ( /\d{10}/ ) || dateString.match ( /\d{13}/ ) ) ) ? parseInt ( dateString, 10 ) : dateString );

        if ( parsedDate.toString () === 'Invalid Date' ) {
            return def;
        }

        return parsedDate.valueOf ();
    };

    const isGeoQuery = R.path ( [ 'lat' ], query ) && R.path ( [ 'lng' ], query ) && R.path ( [ 'radius' ], query );
    const count = Math.min ( parseInt ( R.path ( [ 'count' ], query ) || 100, 10 ), 3000 );
    const offset = Math.max ( 0, parseInt ( R.path ( [ 'offset' ], query ) || 0, 10 ) );

    return generateItemSet ( clients, type, query )
        .flatMap ( itemSet => {
            return redisStream ( 'zrevrangebyscore' )( R.concat ( [
                itemSet,
                valiDate ( '+inf', R.path ( [ 'before' ], query ) ),
                valiDate ( '-inf', R.path ( [ 'after' ], query ) ),
                'WITHSCORES'
            ], isGeoQuery ? [] : [
                'LIMIT',
                0,
                count
            ] ) )
                .map ( R.splitEvery ( 2 ) )
                .flatMap ( ids => {
                    if ( itemSet.match ( /^tmp-/ ) ) {
                        return redisStream ( 'del' )( [ itemSet ] )
                            .map ( R.always ( ids ) );
                    }

                    return H ( [ ids ] );
                } );
        } )
        .flatMap ( ids => {
            const idsOnly = R.map ( R.head, ids );

            if ( isGeoQuery ) {
                return redisStream ( 'georadius' )( [
                    geoSetName ( type ),
                    parseFloat ( query.lng ),
                    parseFloat ( query.lat ),
                    parseInt ( query.radius, 10 ),
                    query.units || 'mi',
                    'WITHDIST',
                    'ASC'
                ] )
                    .sequence ()
                    .map ( geoIdAndDistance => {
                        const matchedId = R.find ( idAndScore => {
                            return R.head ( idAndScore ) === R.head ( geoIdAndDistance );
                        }, ids );

                        if ( matchedId ) {
                            return R.concat ( matchedId, [ R.last ( geoIdAndDistance ) ] );
                        }

                        return null;
                    } )
                    .compact ()
                    .take ( count + offset )
                    .collect ()
                    .map ( R.slice ( offset, count + offset ) );
            }

            return H ( [ R.map ( id => {
                return R.concat ( id, [ undefined ] );
            }, ids ) ] );
        } )
        .sequence ()
        .flatMap ( idTriplet => {
            return H.wrapCallback ( listItem ( redisClient ).iidGet )( type, idTriplet[0] )
                .map ( iid => ( [ ...idTriplet, iid ] ) );
        } )
        .map ( idQuartet => ( R.reduce ( R.merge, {}, [
            {
                id: idQuartet[0],
                iid: idQuartet[3],
                lastModifiedTime: parseInt ( idQuartet[1] )
            },
            idQuartet[2] ? {
                distance: parseFloat ( idQuartet[2] )
            } : {},
            { type }
        ] ) ) )
        .flatMap ( dehydratedItem => {
            return H.wrapCallback ( listItem ( redisClient ).atomicGet )( dehydratedItem )
                .map ( atomicItem => ( {
                    ...dehydratedItem,
                    item: atomicItem
                } ) );
        } )
        .collect ();
};

const hydrateIdsWithDistance = R.curry ( ( clients, type, dehydratedItems ) => {
    return H ( R.map ( dehydratedItem => {
        return H.wrapCallback ( getItem )( clients, type, dehydratedItem.id )
            .map ( item => ( {
                ...item,
                ...dehydratedItem,
                item: {
                    ...item.item,
                    ...dehydratedItem.item
                }
            } ) )
            .errors ( ( error, push ) => {
                if ( error.code === 404 ) {
                    return push ( null, null )
                }
                return push ( error );
            } )
            .stopOnError ( ( error, push ) => {
                return push ( error );
            } );
    }, dehydratedItems ) )
        .parallel ( 100 )
        .compact ()
        .collect ();
} );

const hydrateItemIds = ( stubs, type, ids, callback ) => {
    const clients = getClients ( stubs, 'hydrateItemIds' );

    return hydrateIdsWithDistance ( clients, type, ids )
        .toCallback ( callback );
};

const getItems = ( stubs, type, query, callback ) => {
    const clients = getClients ( stubs, 'getItems' );

    return getItemsDehydratedWithDistance ( clients, type, query )
        .flatMap ( hydrateIdsWithDistance ( clients, type ) )
        .toCallback ( callback );
};

const getItemIds = ( stubs, type, query, callback ) => {
    const clients = getClients ( stubs, 'getItemIds' );

    return getItemsDehydratedWithDistance ( clients, type, query )
        .toCallback ( callback );
};

const atomicOp = ( stubs, type, id, attributePath, command, query, callback ) => {
    const clients = getClients ( stubs, 'atomicOp' );

    return H.wrapCallback ( listItem ( clients.redis ).iidGet )( type, id )
        .flatMap ( iid => {
            return H.wrapCallback ( listItem ( clients.redis ).atomicOp )( { id, type }, attributePath, command, {
                ...query,
                parentItemId: iid,
                attributePath: R.flatten ( [ attributePath ] ).join ( '.' )
            } );
        } )
        .toCallback ( callback );
};

const loadConfig = config => {
    configs.push ( config );
};

const hydrateIds = ( stubs, type, ids, callback ) => {
    const clients = getClients ( stubs, 'hydrateIds' );

    return hydrateIdsWithDistance ( clients, type, R.zip ( ids, R.times ( R.always ( undefined ), ids.length ) ) )
        .toCallback ( callback );
};

module.exports = {
    loadConfig: loadConfig,
    getClients: getClients,
    generateId: generateId,
    encapsulateItem: encapsulateItem,
    generateItemSortedSetNames: generateItemSortedSetNames,
    listItem: R.curry ( listItem ),
    storeItem: R.curry ( storeItem ),
    saveItem: R.curry ( saveItem ) /* stubs, type, id, prevItem, item, callback */,
    saveItemRaw: R.curry ( saveItemRaw ) /* stubs, encapsulatedItem, callback */,
    deleteItem: R.curry ( deleteItem ) /* stubs, type, id, callback */,
    getItem: R.curry ( getItem ) /* stubs, type, id, callback */,
    getItems: R.curry ( getItems )  /* stubs, type, query, callback */,
    getItemCount: R.curry ( getItemCount ) /* stubs, type, query, callback */,
    getItemIds: R.curry ( getItemIds )  /* stubs, type, query, callback */,
    hydrateItemIds: R.curry ( hydrateItemIds ) /* stubs, type, ids, callback */,
    atomicOp: R.curry ( atomicOp ) /* stubs, type, id, attributePath, command, query, callback */,
    /* depricate the below */
    hydrateIds: R.curry ( hydrateIds ) /* stubs, type, ids, callback */
};
