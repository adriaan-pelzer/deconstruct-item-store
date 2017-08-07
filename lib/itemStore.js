const R = require ( 'ramda' );
const H = require ( 'highland' );
const aws = require ( 'aws-sdk' );
const redis = require ( 'redis' );
const uuid = require ( 'uuid' ), generateId = uuid.v4;
const md5 = require ( 'md5' );

const configs = [];

const log = R.compose ( console.log, R.partialRight ( JSON.stringify, [ null, 4 ] ) );

const queryObject = require ( './queryObject.js' );

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

    return R.merge ( client, {
        zinterstore: ( parms, callback ) => {
            return client.zinterstore ( parms, wrapCallback ( callback ) );
        },
        zunionstore: ( parms, callback ) => {
            return client.zunionstore ( parms, wrapCallback ( callback ) );
        },
        zadd: ( parms, callback ) => {
            return client.zadd ( parms, wrapCallback ( callback ) );
        },
        zrevrangebyscore: ( parms, callback ) => {
            return client.zrevrangebyscore ( parms, wrapCallback ( callback ) );
        },
        georadius: ( parms, callback ) => {
            return client.georadius ( parms, wrapCallback ( callback ) );
        },
        geoadd: ( parms, callback ) => {
            return client.geoadd ( parms, wrapCallback ( callback ) );
        },
        zrem: ( parms, callback ) => {
            return client.zrem ( parms, wrapCallback ( callback ) );
        },
        del: ( parms, callback ) => {
            return client.del ( parms, wrapCallback ( callback ) );
        },
        quit: () => {
            return client.quit ();
        }
    } );
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

const encapsulateItem = ( type, id, prevItem, item ) => {
    const time = new Date ().valueOf ();

    const pathAttrTxFuncs = {
        boolean: value => {
            return !!value;
        },
        string: value => {
            return value.toString ( 'utf8' );
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
            return value.toString ( 'utf8' ).toLowerCase ();
        },
        uriLastPathComp: value => {
            return R.last ( value.split ( '/' ) );
        }
    };

    return R.reduce ( R.merge, {}, [
        R.reduce ( ( encapsulatedItem, pathObj ) => {
            const path = pathObj.path, pathAttrs = pathObj.attrs || [];

            if ( R.isNil ( R.path ( path, item ) ) ) {
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
            }, R.path ( path, item ), pathAttrs ) ] ] ) );
        }, {
            id: id,
            publishedTime: R.path ( [ 'publishedTime' ], prevItem ) || time,
            lastModifiedTime: time,
            type: type
        }, R.path ( [ 'typeConfigs', type, 'metadataPaths' ], R.head ( configs ) ) || [] ),
        { item: item },
        prevItem ? {
            previousId: prevItem.id
        } : {}
    ] );
};

const listItem = redisClient => {
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
        list: ( encapsulatedItem, callback ) => {
            return listItemHelper ( 'zadd', encapsulatedItem, [ encapsulatedItem.lastModifiedTime, encapsulatedItem.id ], callback );
        },
        deList: ( encapsulatedItem, callback ) => {
            return listItemHelper ( 'zrem', encapsulatedItem, [ encapsulatedItem.id ], callback );
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

    return H ( [
        H.wrapCallback ( listItem ( clients.redis ).list )( encapsulatedItem ),
        H.wrapCallback ( listItem ( clients.redis ).geoList )( encapsulatedItem ),
        H.wrapCallback ( storeItem ( clients.dynamo ).store )( encapsulatedItem )
    ] )
        .parallel ( 3 )
        .collect ()
        .map ( R.zip ( [ 'list', 'geolist', 'store' ] ) )
        .map ( R.fromPairs )
        .map ( R.merge ( { id: id, type: type } ) )
        .toCallback ( callback );
};

const saveItem = ( stubs, type, id, prevItem, item, callback ) => {
    return saveItemRaw ( stubs, encapsulateItem ( type, id, prevItem, item ), callback );
};

const deleteItem = ( stubs, type, id, callback ) => {
    const clients = getClients ( stubs, 'deleteItem' );

    return H.wrapCallback ( storeItem ( clients.dynamo ).get )( id )
        .pluck ( 'Item' )
        .map ( R.omit ( [ 'HashKey' ] ) )
        .flatMap ( encapsulatedItem => H ( ( push, next ) => {
            if ( encapsulatedItem.type !== type ) {
                push ( { code: 404, message: `There is such an item, but not of type ${type}; its type is ${encapsulatedItem.type}` } );
                return push ( null, H.nil );
            }
            push ( null, encapsulatedItem );
            return push ( null, H.nil );
        } ) )
        .flatMap ( encapsulatedItem => {
            return H ( [
                H.wrapCallback ( listItem ( clients.redis ).deList )( encapsulatedItem ),
                H.wrapCallback ( listItem ( clients.redis ).geoDeList )( encapsulatedItem ),
                H.wrapCallback ( listItem ( clients.redis ).remove )( encapsulatedItem )
            ] );
        } )
        .parallel ( 3 )
        .collect ()
        .map ( R.zip ( [ 'list', 'geolist', 'removedlist' ] ) )
        .map ( R.fromPairs )
        .toCallback ( callback );
};

const getItem = ( stubs, type, id, callback ) => {
    const clients = getClients ( stubs, 'getItem' );

    return H.wrapCallback ( storeItem ( clients.dynamo ).get )( id )
        .pluck ( 'Item' )
        .map ( R.omit ( [ 'HashKey' ] ) )
        .flatMap ( encapsulatedItem => H ( ( push, next ) => {
            if ( encapsulatedItem.type !== type ) {
                push ( { code: 404, message: `There is such an item, but not of type ${type}; its type is ${encapsulatedItem.type}` } );
                return push ( null, H.nil );
            }
            push ( null, encapsulatedItem );
            return push ( null, H.nil );
        } ) )
        .toCallback ( callback );
};

const getItemsDehydratedWithDistance = ( clients, type, query ) => {
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

    const valiDate = ( def, dateString ) => {
        const parsedDate = new Date ( ( R.type ( dateString ) === 'String' && ( dateString.match ( /\d{10}/ ) || dateString.match ( /\d{13}/ ) ) ) ? parseInt ( dateString, 10 ) : dateString );

        if ( parsedDate.toString () === 'Invalid Date' ) {
            return def;
        }

        return parsedDate.valueOf ();
    };

    const isGeoQuery = R.path ( [ 'lat' ], query ) && R.path ( [ 'lng' ], query ) && R.path ( [ 'radius' ], query );
    const count = Math.min ( parseInt ( R.path ( [ 'count' ], query ) || 100, 10 ), 3000 );

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
                .map ( interResult => {
                    return R.concat ( unionResults, [ interResult ] );
                } );
        } )
        .flatMap ( results => {
            return redisStream ( 'zrevrangebyscore' )( R.concat ( [
                R.last ( results )[0],
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
                    return H ( R.map ( key => {
                        return redisStream ( 'del' )( [ key ] );
                    }, R.filter ( key => key.match ( /^tmp-/ ), R.map ( R.head, results ) ) ) )
                        .parallel ( 10 )
                        .collect ()
                        .map ( R.always ( ids ) );
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
                    .reject ( R.isNil )
                    .take ( count )
                    .collect ();
            }

            return H ( [ R.map ( id => {
                return R.concat ( id, [ undefined ] );
            }, ids ) ] );
        } );
};

const hydrateIdsWithDistance = R.curry ( ( clients, type, idPairs ) => {
    return H ( R.map ( idPair => {
        const id = R.head ( idPair );
        const distance = R.isNil ( R.last ( idPair ) ) ? undefined : parseFloat ( R.last ( idPair ) );

        return H.wrapCallback ( getItem )( clients, type, id )
            .map ( R.merge ( {
                distance: distance
            } ) )
            .errors ( ( error, push ) => {
                if ( error.code === 404 ) {
                    return push ( null, null );
                }
                return push ( error );
            } );
    }, idPairs ) )
        .parallel ( 100 )
        .compact ()
        .collect ();
} );

const hydrateIds = ( stubs, type, ids, callback ) => {
    const clients = getClients ( stubs, 'hydrateIds' );

    return hydrateIdsWithDistance ( clients, type, R.zip ( ids, R.times ( R.always ( undefined ), ids.length ) ) )
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
        .map ( R.map ( idTriplet => ( {
            id: idTriplet[0],
            lastModifiedTime: idTriplet[1],
            distance: idTriplet[2]
        } ) ) )
        .toCallback ( callback );
};

const loadConfig = config => {
    configs.push ( config );
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
    getItemIds: R.curry ( getItemIds )  /* stubs, type, query, callback */,
    hydrateIds: R.curry ( hydrateIds ) /* stubs, type, ids, callback */
};
