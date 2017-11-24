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

    const supportedMethods = [
        'zrange', 'zscore', 'zinterstore', 'zunionstore', 'zadd', 'zrevrangebyscore', 'zrem',
        'smembers', 'sadd',
        'lrange', 'rpush',
        'georadius', 'geoadd',
        'del'
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

const getTypePaths = ( typeType, type ) => {
    const config = R.head ( configs );

    const commands = {
        custom: 'reject',
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
    const mergedItem = R.merge ( R.fromPairs ( R.map ( pathObj => {
        return [ R.last ( pathObj.path ), R.path ( pathObj.path, prevItem ) ];
    }, automaticMetadataPaths ) ), item );

    const metadataPaths = getTypePaths ( 'default', type );

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

    const createSetOrList = R.curry ( ( command, key, type, value ) => {
        if ( R.type ( value ) !== 'Array' ) {
            return H ( [ `an attribute of type ${type} has to be an Array, not a ${R.type(value)}` ] );
        }

        return H.wrapCallback ( R.bind ( redisClient.del, redisClient ) )( [ key ] )
            .flatMap ( () => {
                return H.wrapCallback ( R.bind ( redisClient[command], redisClient ) )( [ key, ...value ] );
            } );
    } );

    const customTypeSaveHandlers = {
        zset: R.curry ( ( key, type, value ) => {
            if ( R.type ( value ) !== 'Array' ) {
                return H ( [ `an attribute of type ${type} has to be an Array, not a ${R.type(value)}` ] );
            }

            if ( R.find ( valueItem => {
                return R.type ( valueItem ) !== 'Object';
            }, value ) ) {
                return H ( [ `non-object value in attribute of type ${type}` ] );
            }

            if ( R.find ( valueItem => {
                return ! R.has ( 'score', valueItem ) || ! R.has ( 'member', valueItem );
            }, value ) ) {
                return H ( [ `each value in attribute of type ${type} should have a 'score' and a 'member' attribute` ] );
            }

            return H.wrapCallback ( R.bind ( redisClient.del, redisClient ) )( [ key ] )
                .flatMap ( () => {
                    return H.wrapCallback ( R.bind ( redisClient.zadd, redisClient ) )( R.reduce ( ( parms, { score, member } ) => {
                        return [ ...parms, score, member ];
                    }, [ key ], value ) );
                } );
        } ),
        set: createSetOrList ( 'sadd' ),
        list: createSetOrList ( 'rpush' ),
        default: R.curry ( ( key, type, value ) => {
            return H ( [ `no handler for type '${type}'` ] );
        } )
    };

    const customTypeGetHandlers = {
        zset: R.curry ( ( key, type ) => {
            return H.wrapCallback ( R.bind ( redisClient.zrange, redisClient ) )( [ key, 0, -1, 'WITHSCORES' ] )
                .map ( R.splitEvery ( 2 ) )
                .sequence ()
                .map ( ( [ member, score ] ) => {
                    return { member, score };
                } )
                .collect ();
        } ),
        set: R.curry ( ( key, type ) => {
            return H.wrapCallback ( R.bind ( redisClient.smembers, redisClient ) )( [ key ] );
        } ),
        list: R.curry ( ( key, type ) => {
            return H.wrapCallback ( R.bind ( redisClient.lrange, redisClient ) )( [ key, 0, -1 ] );
        } ),
        default: R.curry ( ( key, type ) => {
            return H ( [ `no handler for type '${type}'` ] );
        } )
    };

    return {
        deleteCustomTypes: ( encapsulatedItem, callback ) => {
            const customTypePaths = getTypePaths ( 'custom', encapsulatedItem.type );

            return H ( R.map ( customTypePath => {
                const key = [ encapsulatedItem.id, ...customTypePath.path ].join ( '-' );
                return H.wrapCallback ( R.bind ( redisClient.del, redisClient ) )( [ key ] );
            }, customTypePaths ) )
                .parallel ( 10 )
                .collect ()
                .map ( R.zip ( R.map ( customTypePath => R.last ( customTypePath.path ), customTypePaths ) ) )
                .map ( R.fromPairs )
                .toCallback ( callback );
        },
        saveCustomTypes: ( encapsulatedItem, callback ) => {
            const customTypePaths = getTypePaths ( 'custom', encapsulatedItem.type );

            return H ( R.map ( customTypePath => {
                const key = [ encapsulatedItem.id, ...customTypePath.path ].join ( '-' );
                const customTypeHandler = ( customTypeSaveHandlers[customTypePath.type] || customTypeSaveHandlers.default )( key, customTypePath.type );
                return customTypeHandler ( R.path ( customTypePath.path, encapsulatedItem.item ) );
            }, customTypePaths ) )
                .parallel ( 10 )
                .collect ()
                .map ( R.zip ( R.map ( customTypePath => R.last ( customTypePath.path ), customTypePaths ) ) )
                .map ( R.fromPairs )
                .toCallback ( callback );
        },
        getCustomTypes: ( dehydratedItem, callback ) => {
            const customTypePaths = getTypePaths ( 'custom', dehydratedItem.type );

            return H ( R.map ( customTypePath => {
                const key = [ dehydratedItem.id, ...customTypePath.path ].join ( '-' );
                return ( customTypeGetHandlers[customTypePath.type] || customTypeGetHandlers.default )( key, customTypePath.type );
            }, customTypePaths ) )
                .parallel ( 10 )
                .collect ()
                .map ( R.zip ( R.map ( customTypePath => R.last ( customTypePath.path ), customTypePaths ) ) )
                .map ( R.fromPairs )
                .toCallback ( callback );
        },
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
    const strippedEncapsulatedItem = R.reduce ( ( encapsulatedItem, customMetadataPath ) => {
        return R.dissocPath ( [ 'item', ...customMetadataPath.path ], encapsulatedItem );
    }, encapsulatedItem, getTypePaths ( 'custom', encapsulatedItem.type ) );

    return H ( [
        H.wrapCallback ( listItem ( clients.redis ).saveCustomTypes )( encapsulatedItem ),
        H.wrapCallback ( listItem ( clients.redis ).list )( strippedEncapsulatedItem ),
        H.wrapCallback ( listItem ( clients.redis ).geoList )( strippedEncapsulatedItem ),
        H.wrapCallback ( storeItem ( clients.dynamo ).store )( strippedEncapsulatedItem )
    ] )
        .parallel ( 3 )
        .collect ()
        .map ( R.zip ( [ 'customTypes', 'list', 'geolist', 'store' ] ) )
        .map ( R.fromPairs )
        .map ( R.merge ( {
            id: encapsulatedItem.id,
            iid: encapsulatedItem.iid,
            type: encapsulatedItem.type
        } ) )
        .toCallback ( callback );
};

const saveItem = ( stubs, type, id, prevItem, item, callback ) => {
    return saveItemRaw ( stubs, encapsulateItem ( type, id, prevItem, item ), callback );
};

const deleteItem = ( stubs, type, id, callback ) => {
    const clients = getClients ( stubs, 'deleteItem' );

    return H.wrapCallback ( getItem )( stubs, type, id )
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
                H.wrapCallback ( listItem ( clients.redis ).deleteCustomTypes )( encapsulatedItem ),
                H.wrapCallback ( listItem ( clients.redis ).deList )( encapsulatedItem ),
                H.wrapCallback ( listItem ( clients.redis ).geoDeList )( encapsulatedItem ),
                H.wrapCallback ( listItem ( clients.redis ).remove )( encapsulatedItem )
            ] );
        } )
        .parallel ( 3 )
        .collect ()
        .map ( R.zip ( [ 'customTypes', 'list', 'geolist', 'removedlist' ] ) )
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
                .flatMap ( encapsulatedItem => H ( ( push, next ) => {
                    if ( R.isNil ( encapsulatedItem.type ) ) {
                        push ( { code: 404, message: `There is no such item` } );
                        return push ( null, H.nil );
                    }

                    if ( encapsulatedItem.type !== type ) {
                        push ( { code: 404, message: `We can find such an item, but of type '${encapsulatedItem.type}', not '${type}'` } );
                        return push ( null, H.nil );
                    }

                    if ( R.isNil ( score ) ) {
                        push ( { code: 404, message: `There was such an item, but it's been deleted` } );
                        return push ( null, H.nil );
                    }

                    if ( encapsulatedItem.type !== type ) {
                        push ( { code: 404, message: `There is such an item, but not of type ${type}; its type is ${encapsulatedItem.type}` } );
                        return push ( null, H.nil );
                    }
                    push ( null, encapsulatedItem );
                    return push ( null, H.nil );
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
                    .reject ( R.isNil )
                    .take ( count + offset )
                    .collect ()
                    .map ( R.slice ( offset, count + offset ) );
            }

            return H ( [ R.map ( id => {
                return R.concat ( id, [ undefined ] );
            }, ids ) ] );
        } )
        .sequence ()
        .map ( idTriplet => ( {
            id: idTriplet[0],
            lastModifiedTime: idTriplet[1],
            distance: idTriplet[2],
            type
        } ) )
        .flatMap ( dehydratedItem => {
            return H.wrapCallback ( listItem ( clients.redis ).getCustomTypes )( dehydratedItem )
                .map ( customTypesObj => ( { ...dehydratedItem, item: customTypesObj } ) );
        } )
        .collect ();
};

const hydrateIdsWithDistance = R.curry ( ( clients, type, dehydratedItems ) => {
    return H ( R.map ( dehydratedItem => {
        return H.wrapCallback ( getItem )( clients, type, dehydratedItem.id )
            .map ( R.merge ( dehydratedItem.distance ? {
                distance: parseFloat ( dehydratedItem.distance )
            } : {} ) )
            .map ( item => R.merge ( item, {
                item: R.merge ( item.item, dehydratedItem.item )
            } ) )
            .errors ( ( error, push ) => {
                if ( error.code === 404 ) {
                    return push ( null, null );
                }
                return push ( error );
            } );
    }, dehydratedItems ) )
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
    getItemCount: R.curry ( getItemCount ) /* stubs, type, query, callback */,
    getItemIds: R.curry ( getItemIds )  /* stubs, type, query, callback */,
    hydrateIds: R.curry ( hydrateIds ) /* stubs, type, ids, callback */
};
