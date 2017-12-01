const H = require ( 'highland' );
const R = require ( 'ramda' );
const itemStore = require ( './lib/itemStore.js' );
const config = require ( './tests/lib/config.js' );
const valueProcessor = require ( './tests/lib/valueProcessor.js' );

const dynamoState = { state: {} };
const dynamoHandlers = {
    put: ( parms, callback ) => {
        dynamoState.state = R.assocPath ( [ parms.TableName, parms.Item.HashKey ], parms.Item, dynamoState.state );
        return callback ( null, 1 );
    },
    delete: ( parms, callback ) => {
        dynamoState.state = R.dissocPath ( [ parms.TableName, parms.Item.HashKey ], dynamoState.state );
        return callback ( null, 1 );
    },
    get: ( parms, callback ) => {
        return callback ( null, { Item: R.path ( [ parms.TableName, parms.Key.HashKey ], dynamoState.state ) } );
    }
};

const dynamoClient = R.fromPairs ( R.map ( dynamoClientMethod => [
    dynamoClientMethod,
    dynamoHandlers[dynamoClientMethod]
], [ 'put', 'delete', 'get' ] ) );

const redis = require ( 'redis' );
const { spawn } = require ( 'child_process' );
const { readdir, unlinkSync } = require ( 'fs' );
const redisProcess = spawn ( 'redis-server' );
const stdout = [];

const run = done => {
    const redisClient = redis.createClient ( { host: 'localhost', port: 6379 } );

    const stubs = {
        redis: redisClient,
        dynamo: dynamoClient
    };

    const testHelpers = {
        equals: ( what, is, shouldBe ) => {
            if ( R.type ( is ) !== R.type ( shouldBe ) ) {
                console.log ( `\u2717 ${what} (types differ: ${R.type ( is )} and ${R.type ( shouldBe )})` );
                return false;
            }

            const trimObject = O => {
                return R.fromPairs ( R.reduce ( ( o, pair ) => {
                    return typeof pair[1] === 'undefined' ? o : [ ...o, pair ];
                }, [], R.toPairs ( O ) ) );
            };

            if ( R.type ( is ) === 'Object' ) {
                if ( R.difference ( R.keys ( trimObject ( is ) ), R.keys ( trimObject ( shouldBe ) ) ).length ) {
                    console.log ( `\u2717 ${what} (objects have different keys, should be ${R.keys(shouldBe)}, is ${R.keys(is)})` );
                    return false;
                }

                return R.reduce ( ( result, key ) => {
                    return result && testHelpers.equals ( `${what}.${key}`, is[key], shouldBe[key] );
                }, true, R.keys ( trimObject ( is ) ) );
            }

            if ( R.equals ( is, shouldBe ) ) {
                console.log ( `\u2713 ${what}` );
                return true;
            }

            console.log ( `\u2717 ${what} (expected ${shouldBe}, got ${is})` );
            return false;
        }
    };

    const runTest = ( testModule, callback ) => {
        const test = {
            config: config,
            tests: R.map ( test => {
                return { validate: ( testHelpers, response, callback ) => {
                    var rc = true;
                    const genericMethodTests = require ( './tests/lib/genericMethodTests.js' )( { config, valueProcessor, test, testHelpers, response } );

                    if ( genericMethodTests[test.method] ) {
                        rc = rc && genericMethodTests[test.method] ( test.args () );
                    }

                    if ( test.validateMore ) {
                        rc = rc && test.validateMore ( testHelpers, response );
                    }
                    return callback ( rc === false ? 'AHHH CRAP!!!' : null, rc === true ? `${test.method} DONE` : null );
                }, ...test };
            }, require ( testModule ) )
        };

        itemStore.loadConfig ( test.config );

        return H ( test.tests )
            .flatMap ( tst => {
                return H.wrapCallback ( itemStore[tst.method] )( stubs, ...tst.args () )
                    .flatMap ( response => {
                        return H.wrapCallback ( tst.validate )( testHelpers, response );
                    } );
            } )
            .stopOnError ( error => {
                return console.error ( `ERROR: ${JSON.stringify ( error, null, 4 )}` );
            } )
            .doto ( console.log )
            .collect ()
            .toCallback ( callback );
    };

    const mainStream = process.argv[2] ?
        H ( [ process.argv[2] ] ).map ( file => R.last ( R.split ( '/', file ) ) ) :
        H.wrapCallback ( readdir )( './tests' ).sequence ();

    return mainStream
        .filter ( file => file.match ( /\.js$/ ) )
        .doto ( file => { console.log ( `START ${file.replace ( '.js', '' ).replace ( /_/g, ' ' )}` ); } )
        .map ( file => [ '.', 'tests', file ].join ( '/' ) )
        .flatMap ( H.wrapCallback ( runTest ) )
        .errors ( console.error )
        .doto ( i => { console.log ( "COMPLETE\n" ); } )
        .doto ( () => {
            redisClient.flushdb ();
        } )
        .collect ()
        .each ( () => {
            redisClient.quit ();
            done ();
        } );
};

const checkOnce = [];

redisProcess.stdout.on ( 'data', data => {
    const dataString = data.toString ();
    stdout.push ( dataString );
    const fullString = stdout.join ( '' );

    if ( checkOnce.length === 0 && fullString.match ( 'The server is now ready to accept connections on port 6379' ) ) {
        checkOnce.push ( '1' );
        run ( () => {
            redisProcess.kill ();
        } );
    }
} );

redisProcess.on ( 'close', ( code, signal ) => {
    console.log ( `redis process terminated: code ${code} signal ${signal}` );
    unlinkSync ( 'dump.rdb' );
} );
