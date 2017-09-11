const R = require ( 'ramda' );
const uuid = require ( 'uuid' ), generateId = uuid.v4;

const queryToObj = query => {
    const redisEncode = str => {
        if ( R.type ( str ) !== 'String' ) {
            return str;
        }

        /* URIEncode everything but [/:.], as that is how redis encodes it */
        const ret = R.reduce ( ( memo, regexPair ) => {
            return memo.replace.apply ( memo, regexPair );
        }, encodeURIComponent ( str ), [
            [ /%40/g, '@' ],
            [ /%20/g, ' ' ],
            [ /%2F/g, '/' ],
            [ /%2E/g, '.' ],
            [ /%3A/g, ':' ],
            [ /\(/g, '%28' ],
            [ /\)/g, '%29' ]
        ] );

        return ret;
    };

    const obj = R.reduce ( ( obj, key ) => {
        if ( R.isNil ( query[key] ) || query[key] === '' ) {
            return obj;
        }

        if ( key.match ( /\[\d?\]$/ ) ) {
            return R.merge ( obj, R.fromPairs ( [
                [ key.replace ( /\[\d?\]/, '' ), R.concat ( obj[key.replace ( /\[\d?\]/, '' )] || [], [ redisEncode ( query[key] ) ] ) ]
            ] ) );
        }

        if ( R.type ( query[key] ) === 'Array' ) {
            return R.merge ( obj, R.fromPairs ( [
                [ key, R.concat ( R.flatten ( R.of ( obj[key] || [] ) ), R.map ( redisEncode, query[key] ) ) ]
            ] ) );
        }

        if ( R.type ( query[key] ) === 'String' ) {
            if ( query[key].match ( '~' ) ) {
                return R.merge ( obj, R.fromPairs ( [
                    [ key, R.concat ( R.flatten ( R.of ( obj[key] || [] ) ), R.map ( redisEncode, query[key].split ( '~' ) ) ) ]
                ] ) );
            }

            return R.merge ( obj, R.fromPairs ( [
                [ key, redisEncode ( query[key] ) ]
            ] ) );
        }

        return R.merge ( obj, R.fromPairs ( [
            [ key, redisEncode ( query[key] ) ]
        ] ) );
    }, {}, R.keys ( query || {} ) );

    return obj;
};

const queryObjToSortedSetNames = ( type, queryObj ) => {
    const randomTempName = () => {
        return [ 'tmp', generateId () ].join ( '-' );
    };

    const keyValueToSetName = R.curry ( ( key, value ) => [ 'items', type, key, value ].join ( '-' ) );

    return R.merge ( R.fromPairs ( R.reduce ( ( setNames, key ) => {
        return R.concat ( setNames, [ [
            randomTempName (),
            R.map ( keyValueToSetName ( key ), R.flatten ( R.of ( queryObj[key] ) ) )
        ] ] );
    }, [], R.keys ( R.omit ( [ 'before', 'after', 'count', 'lat', 'lng', 'radius', 'units' ], queryObj ) ) ) ), R.fromPairs ( [ [
        randomTempName (),
        [ [ 'items', type ].join ( '-' ) ]
    ] ] ) );
};

/*** UNIT TESTS ***/

module.exports = {
    queryToObj: queryToObj,
    queryObjToSortedSetNames: R.curry ( queryObjToSortedSetNames )
};

if ( ! module.parent ) {
    const deepEquals = require ( 'deep-equals' );
    const assert = require ( 'assert' );
    const runTest = ( desc, testFunc ) => {
        console.log ( desc );
        testFunc ();
        console.log ( 'Success' );
    };

    runTest ( 'Test queryObject utilities', () => {
        const testQuery = {
            a: 'A', b: 'B', c: [ 'C0', 'C1' ], 'd[0]': 'D0', 'd[1]': 'D1', 'd[2]': 'D2', 'e[]': 'E0', e: 'E1~E2~E3~E4', 'f[0]': 'F0', f: 'F1~F2~F3~F4',
            before: 'asdf', after: 'sdfg', count: 123
        };
        const testQueryObject = queryToObj ( testQuery );
        const expectedTestQueryObject = {
            a: 'A',
            b: 'B',
            c: [ 'C0', 'C1' ],
            d: [ 'D0', 'D1', 'D2' ],
            e: [ 'E0', 'E1', 'E2', 'E3', 'E4' ],
            f: [ 'F0', 'F1', 'F2', 'F3', 'F4' ],
            before: 'asdf',
            after: 'sdfg',
            count: 123
        };
        const sortedSetNameObject = queryObjToSortedSetNames ( 'testTypes', testQueryObject );
        const expectedSortedSetNameObject = {
            'items-testTypes-union-a-A': [ 'items-testTypes-a-A' ],
            'items-testTypes-union-b-B': [ 'items-testTypes-b-B' ],
            'items-testTypes-union-c-C0-C1': [ 'items-testTypes-c-C0', 'items-testTypes-c-C1' ],
            'items-testTypes-union-d-D0-D1-D2': [ 'items-testTypes-d-D0', 'items-testTypes-d-D1', 'items-testTypes-d-D2' ],
            'items-testTypes-union-e-E0-E1-E2-E3-E4': [ 'items-testTypes-e-E0', 'items-testTypes-e-E1', 'items-testTypes-e-E2', 'items-testTypes-e-E3', 'items-testTypes-e-E4' ],
            'items-testTypes-union-f-F0-F1-F2-F3-F4': [ 'items-testTypes-f-F0', 'items-testTypes-f-F1', 'items-testTypes-f-F2', 'items-testTypes-f-F3', 'items-testTypes-f-F4' ],
            'items-testTypes-union': [ 'items-testTypes' ]
        };

        console.log ( '- queryToObj' );

        try {
            assert ( deepEquals ( testQueryObject, expectedTestQueryObject ), `queryToObj: testQueryObject is not equal to ${JSON.stringify ( expectedTestQueryObject, null, 4 )}: ${JSON.stringify ( testQueryObject, null, 4 )}` );
        } catch ( error ) {
            console.log ( testQueryObject );
            console.error ( error );
        }

        console.log ( '- queryToObj - null' );

        assert ( deepEquals ( queryToObj ( null ), {} ), `queryToObj: null input does not produce an empty object` );

        console.log ( '- queryObjToSortedSetNames' );

        try {
            assert ( deepEquals ( sortedSetNameObject, expectedSortedSetNameObject ), `queryToObj: sortedSetNameObject is not equal to ${JSON.stringify ( expectedSortedSetNameObject, null, 4)}: ${JSON.stringify ( sortedSetNameObject, null, 4 )}` );
        } catch ( error ) {
            console.log ( sortedSetNameObject );
            throw ( error );
        }
    } );
}
