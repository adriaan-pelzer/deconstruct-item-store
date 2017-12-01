const R = require ( 'ramda' );
const itemStore = require ( '../lib/itemStore.js' );
const config = require ( './lib/config.js' );
const valueProcessor = require ( './lib/valueProcessor.js' );

const context = {
    items: {
        centralLondon: {
            latlng: {
                lat: 51.496133,
                lng: -0.137329
            }
        },
        canaryWharf: {
            latlng: {
                lat: 51.495559,
                lng: -0.14763
            }
        },
        northWeald: {
            latlng: {
                lat: 51.722685,
                lng: 0.150805
            }
        },
        manchester: {
            latlng: {
                lat: 53.475481,
                lng: -2.243958
            }
        }
    }
};

module.exports = R.reduce ( R.concat, [], [
    R.map ( placeName => ( {
        method: 'saveItem',
        args: R.memoizeWith ( R.always ( 'a' ), () => ( [ 'geoItem', itemStore.generateId (), null, { placeName, ...context.items[placeName].latlng } ] ) ),
        validateMore: ( testHelpers, response ) => {
            context.items[placeName].id = response.id;
            return true;
        }
    } ), R.keys ( context.items ) ),
    R.map ( placeName => ( {
        method: 'getItem',
        args: R.memoizeWith ( R.always ( 'a' ), () => ( [ 'geoItem', context.items[placeName].id ] ) ),
        validateMore: ( testHelpers, response ) => {
            var rc = true;
            rc = rc && testHelpers.equals ( `getItem geoItem response.item`, response.item, { placeName, ...context.items[placeName].latlng } );
            context.items[placeName].item = response;
            return rc;
        }
    } ), R.keys ( context.items ) ),
    [
        {
            method: 'getItems',
            args: R.memoizeWith ( R.always ( 'a' ), () => ( [ 'geoItem', {} ] ) ),
            validateMore: ( testHelpers, response ) => {
                var rc = true;
                rc = rc && testHelpers.equals ( `getItems 1 geoItem response.length`, response.length, 4 );
                response.forEach ( ( item, idx ) => {
                    const savedItem = R.find ( i => i.item.placeName === item.item.placeName, R.map ( R.prop ( 'item' ), R.values ( context.items ) ) );
                    rc = rc && testHelpers.equals ( `getItems geoItem response[${idx}].item`, item, savedItem );
                } );
                return rc;
            }
        },
        {
            method: 'getItemIds',
            args: R.memoizeWith ( R.always ( 'a' ), () => ( [ 'geoItem', {} ] ) ),
            validateMore: ( testHelpers, response ) => {
                var rc = true;
                rc = rc && testHelpers.equals ( `getItemIds 1 geoItem response.length`, response.length, 4 );
                response.forEach ( ( item, idx ) => {
                    const savedItem = R.find ( i => i.id === item.id, R.map ( R.prop ( 'item' ), R.values ( context.items ) ) );
                    rc = rc && testHelpers.equals ( `getItemIds geoItem response[${idx}]`, R.omit ( [ 'distance' ], item ), { ...R.pick ( [ 'id', 'iid', 'lastModifiedTime', 'type' ], savedItem ), item: {} } );
                } );
                return rc;
            }
        },
        {
            method: 'getItemCount',
            args: R.memoizeWith ( R.always ( 'a' ), () => ( [ 'geoItem', {} ] ) ),
            validateMore: ( testHelpers, response ) => {
                var rc = true;
                rc = rc && testHelpers.equals ( `getItems geoItem response`, response, 4 );
                return rc;
            }
        },
        {
            method: 'getItems',
            args: R.memoizeWith ( R.always ( 'a' ), () => ( [ 'geoItem', { ...context.items.centralLondon.latlng, radius: 10, units: 'm' } ] ) ),
            validateMore: ( testHelpers, response ) => {
                var rc = true;
                rc = rc && testHelpers.equals ( `getItems geoItem response.length`, response.length, 1 );
                response.forEach ( ( item, idx ) => {
                    const savedItem = R.find ( i => i.item.placeName === item.item.placeName, R.map ( R.prop ( 'item' ), R.values ( context.items ) ) );
                    rc = rc && testHelpers.equals ( `getItems geoItem response[${idx}].item`, R.omit ( [ 'distance' ], item ), savedItem );
                } );
                rc = rc && testHelpers.equals ( `getItems geoItem response[].item.placeName`, R.map ( R.path ( [ 'item', 'placeName' ] ), response ), [
                    'centralLondon'
                ] );
                return rc;
            }
        },
        {
            method: 'getItemIds',
            args: R.memoizeWith ( R.always ( 'a' ), () => ( [ 'geoItem', { ...context.items.centralLondon.latlng, radius: 10, units: 'm' } ] ) ),
            validateMore: ( testHelpers, response ) => {
                var rc = true;
                rc = rc && testHelpers.equals ( `getItemIds geoItem response.length`, response.length, 1 );
                response.forEach ( ( item, idx ) => {
                    const savedItem = R.find ( i => i.id === item.id, R.map ( R.prop ( 'item' ), R.values ( context.items ) ) );
                    rc = rc && testHelpers.equals ( `getItemIds geoItem response[${idx}]`, R.omit ( [ 'distance' ], item ), { ...R.pick ( [ 'id', 'iid', 'lastModifiedTime', 'type' ], savedItem ), item: {} } );
                } );
                return rc;
            }
        },
        {
            method: 'getItemCount',
            args: R.memoizeWith ( R.always ( 'a' ), () => ( [ 'geoItem', { ...context.items.centralLondon.latlng, radius: 10, units: 'm' } ] ) ),
            validateMore: ( testHelpers, response ) => {
                var rc = true;
                rc = rc && testHelpers.equals ( `getItems geoItem response`, response, 1 );
                return rc;
            }
        },
        {
            method: 'getItems',
            args: R.memoizeWith ( R.always ( 'a' ), () => ( [ 'geoItem', { ...context.items.centralLondon.latlng, radius: 1, units: 'mi' } ] ) ),
            validateMore: ( testHelpers, response ) => {
                var rc = true;
                rc = rc && testHelpers.equals ( `getItems geoItem response.length`, response.length, 2 );
                response.forEach ( ( item, idx ) => {
                    const savedItem = R.find ( i => i.item.placeName === item.item.placeName, R.map ( R.prop ( 'item' ), R.values ( context.items ) ) );
                    rc = rc && testHelpers.equals ( `getItems geoItem response[${idx}].item`, R.omit ( [ 'distance' ], item ), savedItem );
                } );
                rc = rc && testHelpers.equals ( `getItems geoItem response[].item.placeName`, R.map ( R.path ( [ 'item', 'placeName' ] ), response ), [
                    'centralLondon',
                    'canaryWharf'
                ] );
                rc = rc && testHelpers.equals ( `getItems geoItem response[].distance`, R.map ( R.path ( [ 'distance' ] ), response ), [
                    0.0001,
                    0.4451
                ] );
                return rc;
            }
        },
        {
            method: 'getItemIds',
            args: R.memoizeWith ( R.always ( 'a' ), () => ( [ 'geoItem', { ...context.items.centralLondon.latlng, radius: 1, units: 'mi' } ] ) ),
            validateMore: ( testHelpers, response ) => {
                var rc = true;
                rc = rc && testHelpers.equals ( `getItemIds geoItem response.length`, response.length, 2 );
                response.forEach ( ( item, idx ) => {
                    const savedItem = R.find ( i => i.id === item.id, R.map ( R.prop ( 'item' ), R.values ( context.items ) ) );
                    rc = rc && testHelpers.equals ( `getItemIds geoItem response[${idx}]`, R.omit ( [ 'distance' ], item ), { ...R.pick ( [ 'id', 'iid', 'lastModifiedTime', 'type' ], savedItem ), item: {} } );
                } );
                rc = rc && testHelpers.equals ( `getItems geoItem response[].distance`, R.map ( R.path ( [ 'distance' ] ), response ), [
                    0.0001,
                    0.4451
                ] );
                return rc;
            }
        },
        {
            method: 'getItemCount',
            args: R.memoizeWith ( R.always ( 'a' ), () => ( [ 'geoItem', { ...context.items.centralLondon.latlng, radius: 1, units: 'mi' } ] ) ),
            validateMore: ( testHelpers, response ) => {
                var rc = true;
                rc = rc && testHelpers.equals ( `getItems geoItem response`, response, 2 );
                return rc;
            }
        },
        {
            method: 'getItems',
            args: R.memoizeWith ( R.always ( 'a' ), () => ( [ 'geoItem', { ...context.items.centralLondon.latlng, radius: 1 } ] ) ),
            validateMore: ( testHelpers, response ) => {
                var rc = true;
                rc = rc && testHelpers.equals ( `getItems geoItem response.length`, response.length, 2 );
                response.forEach ( ( item, idx ) => {
                    const savedItem = R.find ( i => i.item.placeName === item.item.placeName, R.map ( R.prop ( 'item' ), R.values ( context.items ) ) );
                    rc = rc && testHelpers.equals ( `getItems geoItem response[${idx}].item`, R.omit ( [ 'distance' ], item ), savedItem );
                } );
                rc = rc && testHelpers.equals ( `getItems geoItem response[].item.placeName`, R.map ( R.path ( [ 'item', 'placeName' ] ), response ), [
                    'centralLondon',
                    'canaryWharf'
                ] );
                rc = rc && testHelpers.equals ( `getItems geoItem response[].distance`, R.map ( R.path ( [ 'distance' ] ), response ), [
                    0.0001,
                    0.4451
                ] );
                return rc;
            }
        },
        {
            method: 'getItemIds',
            args: R.memoizeWith ( R.always ( 'a' ), () => ( [ 'geoItem', { ...context.items.centralLondon.latlng, radius: 1 } ] ) ),
            validateMore: ( testHelpers, response ) => {
                var rc = true;
                rc = rc && testHelpers.equals ( `getItemIds geoItem response.length`, response.length, 2 );
                response.forEach ( ( item, idx ) => {
                    const savedItem = R.find ( i => i.id === item.id, R.map ( R.prop ( 'item' ), R.values ( context.items ) ) );
                    rc = rc && testHelpers.equals ( `getItemIds geoItem response[${idx}]`, R.omit ( [ 'distance' ], item ), { ...R.pick ( [ 'id', 'iid', 'lastModifiedTime', 'type' ], savedItem ), item: {} } );
                } );
                rc = rc && testHelpers.equals ( `getItems geoItem response[].distance`, R.map ( R.path ( [ 'distance' ] ), response ), [
                    0.0001,
                    0.4451
                ] );
                return rc;
            }
        },
        {
            method: 'getItemCount',
            args: R.memoizeWith ( R.always ( 'a' ), () => ( [ 'geoItem', { ...context.items.centralLondon.latlng, radius: 1 } ] ) ),
            validateMore: ( testHelpers, response ) => {
                var rc = true;
                rc = rc && testHelpers.equals ( `getItems geoItem response`, response, 2 );
                return rc;
            }
        },
        {
            method: 'getItems',
            args: R.memoizeWith ( R.always ( 'a' ), () => ( [ 'geoItem', { ...context.items.centralLondon.latlng, radius: 10, units: 'km' } ] ) ),
            validateMore: ( testHelpers, response ) => {
                var rc = true;
                rc = rc && testHelpers.equals ( `getItems geoItem response.length`, response.length, 2 );
                response.forEach ( ( item, idx ) => {
                    const savedItem = R.find ( i => i.item.placeName === item.item.placeName, R.map ( R.prop ( 'item' ), R.values ( context.items ) ) );
                    rc = rc && testHelpers.equals ( `getItems geoItem response[${idx}].item`, R.omit ( [ 'distance' ], item ), savedItem );
                } );
                rc = rc && testHelpers.equals ( `getItems geoItem response[].item.placeName`, R.map ( R.path ( [ 'item', 'placeName' ] ), response ), [
                    'centralLondon',
                    'canaryWharf'
                ] );
                rc = rc && testHelpers.equals ( `getItems geoItem response[].distance`, R.map ( R.path ( [ 'distance' ] ), response ), [
                    0.0002,
                    0.7163
                ] );
                return rc;
            }
        },
        {
            method: 'getItemIds',
            args: R.memoizeWith ( R.always ( 'a' ), () => ( [ 'geoItem', { ...context.items.centralLondon.latlng, radius: 10, units: 'km' } ] ) ),
            validateMore: ( testHelpers, response ) => {
                var rc = true;
                rc = rc && testHelpers.equals ( `getItemIds geoItem response.length`, response.length, 2 );
                response.forEach ( ( item, idx ) => {
                    const savedItem = R.find ( i => i.id === item.id, R.map ( R.prop ( 'item' ), R.values ( context.items ) ) );
                    rc = rc && testHelpers.equals ( `getItemIds geoItem response[${idx}]`, R.omit ( [ 'distance' ], item ), { ...R.pick ( [ 'id', 'iid', 'lastModifiedTime', 'type' ], savedItem ), item: {} } );
                } );
                rc = rc && testHelpers.equals ( `getItems geoItem response[].distance`, R.map ( R.path ( [ 'distance' ] ), response ), [
                    0.0002,
                    0.7163
                ] );
                return rc;
            }
        },
        {
            method: 'getItemCount',
            args: R.memoizeWith ( R.always ( 'a' ), () => ( [ 'geoItem', { ...context.items.centralLondon.latlng, radius: 10, units: 'km' } ] ) ),
            validateMore: ( testHelpers, response ) => {
                var rc = true;
                rc = rc && testHelpers.equals ( `getItems geoItem response`, response, 2 );
                return rc;
            }
        },
        {
            method: 'getItems',
            args: R.memoizeWith ( R.always ( 'a' ), () => ( [ 'geoItem', { ...context.items.centralLondon.latlng, radius: 20 } ] ) ),
            validateMore: ( testHelpers, response ) => {
                var rc = true;
                rc = rc && testHelpers.equals ( `getItems geoItem response.length`, response.length, 3 );
                response.forEach ( ( item, idx ) => {
                    const savedItem = R.find ( i => i.item.placeName === item.item.placeName, R.map ( R.prop ( 'item' ), R.values ( context.items ) ) );
                    rc = rc && testHelpers.equals ( `getItems geoItem response[${idx}].item`, R.omit ( [ 'distance' ], item ), savedItem );
                } );
                rc = rc && testHelpers.equals ( `getItems geoItem response[].item.placeName`, R.map ( R.path ( [ 'item', 'placeName' ] ), response ), [
                    'centralLondon',
                    'canaryWharf',
                    'northWeald'
                ] );
                rc = rc && testHelpers.equals ( `getItems geoItem response[].distance`, R.map ( R.path ( [ 'distance' ] ), response ), [
                    0.0001,
                    0.4451,
                    19.9525
                ] );
                return rc;
            }
        },
        {
            method: 'getItemIds',
            args: R.memoizeWith ( R.always ( 'a' ), () => ( [ 'geoItem', { ...context.items.centralLondon.latlng, radius: 20 } ] ) ),
            validateMore: ( testHelpers, response ) => {
                var rc = true;
                rc = rc && testHelpers.equals ( `getItemIds geoItem response.length`, response.length, 3 );
                response.forEach ( ( item, idx ) => {
                    const savedItem = R.find ( i => i.id === item.id, R.map ( R.prop ( 'item' ), R.values ( context.items ) ) );
                    rc = rc && testHelpers.equals ( `getItemIds geoItem response[${idx}]`, R.omit ( [ 'distance' ], item ), { ...R.pick ( [ 'id', 'iid', 'lastModifiedTime', 'type' ], savedItem ), item: {} } );
                } );
                rc = rc && testHelpers.equals ( `getItems geoItem response[].distance`, R.map ( R.path ( [ 'distance' ] ), response ), [
                    0.0001,
                    0.4451,
                    19.9525
                ] );
                return rc;
            }
        },
        {
            method: 'getItemCount',
            args: R.memoizeWith ( R.always ( 'a' ), () => ( [ 'geoItem', { ...context.items.centralLondon.latlng, radius: 20 } ] ) ),
            validateMore: ( testHelpers, response ) => {
                var rc = true;
                rc = rc && testHelpers.equals ( `getItems geoItem response`, response, 3 );
                return rc;
            }
        },
        {
            method: 'getItems',
            args: R.memoizeWith ( R.always ( 'a' ), () => ( [ 'geoItem', { ...context.items.centralLondon.latlng, radius: 164 } ] ) ),
            validateMore: ( testHelpers, response ) => {
                var rc = true;
                rc = rc && testHelpers.equals ( `getItems geoItem response.length`, response.length, 4 );
                response.forEach ( ( item, idx ) => {
                    const savedItem = R.find ( i => i.item.placeName === item.item.placeName, R.map ( R.prop ( 'item' ), R.values ( context.items ) ) );
                    rc = rc && testHelpers.equals ( `getItems geoItem response[${idx}].item`, R.omit ( [ 'distance' ], item ), savedItem );
                } );
                rc = rc && testHelpers.equals ( `getItems geoItem response[].item.placeName`, R.map ( R.path ( [ 'item', 'placeName' ] ), response ), [
                    'centralLondon',
                    'canaryWharf',
                    'northWeald',
                    'manchester'
                ] );
                rc = rc && testHelpers.equals ( `getItems geoItem response[].distance`, R.map ( R.path ( [ 'distance' ] ), response ), [
                    0.0001,
                    0.4451,
                    19.9525,
                    163.0013
                ] );
                return rc;
            }
        },
        {
            method: 'getItemIds',
            args: R.memoizeWith ( R.always ( 'a' ), () => ( [ 'geoItem', { ...context.items.centralLondon.latlng, radius: 164 } ] ) ),
            validateMore: ( testHelpers, response ) => {
                var rc = true;
                rc = rc && testHelpers.equals ( `getItemIds geoItem response.length`, response.length, 4 );
                response.forEach ( ( item, idx ) => {
                    const savedItem = R.find ( i => i.id === item.id, R.map ( R.prop ( 'item' ), R.values ( context.items ) ) );
                    rc = rc && testHelpers.equals ( `getItemIds geoItem response[${idx}]`, R.omit ( [ 'distance' ], item ), { ...R.pick ( [ 'id', 'iid', 'lastModifiedTime', 'type' ], savedItem ), item: {} } );
                } );
                rc = rc && testHelpers.equals ( `getItems geoItem response[].distance`, R.map ( R.path ( [ 'distance' ] ), response ), [
                    0.0001,
                    0.4451,
                    19.9525,
                    163.0013
                ] );
                context.itemIds = response;
                return rc;
            }
        },
        {
            method: 'hydrateItemIds',
            args: R.memoizeWith ( R.always ( 'a' ), () => ( [ 'geoItem', context.itemIds ] ) ),
            validateMore: ( testHelpers, response ) => {
                var rc = true;
                rc = rc && testHelpers.equals ( `getItems geoItem response.length`, response.length, 4 );
                response.forEach ( ( item, idx ) => {
                    const savedItem = R.find ( i => i.item.placeName === item.item.placeName, R.map ( R.prop ( 'item' ), R.values ( context.items ) ) );
                    rc = rc && testHelpers.equals ( `getItems geoItem response[${idx}].item`, R.omit ( [ 'distance' ], item ), savedItem );
                } );
                rc = rc && testHelpers.equals ( `getItems geoItem response[].item.placeName`, R.map ( R.path ( [ 'item', 'placeName' ] ), response ), [
                    'centralLondon',
                    'canaryWharf',
                    'northWeald',
                    'manchester'
                ] );
                rc = rc && testHelpers.equals ( `getItems geoItem response[].distance`, R.map ( R.path ( [ 'distance' ] ), response ), [
                    0.0001,
                    0.4451,
                    19.9525,
                    163.0013
                ] );
                return rc;
            }
        },
        {
            method: 'getItemCount',
            args: R.memoizeWith ( R.always ( 'a' ), () => ( [ 'geoItem', { ...context.items.centralLondon.latlng, radius: 164 } ] ) ),
            validateMore: ( testHelpers, response ) => {
                var rc = true;
                rc = rc && testHelpers.equals ( `getItems geoItem response`, response, 4 );
                return rc;
            }
        },
        {
            method: 'getItems',
            args: R.memoizeWith ( R.always ( 'a' ), () => ( [ 'geoItem', { ...context.items.centralLondon.latlng, radius: 164, count: 2 } ] ) ),
            validateMore: ( testHelpers, response ) => {
                var rc = true;
                rc = rc && testHelpers.equals ( `getItems geoItem response.length`, response.length, 2 );
                response.forEach ( ( item, idx ) => {
                    const savedItem = R.find ( i => i.item.placeName === item.item.placeName, R.map ( R.prop ( 'item' ), R.values ( context.items ) ) );
                    rc = rc && testHelpers.equals ( `getItems geoItem response[${idx}].item`, R.omit ( [ 'distance' ], item ), savedItem );
                } );
                rc = rc && testHelpers.equals ( `getItems geoItem response[].item.placeName`, R.map ( R.path ( [ 'item', 'placeName' ] ), response ), [
                    'centralLondon',
                    'canaryWharf'
                ] );
                rc = rc && testHelpers.equals ( `getItems geoItem response[].distance`, R.map ( R.path ( [ 'distance' ] ), response ), [
                    0.0001,
                    0.4451
                ] );
                return rc;
            }
        },
        {
            method: 'getItemIds',
            args: R.memoizeWith ( R.always ( 'a' ), () => ( [ 'geoItem', { ...context.items.centralLondon.latlng, radius: 164, count: 2 } ] ) ),
            validateMore: ( testHelpers, response ) => {
                var rc = true;
                rc = rc && testHelpers.equals ( `getItemIds geoItem response.length`, response.length, 2 );
                response.forEach ( ( item, idx ) => {
                    const savedItem = R.find ( i => i.id === item.id, R.map ( R.prop ( 'item' ), R.values ( context.items ) ) );
                    rc = rc && testHelpers.equals ( `getItemIds geoItem response[${idx}]`, R.omit ( [ 'distance' ], item ), { ...R.pick ( [ 'id', 'iid', 'lastModifiedTime', 'type' ], savedItem ), item: {} } );
                } );
                rc = rc && testHelpers.equals ( `getItems geoItem response[].distance`, R.map ( R.path ( [ 'distance' ] ), response ), [
                    0.0001,
                    0.4451
                ] );
                return rc;
            }
        },
        {
            method: 'getItemCount',
            args: R.memoizeWith ( R.always ( 'a' ), () => ( [ 'geoItem', { ...context.items.centralLondon.latlng, radius: 164, count: 2 } ] ) ),
            validateMore: ( testHelpers, response ) => {
                var rc = true;
                rc = rc && testHelpers.equals ( `getItems geoItem response`, response, 4 );
                return rc;
            }
        },
        {
            method: 'getItems',
            args: R.memoizeWith ( R.always ( 'a' ), () => ( [ 'geoItem', { ...context.items.centralLondon.latlng, radius: 164, count: 2, offset: 2 } ] ) ),
            validateMore: ( testHelpers, response ) => {
                var rc = true;
                rc = rc && testHelpers.equals ( `getItems geoItem response.length`, response.length, 2 );
                response.forEach ( ( item, idx ) => {
                    const savedItem = R.find ( i => i.item.placeName === item.item.placeName, R.map ( R.prop ( 'item' ), R.values ( context.items ) ) );
                    rc = rc && testHelpers.equals ( `getItems geoItem response[${idx}].item`, R.omit ( [ 'distance' ], item ), savedItem );
                } );
                rc = rc && testHelpers.equals ( `getItems geoItem response[].item.placeName`, R.map ( R.path ( [ 'item', 'placeName' ] ), response ), [
                    'northWeald',
                    'manchester'
                ] );
                rc = rc && testHelpers.equals ( `getItems geoItem response[].distance`, R.map ( R.path ( [ 'distance' ] ), response ), [
                    19.9525,
                    163.0013
                ] );
                return rc;
            }
        },
        {
            method: 'getItemIds',
            args: R.memoizeWith ( R.always ( 'a' ), () => ( [ 'geoItem', { ...context.items.centralLondon.latlng, radius: 164, count: 2, offset: 2 } ] ) ),
            validateMore: ( testHelpers, response ) => {
                var rc = true;
                rc = rc && testHelpers.equals ( `getItemIds geoItem response.length`, response.length, 2 );
                response.forEach ( ( item, idx ) => {
                    const savedItem = R.find ( i => i.id === item.id, R.map ( R.prop ( 'item' ), R.values ( context.items ) ) );
                    rc = rc && testHelpers.equals ( `getItemIds geoItem response[${idx}]`, R.omit ( [ 'distance' ], item ), { ...R.pick ( [ 'id', 'iid', 'lastModifiedTime', 'type' ], savedItem ), item: {} } );
                } );
                rc = rc && testHelpers.equals ( `getItems geoItem response[].distance`, R.map ( R.path ( [ 'distance' ] ), response ), [
                    19.9525,
                    163.0013
                ] );
                context.itemIds = response;
                return rc;
            }
        },
        {
            method: 'hydrateItemIds',
            args: R.memoizeWith ( R.always ( 'a' ), () => ( [ 'geoItem', context.itemIds ] ) ),
            validateMore: ( testHelpers, response ) => {
                var rc = true;
                rc = rc && testHelpers.equals ( `getItems geoItem response.length`, response.length, 2 );
                response.forEach ( ( item, idx ) => {
                    const savedItem = R.find ( i => i.item.placeName === item.item.placeName, R.map ( R.prop ( 'item' ), R.values ( context.items ) ) );
                    rc = rc && testHelpers.equals ( `getItems geoItem response[${idx}].item`, R.omit ( [ 'distance' ], item ), savedItem );
                } );
                rc = rc && testHelpers.equals ( `getItems geoItem response[].item.placeName`, R.map ( R.path ( [ 'item', 'placeName' ] ), response ), [
                    'northWeald',
                    'manchester'
                ] );
                rc = rc && testHelpers.equals ( `getItems geoItem response[].distance`, R.map ( R.path ( [ 'distance' ] ), response ), [
                    19.9525,
                    163.0013
                ] );
                return rc;
            }
        },
        {
            method: 'getItemCount',
            args: R.memoizeWith ( R.always ( 'a' ), () => ( [ 'geoItem', { ...context.items.centralLondon.latlng, radius: 164, count: 2, offset: 2 } ] ) ),
            validateMore: ( testHelpers, response ) => {
                var rc = true;
                rc = rc && testHelpers.equals ( `getItems geoItem response`, response, 4 );
                return rc;
            }
        },
    ],
] );
