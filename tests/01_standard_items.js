const R = require ( 'ramda' );
const itemStore = require ( '../lib/itemStore.js' );
const config = require ( './lib/config.js' );
const valueProcessor = require ( './lib/valueProcessor.js' );

const context = {};

module.exports = [
    {
        method: 'saveItem',
        args: R.memoizeWith ( R.always ( 'a' ), () => ( [ 'standardItem', itemStore.generateId (), null, {
            name: 'standardItem',
            url: 'http://www.test.co.uk/resource',
            enabled: true,
            count: 10
        } ] ) ),
        validateMore: ( testHelpers, response ) => {
            context.savedItem = {
                name: 'standardItem',
                url: 'http://www.test.co.uk/resource',
                enabled: true,
                count: 10
            };
            return true;
        }
    },
    {
        method: 'getItems',
        args: R.memoizeWith ( R.always ( 'a' ), () => ( [ 'standardItem', {} ] ) ),
        validateMore: ( testHelpers, response ) => {
            var rc = true;
            rc = rc && testHelpers.equals ( `getItems standardItem response.length`, response.length, 1 );
            response.forEach ( ( item, idx ) => {
                context.previousItem = item;
                rc = rc && testHelpers.equals ( `getItems standardItem response[${idx}].item`, item.item, context.savedItem );
            } );
            return rc;
        }
    },
    {
        method: 'getItem',
        args: R.memoizeWith ( R.always ( 'a' ), () => ( [ 'standardItem', context.previousItem.id ] ) ),
        validateMore: ( testHelpers, response ) => {
            var rc = true;
            rc = rc && testHelpers.equals ( `getItem standardItem response.item`, response.item, context.savedItem );
            return rc;
        }
    },
    {
        method: 'deleteItem',
        args: R.memoizeWith ( R.always ( 'a' ), () => ( [ 'standardItem', context.previousItem.id ] ) ),
        validateMore: ( testHelpers, response ) => {
            var rc = true;
            const type = context.previousItem.type;
            config.typeConfigs[type].metadataPaths.forEach ( path => {
                const key = R.last ( path.path );
                const value = valueProcessor ( R.path ( path.path, context.previousItem.item ), path.attrs || [] );
                const listName = `items-${type}-${key}-${value}`;
                rc = rc && testHelpers.equals ( `deleteItem ${type} response.list.${listName}`, response.list[listName], 1 );
            } );
            return rc;
        }
    },
    {
        method: 'saveItem',
        args: R.memoizeWith ( R.always ( 'a' ), () => ( [ 'standardItem', itemStore.generateId (), context.previousItem, {
            name: 'standardItemRenamed',
            url: 'http://www.test.co.uk/resource/new',
            enabled: false,
            count: 15
        } ] ) ),
        validateMore: ( testHelpers, response ) => {
            context.savedItems = [ {
                name: 'standardItemRenamed',
                url: 'http://www.test.co.uk/resource/new',
                enabled: false,
                count: 15
            } ];
            return true;
        }
    },
    {
        method: 'getItems',
        args: R.memoizeWith ( R.always ( 'a' ), () => ( [ 'standardItem', {} ] ) ),
        validateMore: ( testHelpers, response ) => {
            var rc = true;
            rc = rc && testHelpers.equals ( `getItems standardItem response.length`, response.length, 1 );
            response.forEach ( ( item, idx ) => {
                rc = rc && testHelpers.equals ( `getItems standardItem response[${idx}].item`, item.item, context.savedItems[0] );
            } );
            return rc;
        }
    },
    {
        method: 'saveItem',
        args: R.memoizeWith ( R.always ( 'a' ), () => ( [ 'standardItem', itemStore.generateId (), null, {
            name: 'standardItem',
            url: 'http://www.test.co.uk/resource/second',
            enabled: true,
            count: 10
        } ] ) ),
        validateMore: ( testHelpers, response ) => {
            context.savedItems.push ( {
                name: 'standardItem',
                url: 'http://www.test.co.uk/resource/second',
                enabled: true,
                count: 10
            } );
            return true;
        }
    },
    {
        method: 'getItems',
        args: R.memoizeWith ( R.always ( 'a' ), () => ( [ 'standardItem', {} ] ) ),
        validateMore: ( testHelpers, response ) => {
            var rc = true;
            rc = rc && testHelpers.equals ( `getItems standardItem response.length`, response.length, 2 );
            response.forEach ( ( item, idx ) => {
                rc = rc && testHelpers.equals ( `getItems standardItem response[${idx}].item`, item.item, R.reverse ( context.savedItems )[idx] );
            } );
            context.items = response;
            context.itemIds = R.map ( R.pick ( [ 'id', 'iid', 'type', 'lastModifiedTime' ] ), response );
            return rc;
        }
    },
    {
        method: 'getItemIds',
        args: R.memoizeWith ( R.always ( 'a' ), () => ( [ 'standardItem', {} ] ) ),
        validateMore: ( testHelpers, response ) => {
            var rc = true;
            rc = rc && testHelpers.equals ( `getItemIds standardItem response.length`, response.length, 2 );
            response.forEach ( ( item, idx ) => {
                rc = rc && testHelpers.equals ( `getItemIds standardItem response[${idx}]`, item, context.itemIds[idx] );
            } );
            context.itemIds = response;
            return rc;
        }
    },
    {
        method: 'hydrateItemIds',
        args: R.memoizeWith ( R.always ( 'a' ), () => ( [ 'standardItem', context.itemIds ] ) ),
        validateMore: ( testHelpers, response ) => {
            var rc = true;
            rc = rc && testHelpers.equals ( `hydrateItemIds standardItem response.length`, response.length, 2 );
            response.forEach ( ( item, idx ) => {
                rc = rc && testHelpers.equals ( `hydrateItemIds standardItem response[${idx}]`, item, context.items[idx] );
            } );
            return rc;
        }
    },
    {
        method: 'getItemCount',
        args: R.memoizeWith ( R.always ( 'a' ), () => ( [ 'standardItem', {} ] ) ),
        validateMore: ( testHelpers, response ) => {
            var rc = true;
            rc = rc && testHelpers.equals ( `getItems standardItem response`, response, 2 );
            return rc;
        }
    },
    {
        method: 'getItems',
        args: R.memoizeWith ( R.always ( 'a' ), () => ( [ 'standardItem', { count: 1 } ] ) ),
        validateMore: ( testHelpers, response ) => {
            var rc = true;
            rc = rc && testHelpers.equals ( `getItems standardItem response.length`, response.length, 1 );
            response.forEach ( ( item, idx ) => {
                rc = rc && testHelpers.equals ( `getItems standardItem response[${idx}].item`, item.item, R.reverse ( context.savedItems )[idx] );
            } );
            context.items = response;
            context.itemIds = R.map ( R.pick ( [ 'id', 'iid', 'type', 'lastModifiedTime' ] ), response );
            return rc;
        }
    },
    {
        method: 'getItemIds',
        args: R.memoizeWith ( R.always ( 'a' ), () => ( [ 'standardItem', { count: 1 } ] ) ),
        validateMore: ( testHelpers, response ) => {
            var rc = true;
            rc = rc && testHelpers.equals ( `getItemIds standardItem response.length`, response.length, 1 );
            response.forEach ( ( item, idx ) => {
                rc = rc && testHelpers.equals ( `getItemIds standardItem response[${idx}]`, item, context.itemIds[idx] );
            } );
            context.itemIds = response;
            return rc;
        }
    },
    {
        method: 'hydrateItemIds',
        args: R.memoizeWith ( R.always ( 'a' ), () => ( [ 'standardItem', context.itemIds ] ) ),
        validateMore: ( testHelpers, response ) => {
            var rc = true;
            rc = rc && testHelpers.equals ( `hydrateItemIds standardItem response.length`, response.length, 1 );
            response.forEach ( ( item, idx ) => {
                rc = rc && testHelpers.equals ( `hydrateItemIds standardItem response[${idx}]`, item, context.items[idx] );
            } );
            return rc;
        }
    },
    {
        method: 'getItemCount',
        args: R.memoizeWith ( R.always ( 'a' ), () => ( [ 'standardItem', { count: 1 } ] ) ),
        validateMore: ( testHelpers, response ) => {
            var rc = true;
            rc = rc && testHelpers.equals ( `getItems standardItem response`, response, 2 );
            return rc;
        }
    },
    {
        method: 'getItems',
        args: R.memoizeWith ( R.always ( 'a' ), () => ( [ 'standardItem', { name: 'standardItemRenamed' } ] ) ),
        validateMore: ( testHelpers, response ) => {
            var rc = true;
            const savedItem = R.find ( item => item.name === 'standardItemRenamed', context.savedItems );
            rc = rc && testHelpers.equals ( `getItems standardItem response.length`, response.length, 1 );
            response.forEach ( ( item, idx ) => {
                rc = rc && testHelpers.equals ( `getItems standardItem response[${idx}].item`, item.item, savedItem );
            } );
            context.items = response;
            context.itemIds = R.map ( R.pick ( [ 'id', 'iid', 'type', 'lastModifiedTime' ] ), response );
            return rc;
        }
    },
    {
        method: 'getItemIds',
        args: R.memoizeWith ( R.always ( 'a' ), () => ( [ 'standardItem', { name: 'standardItemRenamed' } ] ) ),
        validateMore: ( testHelpers, response ) => {
            var rc = true;
            rc = rc && testHelpers.equals ( `getItemIds standardItem response.length`, response.length, 1 );
            response.forEach ( ( item, idx ) => {
                rc = rc && testHelpers.equals ( `getItemIds standardItem response[${idx}]`, item, context.itemIds[idx] );
            } );
            context.itemIds = response;
            return rc;
        }
    },
    {
        method: 'hydrateItemIds',
        args: R.memoizeWith ( R.always ( 'a' ), () => ( [ 'standardItem', context.itemIds ] ) ),
        validateMore: ( testHelpers, response ) => {
            var rc = true;
            rc = rc && testHelpers.equals ( `hydrateItemIds standardItem response.length`, response.length, 1 );
            response.forEach ( ( item, idx ) => {
                rc = rc && testHelpers.equals ( `hydrateItemIds standardItem response[${idx}]`, item, context.items[idx] );
            } );
            return rc;
        }
    },
    {
        method: 'getItemCount',
        args: R.memoizeWith ( R.always ( 'a' ), () => ( [ 'standardItem', { name: 'standardItemRenamed' } ] ) ),
        validateMore: ( testHelpers, response ) => {
            var rc = true;
            rc = rc && testHelpers.equals ( `getItems standardItem response`, response, 1 );
            return rc;
        }
    },
];
