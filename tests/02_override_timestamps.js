const R = require ( 'ramda' );
const itemStore = require ( '../lib/itemStore.js' );
const config = require ( './lib/config.js' );
const valueProcessor = require ( './lib/valueProcessor.js' );

const context = {};
const lastModifiedTime = new Date ().valueOf ();
const publishedTime = lastModifiedTime - 24 * 60 * 60 * 1000;
const item = {
    name: 'standardItemOverride',
    url: 'http://www.test.co.uk/resource',
    enabled: true,
    lastModifiedTime: lastModifiedTime,
    publishedTime: publishedTime
}

module.exports = [
    {
        method: 'saveItem',
        args: R.memoizeWith ( R.always ( 'a' ), () => ( [ 'standardItemOverrides', itemStore.generateId (), null, item ] ) ),
        validateMore: ( testHelpers, response ) => {
            context.savedItem = item;
            return true;
        }
    },
    {
        method: 'getItems',
        args: R.memoizeWith ( R.always ( 'a' ), () => ( [ 'standardItemOverrides', {} ] ) ),
        validateMore: ( testHelpers, response ) => {
            var rc = true;
            rc = rc && testHelpers.equals ( `getItems standardItem response.length`, response.length, 1 );
            response.forEach ( ( item, idx ) => {
                rc = rc && testHelpers.equals ( `getItems standardItem response[${idx}].item`, item.item, context.savedItem );
                rc = rc && testHelpers.equals ( `getItems standardItem response[${idx}].item.lastModifiedTime`, item.lastModifiedTime, lastModifiedTime );
                rc = rc && testHelpers.equals ( `getItems standardItem response[${idx}].item.publishedTime`, item.publishedTime, publishedTime );
                context.previousItem = item;
            } );
            return rc;
        }
    },
    {
        method: 'deleteItem',
        args: R.memoizeWith ( R.always ( 'a' ), () => ( [ 'standardItemOverrides', context.previousItem.id ] ) ),
        validateMore: ( testHelpers, response ) => {
            var rc = true;
            const type = context.previousItem.type;
            config.typeConfigs[type].metadataPaths.forEach ( path => {
                const key = R.last ( path.path );
                const value = valueProcessor ( R.path ( path.path, context.previousItem.item ), path.attrs || [] );
                const listName = `items-${type}-${key}-${value}`;
                if ( ! R.contains ( key, [ 'lastModifiedTime', 'publishedTime' ] ) ) {
                    rc = rc && testHelpers.equals ( `deleteItem ${type} response.list.${listName}`, response.list[listName], 1 );
                }
            } );
            return rc;
        }
    },
    {
        method: 'saveItem',
        args: R.memoizeWith ( R.always ( 'a' ), () => ( [ 'standardItemOverrides', itemStore.generateId (), context.previousItem, {
            ...item,
            name: 'standardItemRenamed',
            url: 'http://www.test.co.uk/resource/new'
        } ] ) )
    },
    {
        method: 'getItems',
        args: R.memoizeWith ( R.always ( 'a' ), () => ( [ 'standardItemOverrides', {} ] ) ),
        validateMore: ( testHelpers, response ) => {
            var rc = true;
            rc = rc && testHelpers.equals ( `getItems standardItem response.length`, response.length, 1 );
            response.forEach ( ( item, idx ) => {
                rc = rc && testHelpers.equals ( `getItems standardItem response[${idx}].item`, item.item, {
                    ... context.savedItem,
                    name: 'standardItemRenamed',
                    url: 'http://www.test.co.uk/resource/new'
                } );
                rc = rc && testHelpers.equals ( `getItems standardItem response[${idx}].item.lastModifiedTime`, item.lastModifiedTime, lastModifiedTime );
                rc = rc && testHelpers.equals ( `getItems standardItem response[${idx}].item.publishedTime`, item.publishedTime, publishedTime );
                context.previousItem = item;
            } );
            return rc;
        }
    },
];
