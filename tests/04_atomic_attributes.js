const R = require ( 'ramda' );
const itemStore = require ( '../lib/itemStore.js' );
const config = require ( './lib/config.js' );
const valueProcessor = require ( './lib/valueProcessor.js' );

const context = {
    items: {
        plain: {
            atomicAttrs: {
                key: 'hallo',
                dehydratedKey: 'bye',
                list: [ '0', '1', '2', '3' ],
                dehydratedList: [ 'a', 'b', 'c', 'd' ],
                set: [ '00', '11', '22', '33' ],
                dehydratedSet: [ 'aa', 'bb', 'cc', 'dd' ],
                sortedSet: [
                    { score: 0, member: '0' },
                    { score: 1, member: '1' },
                    { score: 2, member: '2' },
                    { score: 3, member: '3' }
                ],
                dehydratedSortedSet: [
                    { score: 0, member: 'a' },
                    { score: 1, member: 'b' },
                    { score: 2, member: 'c' },
                    { score: 3, member: 'd' }
                ],
                hyperLogLog: [ 'aa', 'bb', 'cc', 'dd' ]
            }
        },
        dupSet: {
            atomicAttrs: {
                set: [ '00', '11', '11', '22', '33' ],
                dehydratedSet: [ 'aa', 'bb', 'bb', 'cc', 'dd' ],
                hyperLogLog: [ 'aa', 'bb', 'cc', 'cc', 'dd' ]
            }
        }
    },
    compare: ( method, testHelpers, atomicAttrs, response ) => {
        var rc = true;
        rc = rc && testHelpers.equals ( `${method} atomicItem response.item.key`, response.item.key, atomicAttrs.key );
        rc = rc && testHelpers.equals ( `${method} atomicItem response.item.dehydratedKey`, response.item.dehydratedKey, atomicAttrs.dehydratedKey ? {
            type: 'key',
            attrs: [ 'dehydrated' ],
            cardinality: 1
        } : undefined );
        rc = rc && testHelpers.equals ( `${method} atomicItem response.item.list`, response.item.list, atomicAttrs.list );
        rc = rc && testHelpers.equals ( `${method} atomicItem response.item.dehydratedList`, response.item.dehydratedList, atomicAttrs.dehydratedList ? {
            type: 'list',
            attrs: [ 'dehydrated' ],
            cardinality: atomicAttrs.dehydratedList.length
        } : undefined );
        rc = rc && testHelpers.equals ( `${method} atomicItem response.item.set`, R.difference ( response.item.set || [], R.uniq ( atomicAttrs.set || [] ) ), [] );
        rc = rc && testHelpers.equals ( `${method} atomicItem response.item.dehydratedSet`, response.item.dehydratedSet, atomicAttrs.dehydratedSet ? {
            type: 'set',
            attrs: [ 'dehydrated' ],
            cardinality: R.uniq ( atomicAttrs.dehydratedSet ).length
        } : undefined );
        rc = rc && testHelpers.equals ( `${method} atomicItem response.item.sortedSet`, response.item.sortedSet || [], R.uniqBy ( R.prop ( 'member' ), atomicAttrs.sortedSet || [] ) );
        rc = rc && testHelpers.equals ( `${method} atomicItem response.item.dehydratedSortedSet`, response.item.dehydratedSortedSet, atomicAttrs.dehydratedSortedSet ? {
            type: 'zset',
            attrs: [ 'dehydrated' ],
            cardinality: R.uniqBy ( R.prop ( 'member' ), atomicAttrs.dehydratedSortedSet ).length
        } : undefined );
        rc = rc && testHelpers.equals ( `${method} atomicItem response.item.hyperLogLog`, response.item.hyperLogLog || [], R.uniq ( atomicAttrs.hyperLogLog || [] ).length );
        rc = rc && testHelpers.equals ( `${method} atomicItem response.item.dehydratedHyperLogLog`, typeof response.item.dehydratedHyperLogLog, 'undefined' );
        return rc;
    }
};

module.exports = R.concat ( R.reduce ( R.concat, [], R.map ( itemName => ( [
    {
        method: 'saveItem',
        args: R.memoizeWith ( R.always ( 'a' ), () => ( [ 'atomicItem', itemStore.generateId (), null, context.items[itemName].atomicAttrs ] ) ),
        validateMore: ( testHelpers, response ) => {
            context.items[itemName].id = response.id;
            context.items[itemName].previous_id = undefined;
            context.items[itemName].iid = response.id;
            return true;
        }
    },
    {
        method: 'getItem',
        args: R.memoizeWith ( R.always ( 'a' ), () => ( [ 'atomicItem', context.items[itemName].id ] ) ),
        validateMore: ( testHelpers, response ) => {
            var rc = true;
            rc = rc && testHelpers.equals ( `getItem atomicItem response.id`, response.id, context.items[itemName].id );
            rc = rc && testHelpers.equals ( `getItem atomicItem response.iid`, response.iid, context.items[itemName].iid );
            rc = rc && testHelpers.equals ( `getItem atomicItem response.previous_id`, typeof response.previous_id, 'undefined' );
            rc = rc && context.compare ( 'getItem', testHelpers, context.items[itemName].atomicAttrs, response );
            context.items[itemName].item = response;
            return rc;
        }
    },
    {
        method: 'deleteItem',
        args: R.memoizeWith ( R.always ( 'a' ), () => ( [ 'atomicItem', context.items[itemName].id ] ) ),
        validateMore: ( testHelpers, response ) => {
            var rc = true;
            context.items[itemName].previous_id = context.items[itemName].id
            return rc;
        }
    },
    {
        method: 'saveItem',
        args: R.memoizeWith ( R.always ( 'a' ), () => ( [ 'atomicItem', itemStore.generateId (), context.items[itemName].item, context.items[itemName].atomicAttrs ] ) ),
        validateMore: ( testHelpers, response ) => {
            var rc = true;
            rc = rc && testHelpers.equals ( `getItems atomicItem response.iid`, response.iid, context.items[itemName].iid );
            context.items[itemName].id = response.id;
            return rc;
        }
    },
    {
        method: 'getItem',
        args: R.memoizeWith ( R.always ( 'a' ), () => ( [ 'atomicItem', context.items[itemName].id ] ) ),
        validateMore: ( testHelpers, response ) => {
            var rc = true;
            rc = rc && testHelpers.equals ( `getItem atomicItem response.id`, response.id, context.items[itemName].id );
            rc = rc && testHelpers.equals ( `getItem atomicItem response.iid`, response.iid, context.items[itemName].iid );
            rc = rc && testHelpers.equals ( `getItem atomicItem response.previous_id`, response.previous_id, context.items[itemName].previous_id );
            rc = rc && testHelpers.equals ( `getItem atomicItem response.item`, response.item, context.items[itemName].item.item );
            context.items[itemName].item = response;
            return rc;
        }
    },
    {
        method: 'deleteItem',
        args: R.memoizeWith ( R.always ( 'a' ), () => ( [ 'atomicItem', context.items[itemName].id ] ) ),
        validateMore: ( testHelpers, response ) => {
            var rc = true;
            context.items[itemName].previous_id = context.items[itemName].id
            return rc;
        }
    },
    {
        method: 'saveItem',
        args: R.memoizeWith ( R.always ( 'a' ), () => ( [ 'atomicItem', itemStore.generateId (), context.items[itemName].item, context.items[itemName].atomicAttrs ] ) ),
        validateMore: ( testHelpers, response ) => {
            var rc = true;
            rc = rc && testHelpers.equals ( `saveItem atomicItem response.iid`, response.iid, context.items[itemName].iid );
            context.items[itemName].id = response.id;
            return rc;
        }
    },
    {
        method: 'getItem',
        args: R.memoizeWith ( R.always ( 'a' ), () => ( [ 'atomicItem', context.items[itemName].id ] ) ),
        validateMore: ( testHelpers, response ) => {
            var rc = true;
            rc = rc && testHelpers.equals ( `getItem atomicItem response.id`, response.id, context.items[itemName].id );
            rc = rc && testHelpers.equals ( `getItem atomicItem response.iid`, response.iid, context.items[itemName].iid );
            rc = rc && testHelpers.equals ( `getItem atomicItem response.previous_id`, response.previous_id, context.items[itemName].previous_id );
            rc = rc && testHelpers.equals ( `getItem atomicItem response.item`, response.item, context.items[itemName].item.item );
            context.items[itemName].item = response;
            return rc;
        }
    },
    {
        method: 'deleteItem',
        args: R.memoizeWith ( R.always ( 'a' ), () => ( [ 'atomicItem', context.items[itemName].id ] ) ),
        validateMore: ( testHelpers, response ) => {
            var rc = true;
            context.items[itemName].previous_id = context.items[itemName].id
            return rc;
        }
    },
    {
        method: 'saveItem',
        args: R.memoizeWith ( R.always ( 'a' ), () => ( [ 'atomicItem', itemStore.generateId (), context.items[itemName].item, {} ] ) ),
        validateMore: ( testHelpers, response ) => {
            var rc = true;
            rc = rc && testHelpers.equals ( `saveItem atomicItem response.iid`, response.iid, context.items[itemName].iid );
            context.items[itemName].id = response.id;
            return rc;
        }
    },
    {
        method: 'getItem',
        args: R.memoizeWith ( R.always ( 'a' ), () => ( [ 'atomicItem', context.items[itemName].id ] ) ),
        validateMore: ( testHelpers, response ) => {
            var rc = true;
            rc = rc && testHelpers.equals ( `getItem atomicItem response.id`, response.id, context.items[itemName].id );
            rc = rc && testHelpers.equals ( `getItem atomicItem response.iid`, response.iid, context.items[itemName].iid );
            rc = rc && testHelpers.equals ( `getItem atomicItem response.previous_id`, response.previous_id, context.items[itemName].previous_id );
            rc = rc && testHelpers.equals ( `getItem atomicItem response.item`, response.item, context.items[itemName].item.item );
            context.items[itemName].item = response;
            return rc;
        }
    },
] ), R.keys ( context.items ) ) ), [
    {
        method: 'getItemIds',
        args: R.memoizeWith ( R.always ( 'a' ), () => ( [ 'atomicItem', {} ] ) ),
        validateMore: ( testHelpers, response ) => {
            var rc = true;
            context.count++;
            rc = rc && testHelpers.equals ( `getItemIds atomicItem response.length`, response.length, 2 );
            response.forEach ( ( item, idx ) => {
                const itemName = R.find ( itemName => {
                    return context.items[itemName].id === item.id;
                }, R.keys ( context.items ) );

                rc = rc && testHelpers.equals ( `getItemIds atomicItem response[${idx}].id`, item.id, context.items[itemName].id );
                rc = rc && testHelpers.equals ( `getItemIds atomicItem response[${idx}].iid`, item.iid, context.items[itemName].iid );
                rc = rc && testHelpers.equals ( `getItemIds atomicItem response[${idx}].previous_id`, typeof item.previous_id, 'undefined' );
                rc = rc && testHelpers.equals ( `getItemIds atomicItem response[${idx}].item`, item.item, context.items[itemName].item.item );
                context.items[itemName].dehydratedItem = item;
            } );
            context.getItemIds = response;
            return rc;
        }
    },
    {
        method: 'hydrateItemIds',
        args: R.memoizeWith ( R.always ( 'a' ), () => ( [ 'atomicItem', context.getItemIds ] ) ),
        validateMore: ( testHelpers, response ) => {
            var rc = true;
            rc = rc && testHelpers.equals ( `hydrateItemIds atomicItem response.length`, response.length, 2 );
            response.forEach ( ( item, idx ) => {
                const itemName = R.find ( itemName => {
                    return context.items[itemName].id === item.id;
                }, R.keys ( context.items ) );

                rc = rc && testHelpers.equals ( `hydrateItemIds atomicItem response[${idx}].id`, item.id, context.items[itemName].id );
                rc = rc && testHelpers.equals ( `hydrateItemIds atomicItem response[${idx}].iid`, item.iid, context.items[itemName].iid );
                rc = rc && testHelpers.equals ( `hydrateItemIds atomicItem response[${idx}].previous_id`, item.previous_id, context.items[itemName].previous_id );
                rc = rc && testHelpers.equals ( `hydrateItemIds atomicItem response[${idx}].item`, item.item, context.items[itemName].item.item );
            } );
            return rc;
        }
    },
    {
        method: 'getItems',
        args: R.memoizeWith ( R.always ( 'a' ), () => ( [ 'atomicItem', {} ] ) ),
        validateMore: ( testHelpers, response ) => {
            var rc = true;
            rc = rc && testHelpers.equals ( `getItems atomicItem response.length`, response.length, 2 );
            response.forEach ( ( item, idx ) => {
                const itemName = R.find ( itemName => {
                    return context.items[itemName].id === item.id;
                }, R.keys ( context.items ) );

                rc = rc && testHelpers.equals ( `getItems atomicItem response[${idx}].id`, item.id, context.items[itemName].id );
                rc = rc && testHelpers.equals ( `getItems atomicItem response[${idx}].iid`, item.iid, context.items[itemName].iid );
                rc = rc && testHelpers.equals ( `getItems atomicItem response[${idx}].previous_id`, item.previous_id, context.items[itemName].previous_id );
                rc = rc && testHelpers.equals ( `getItems atomicItem response[${idx}].item`, item.item, context.items[itemName].item.item );
            } );
            return rc;
        }
    },
] );
