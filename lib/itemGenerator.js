const H = require ( 'highland' );
const R = require ( 'ramda' );
const itemStore = require ( './itemStore.js' );

const itemGenerator = ( type, qs, before, pIds, utils ) => {
    const prevIds = pIds || [];

    if ( type === undefined ) {
        const clients = itemStore.getClients ();
        return clients.redis.quit ();
    }

    return H ( ( push, next ) => {
        return H.wrapCallback ( itemStore.getItems )( null, type, R.merge ( qs || {}, {
            before: before
        } ) )
            .map ( R.filter ( item => {
                return R.not ( R.contains ( item.id, prevIds ) );
            } ) )
            .errors ( R.unary ( push ) )
            .each ( items => {
                if ( items.length ) {
                    const allItemsUpdatedAtTheSameTime = items.length > 1 && R.length ( R.uniq ( R.map ( R.prop ( 'lastModifiedTime' ), items ) ) ) === 1;

                    items.forEach ( item => {
                        push ( null, item );
                    } );
                    return setTimeout ( () => {
                        next ( itemGenerator ( type, allItemsUpdatedAtTheSameTime ? R.merge ( qs, {
                            count: Math.min ( ( qs.count || 100 ) * 2, 1000 )
                        } ) : qs, R.last ( items ).lastModifiedTime, R.concat ( prevIds, R.map ( R.prop ( 'id' ), items ) ), utils ) );
                    }, 0 );
                }

                return push ( null, H.nil );
            } );
    } );
};

module.exports = {
    generator: itemGenerator,
    loadConfig: config => {
        itemStore.loadConfig ( config );
    }
};
