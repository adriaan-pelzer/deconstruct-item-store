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
                    items.forEach ( item => {
                        push ( null, item );
                    } );
                    return setTimeout ( () => {
                        next ( itemGenerator ( type, qs, R.last ( items ).lastModifiedTime, R.map ( R.prop ( 'id' ), items ), utils ) );
                    }, 0 );
                }

                return push ( null, H.nil );
            } );
    } );
};

module.exports = itemGenerator;
