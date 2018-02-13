const R = require ( 'ramda' );
const defaultAttrs = [ 'lastModifiedTime', 'publishedTime', 'id', 'iid', 'type', 'previousId', 'parent_iid', 'parent_type', 'lng', 'lat' ];

module.exports = ( { config, valueProcessor, test, testHelpers, response } ) => ( {
    saveItem: ( [ type, id, prevItem, item ] ) => {
        var rc = true;
        rc = rc && testHelpers.equals ( `generic ${test.method} ${type} response.id`, response.id, id );
        if ( ! prevItem ) {
            rc = rc && testHelpers.equals ( `generic ${test.method} ${type} response.iid`, response.iid, id );
            rc = rc && testHelpers.equals ( `generic ${test.method} ${type} response.list.items-${type}-iid-${id}`, response.list[`items-${type}-iid-${id}`], 1 );
        } else {
            rc = rc && testHelpers.equals ( `generic ${test.method} ${type} response.iid`, response.iid, prevItem.iid );
            rc = rc && testHelpers.equals ( `generic ${test.method} ${type} response.list.items-${type}-iid-${prevItem.iid}`, response.list[`items-${type}-iid-${prevItem.iid}`], 1 );
        }
        rc = rc && testHelpers.equals ( `generic ${test.method} ${type} response.type`, response.type, type );
        rc = rc && testHelpers.equals ( `generic ${test.method} ${type} response.list.items-${type}`, response.list[`items-${type}`], 1 );
        rc = rc && testHelpers.equals ( `generic ${test.method} ${type} response.geolist.items-${type}-geo`, response.geolist[`items-${type}-geo`], item.lng && item.lat ? 1 : 0 );
        rc = rc && testHelpers.equals ( `generic ${test.method} ${type} response.store`, response.store, 1 );
        R.filter ( path => R.isNil ( path.type ), config.typeConfigs[type].metadataPaths ).forEach ( path => {
            const key = R.last ( path.path );
            const value = valueProcessor ( R.path ( path.path, item ), path.attrs || [] );
            const listName = `items-${type}-${key}-${value}`;
            if ( ! R.contains ( key, defaultAttrs ) ) {
                rc = rc && testHelpers.equals ( `generic ${test.method} ${type} response.list.${listName}`, response.list[listName], 1 );
            }
        } );
        R.reject ( path => R.isNil ( path.type ), config.typeConfigs[type].metadataPaths ).forEach ( path => {
            const key = R.last ( path.path );
            const value = valueProcessor ( R.path ( path.path, item ), path.attrs || [] );
            const listName = `items-${type}-${key}-${value}`;
            const redisKey = `${response.iid}-${path.path.join ( '-' )}`;
            const atomicValue = R.path ( path.path, item ) && R.flatten ( [ R.path ( path.path, item ) ] );
            const getExpectedCard = ( type, value ) => {
                if ( type === 'key' ) { return 'OK'; }
                if ( R.type ( value ) !== 'Array' ) { return 0; }
                if ( type === 'hll' ) { return 1; }
                if ( type === 'set' ) { return R.uniq ( value ).length; }
                return value.length;
            };

            if ( atomicValue ) {
                rc = rc && testHelpers.equals ( `generic ${test.method} ${type} response.atomic.${redisKey}`, response.atomic[redisKey], getExpectedCard ( path.type, atomicValue ) );
            }
        } );
        return rc;
    },
    deleteItem: ( [ type, id ] ) => {
        var rc = true;
        rc = rc && testHelpers.equals ( `generic ${test.method} ${type} response.list.items-${type}`, response.list[`items-${type}`], 1 );
        return rc;
    },
    getItem: ( [ type, id ] ) => {
        var rc = true;
        rc = rc && testHelpers.equals ( `generic ${test.method} ${type} response.type`, response.type, type );
        rc = rc && testHelpers.equals ( `generic ${test.method} ${type} response.lastModifiedTime type`, R.type ( response.lastModifiedTime ), 'Number' );
        rc = rc && testHelpers.equals ( `generic ${test.method} ${type} response.publishedTime type`, R.type ( response.publishedTime ), 'Number' );
        config.typeConfigs[type].metadataPaths.forEach ( path => {
            const key = R.last ( path.path );
            const value = valueProcessor ( R.path ( path.path, response.item ), path.attrs || [] );
            if ( ! R.contains ( key, defaultAttrs ) && ! path.type ) {
                rc = rc && testHelpers.equals ( `generic ${test.method} ${type} response.${key}`, response[key], value );
            }
        } );
        return rc;
    },
    getItems: ( [ type, query ] ) => {
        var rc = true;
        response.forEach ( ( item, idx ) => {
            rc = rc && testHelpers.equals ( `generic ${test.method} ${type} response[${idx}].type`, item.type, type );
            rc = rc && testHelpers.equals ( `generic ${test.method} ${type} response[${idx}].lastModifiedTime type`, R.type ( item.lastModifiedTime ), 'Number' );
            rc = rc && testHelpers.equals ( `generic ${test.method} ${type} response[${idx}].publishedTime type`, R.type ( item.publishedTime ), 'Number' );
            if ( query.lat && query.lng ) {
                rc = rc && testHelpers.equals ( `generic ${test.method} ${type} response[${idx}].distance type`, R.type ( item.distance ), 'Number' );
            }
            R.filter ( path => R.isNil ( path.type ), config.typeConfigs[type].metadataPaths ).forEach ( path => {
                const key = R.last ( path.path );
                const value = valueProcessor ( R.path ( path.path, item.item ), path.attrs || [] );
                if ( ! R.contains ( key, defaultAttrs ) ) {
                    rc = rc && testHelpers.equals ( `generic ${test.method} ${type} response[${idx}].${key}`, item[key], value );
                }
            } );
        } );
        return rc;
    }
} );
