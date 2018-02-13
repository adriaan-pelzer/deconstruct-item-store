module.exports = {
    automaticMetadataPaths: [
        { path: [ 'parent_iid' ], attrs: [ 'string' ] },
        { path: [ 'parent_type' ], attrs: [ 'string' ] }
    ],
    typeConfigs: {
        'standardItem': {
            metadataPaths: [
                { path: [ 'name' ], attrs: [ 'string' ] },
                { path: [ 'url' ], attrs: [ 'string', 'md5Hash' ] },
                { path: [ 'enabled' ], attrs: [ 'boolean' ] },
                { path: [ 'count' ], attrs: [ 'int' ] }
            ]
        },
        'standardItemOverrides': {
            metadataPaths: [
                { path: [ 'name' ], attrs: [ 'string' ] },
                { path: [ 'url' ], attrs: [ 'string', 'md5Hash' ] },
                { path: [ 'enabled' ], attrs: [ 'boolean' ] },
                { path: [ 'lastModifiedTime' ], attrs: [ 'int' ] },
                { path: [ 'publishedTime' ], attrs: [ 'int' ] }
            ]
        },
        'geoItem': {
            metadataPaths: [
                { path: [ 'lat' ], attrs: [ 'float' ] },
                { path: [ 'lng' ], attrs: [ 'float' ] }
            ]
        },
        'atomicItem': {
            metadataPaths: [
                { path: [ 'key' ], type: 'key' },
                { path: [ 'dehydratedKey' ], type: 'key', attrs: [ 'dehydrated' ] },
                { path: [ 'list' ], type: 'list' },
                { path: [ 'dehydratedList' ], type: 'list', attrs: [ 'dehydrated' ] },
                { path: [ 'set' ], type: 'set' },
                { path: [ 'dehydratedSet' ], type: 'set', attrs: [ 'dehydrated' ] },
                { path: [ 'sortedSet' ], type: 'zset' },
                { path: [ 'dehydratedSortedSet' ], type: 'zset', attrs: [ 'dehydrated' ] },
                { path: [ 'hyperLogLog' ], type: 'hll' },
                { path: [ 'dehydratedHyperLogLog' ], type: 'hll', attrs: [ 'dehydrated' ] }
            ]
        }
    }
};
