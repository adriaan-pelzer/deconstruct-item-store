# deconstruct-item-store
A deconstructed API storage paradigm, tailored around the [deconstruct-api](https://github.com/adriaan-pelzer/deconstruct-api), but loosely coupled to a high enough degree to be used anywhere else.

It uses both redis and AWS DynamoDB to provide a very fast, scalable dehydrated/hydrated storage paradigm, using redis to store lists of data item id's, and DynamoDB for the fully hydrated items themselves.

## installation

In your project folder:
```
  npm install --save deconstruct-item-store
```

Specify redis & DynamoDB connection parameters by means of the following environment variables:

```
    REDIS_PORT=<PORT>
    REDIS_HOST=<HOST>
    DYNAMO_TABLE=<TABLE>
```

## items

An item is an atom of data - the smallest unit of data that you want to store.

Every item has a type, and every item is wrapped in an envelope of metadata when it is stored. By default this metadata envelope contains the attributes _id_, _publishedTime_, _lastModifiedTime_, and _type_. The item itself can be found in the _item_ attribute.

The itemStore maintains these attributes, and for sinatnce, assigns a unique id every time an item is stored or updated. It also takes care of updating the timestamps.

### config

Every type can also be specified to have an additional set of custom metadata attributes, as such:

```js
    const config = {
        typeConfigs: {
            type1: {
                path: [ 'some', 'deep', 'attr' ],
                attrs: [ 'string', 'md5Hash' ]
            }
        }
    };

    itemStore.loadConfig ( config );
```

In the above code, the value of _item.some.deep.attr_ will be converted to a string, an md5 hash will be calculated of it, and the result will be stored in the _attr_ metadata attribute. A list of possible custom metadata attrs are:

    string: convert to a string, if it's not already one
    float: convert to a float, if it's not already one
    int: convert to an integer, if it's not already one
    boolean: convert to a boolean, if it's not already one
    md5Hash: calculate the md5 hash of the value
    uriLastPathComp: take only the last path component (after the last '/')
    toLowerCase: convert the value to lower case

The attrs are applied from left to right, in series

The itemStore config can optionally specify redis and dynamo connection parameters, but these are best provided as environment variables (see _installation_ above)

```js
    const config = {
        redis: {
            host: <HOST>,
            port: <PORT>
        },
        dynamo: {
            table: <TABLE>
        }
    };
```

## itemStore

```js
    const itemStore = require ( 'deconstruct-item-store' ).itemStore
```

All the methods below are curried.

### saveItem

```js
    itemStore.saveItem ( stubs, type, id, prevItem, item, callback );
```

### deleteItem

```js
    itemStore.deleteItem ( stubs, type, id, item, callback );
```

### getItem

```js
    itemStore.getItem ( stubs, type, id, callback );
```

### getItems

```js
    itemStore.getItems ( stubs, type, query, callback );
```

### getItemIds

```js
    itemStore.getItemIds ( stubs, type, query, callback );
```

### hydrateIds

```js
    itemStore.hydrateIds ( stubs, type, ids, callback );
```

## itemGenerator

```js
    const itemGenerator = require ( 'deconstruct-item-store' ).itemGenerator
```
