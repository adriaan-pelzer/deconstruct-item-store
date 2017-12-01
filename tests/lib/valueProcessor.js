const R = require ( 'ramda' );
const md5 = require ( 'md5' );

const valueProcessor = ( value, attrs ) => {
    const transform = {
        md5Hash: md5,
        int: parseInt,
        float: parseFloat,
        string: s => s.toString ()
    };

    return R.reduce ( ( value, attr ) => {
        return ( transform[attr] || R.identity )( value );
    }, value, attrs );
};

module.exports = valueProcessor;
