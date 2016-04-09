var thrift = require('thrift'),
    HBase = require('./db/Hbase.js'),
    HBaseTypes = require('./db/Hbase_types.js'),
    connection = thrift.createConnection('10.182.131.133', 9090, {
		transport: thrift.TFramedTransport,
		protocol: thrift.TBinaryProtocol,
		timeout: 10000
	});
	
var client = thrift.createClient(HBase,connection);

exports.db = client;
exports.HBaseTypes = HBaseTypes;


connection.on('connect', function () {
	console.log('hbase thrift connected');
	 
})

connection.on('error', function(err) {
	console.log('hbase connection error');
	console.error(err);
	connection.end();
	connection = thrift.createConnection('10.182.131.133', 9090, {
		transport: thrift.TFramedTransport,
		protocol: thrift.TBinaryProtocol,
		timeout: 10000
	});
	client = thrift.createClient(HBase,connection);
	
	var touch = require("touch");
	touch('./db.js');
	
});
