
/**
 * Module dependencies.
 */

var express = require('express'),
	https = require('https'),
	fs = require('fs');
		
var db =  require('./db').db;
var HBaseTypes =  require('./db').HBaseTypes;
	
var memcachedStore = require('connect-memcached')(express);

// Memcached session store
sessionObj = new memcachedStore({
				hosts: [ '1.2.3.4:11211' ],
				prefix: 'sample'
			});

	
// Configurations
	
var app = express();

// If SSL is required
/*
var sslOptions = {
  key: fs.readFileSync('cert/xxx.key'),
  cert: fs.readFileSync('cert/xxx.crt'),
  ca: [
  	fs.readFileSync('cert/ca.crt', 'utf8')
	]
};
*/

var allowCrossDomain = function(req, res, next) {
  res.header("Access-Control-Allow-Origin", "https://xxx.com");
  res.header("Access-Control-Allow-Headers", "X-Requested-With");
  next();
};

app.disable('x-powered-by');
app.use(express.compress());
app.use(express.timeout(1200000));
app.use(express.methodOverride());
app.use(express.logger('dev'));
app.use(express.json({limit: '250mb'}));
app.use(express.urlencoded({limit: '250mb'}));
app.use(express.cookieParser(''));
app.use(express.session({
	key: 'somekey',
	secret: '',
	cookie: {
		path: '/',
		httpOnly : true,
		secure: false,  
		maxAge  : new Date(Date.now() + 3600000)
	},
	store: sessionObj
}));
app.use(allowCrossDomain);
	
if(app.get('env') == 'development'){
  app.use(express.errorHandler({ dumpExceptions: true, showStack: true }));
}

if(app.get('env') == 'production'){
  app.use(express.errorHandler());
}

var server  = https.createServer(sslOptions, app);

var io = require('socket.io').listen(server);

server.listen(11747);

// Routes
	
require('./routes/get')(app, express, db, HBaseTypes, io);

app.use(function(err,req, res, next){
	
	//500 Route
	console.log('500 error');
	console.log(err);
	res.status(500);
		
	
});

//Global Error Logging

process.on('uncaughtException', function(err) {
	
	console.error((new Date()).toUTCString() + " uncaughtException: " + err.message);
	console.error(err.stack);
	
	
});

process.on('ReferenceError', function(err){
	console.log('ReferenceError exception: ' + err);
})

process.on('exit', function() {
	console.log('About to exit.');
});


