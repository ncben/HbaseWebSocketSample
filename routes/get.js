module.exports = function(app, express, db, HBaseTypes, io){
	
	var _ = require('underscore');
	var orSeparator = '|^|OR|$|';
	var andSeparator = '|^|AND|$|';
	var comparatorRegex = /[!]|[=]|[<]|[>]/;
	var comparatorRegexExec = /(!=)|(=)|(<=)|(>=)|(<)|(>)/;
	var scanBatchSize = 10000;
	var batchEmitSize = 1;
	
	io.set('log level', 1);

	io.on('connection', function(socket){ 
		console.log('socket.io connected');
		socket.on('getMetric', function(data){ getMetric(data,socket) });
		socket.on('getMetricDetail', function(data){ getMetricDetail(data,socket) });
		socket.on('exportData', function(data){ exportData(data,socket) });
		socket.on('exportDataByKeys', function(data){ exportDataByKeys(data,socket) });
	});
	
	app.post('/saveExportData', function(req, res){
		
		if(!req.body || typeof req.body != 'object'){res.send(400);};
				
		var uuid = require('node-uuid').v4();
		var fs = require('fs');
				
		fs.writeFile(require("path").resolve(__dirname+'/../exports/'+uuid+'.csv'), Object.keys(req.body)[0], function (err) {
			if (err) throw err;
			
			res.json({file: uuid+'.csv'});
			setTimeout(function(){
				fs.unlink(require("path").resolve(__dirname+'/../exports/'+uuid+'.csv'), function (err) {
				if (err) throw err;
					console.log('successfully deleted export file');
				});
			}, 60000);
		});	
	
	})
	
	app.get('/getFile', function(req, res){
		
		if(!req.query.file){res.send(400);};
		
		var path = require("path");
		var mime = require("mime");
		var fs = require('fs');
		
		var file = path.resolve(__dirname+'/../exports')+'/' + req.query.file;
		
		fs.exists(file, function (exists) {
			if(exists){
				var filename = path.basename(file);
				var mimetype = mime.lookup(file);
				
				res.setHeader('Content-disposition', 'attachment; filename=' + filename);
				res.setHeader('Content-type', mimetype);
				
				var filestream = fs.createReadStream(file);
				filestream.pipe(res);
			}else{
				res.send(404);	
			}
		});
		
		
	
	})
	
	
	var exportDataByKeys = function(data,socket){
		
		if(!socket.handshake.headers.cookie || !require('cookie').parse(socket.handshake.headers.cookie).user){
		
			socket.emit('error', {error:'No cookies detected.'});
			return;
			
		}	
		
		var columns = {};	
		var Stream = require('stream');
		var stream = new Stream();
		
		var c=0;
		var a=[];
		
		stream.on('data', function(obj) {
			++c;
			if(obj.id)a.push({id: obj.id,  i: 0, l: 1,  v: obj.v});
			if(c==batchEmitSize){
				c=0;
				socket.emit('data', a);
				a=[];
			}
			
		})
		
		var getsArr = [];
		
		var colsArr = [new HBaseTypes.TColumn({family: 'formdata', qualifier: null}), new HBaseTypes.TColumn({family: 'formdatastats', qualifier: null})];
		
		var getMultiple = function(){
			
			if(typeof data.keys == 'object'){
			
				for(var i=0,l=data.keys.length;	i<l;i++){
					
					getsArr.push(new HBaseTypes.TGet({row:  data.pid+':'+data.keys[i].split(':')[1], columns: colsArr }));
					
				}
				
			}
			
			db.getMultiple('consumer-data', getsArr, function(err, scanData){
				
				if(err){
					console.log(err);
				
				}
											
				for(var r=0;r<scanData.length;r++){
										
					var entryCountCol = _.findWhere(scanData[r].columnValues, {qualifier: 'entrycount'});
					var bonusEntryCountCol = _.findWhere(scanData[r].columnValues, {qualifier: 'bonusentrycount'});
					
					if(entryCountCol){
					
						scanData[r].columnValues.push({family: 'formdatastats', qualifier: 'entrycount', value: parseInt(convertCharStr2CP(entryCountCol.value, 'none', true, 'dec').replace(/ /g,''),10)});
												
					}
					
					if(bonusEntryCountCol){
					
						scanData[r].columnValues.push({family: 'formdatastats', qualifier: 'bonusentrycount', value: parseInt(convertCharStr2CP(bonusEntryCountCol.value, 'none', true, 'dec').replace(/ /g,''),10)})
						
					}
																						
					if(scanData[r].columnValues[0].family == 'formdatastats' && (scanData[r].columnValues[0].qualifier == 'entrycount' || scanData[r].columnValues[0].qualifier == 'bonusentrycount')){
												
						
					}else{
					
						stream.emit('data', {id: scanData[r].row, v: scanData[r].columnValues})
						
					}
											
				}
									
				console.log('getMultiple complete');
				socket.emit('complete', {id: data.m, columns: columns});					
							
			})
			
		}
	
				
		db.get('promobuilder',  new HBaseTypes.TGet({row:  'promo:'+data.pid, columns: [new HBaseTypes.TColumn({family: 'pages', qualifier: null})] }), function(err,pagesData) {
																								
			for(var i in pagesData.columnValues){
				
				var key = pagesData.columnValues[i].qualifier;
								
				var itemKey = key.split(':')[3];

				if(itemKey == 'id'){
													
					var newKey = key.split(':');
					
					newKey.pop();								
					
					var label = _.findWhere(pagesData.columnValues, {qualifier: newKey.join(':')+':label'});
					
					if(label){
												
						columns[pagesData.columnValues[i].value] = {value: label.value, qualifier: pagesData.columnValues[i].qualifier};
					}
					
				} 
				
			}

			getMultiple();
			
			
		})
	
		
	}

	
	var exportData = function(data,socket){
					console.log('exportData');
		if(!socket.handshake.headers.cookie || !require('cookie').parse(socket.handshake.headers.cookie).user){
		
			socket.emit('error', {error:'No cookies detected.'});
			return;
			
		}	

		if(typeof data.conditions != 'object' ){
			
			data.conditions = [];
			
		}	
		
		var columnsMetricComparatorSymbol = function(text){
		
			var conversion = {
			 'is': '=;binary;',
			 'is not': '!=;binary;',
			 'contains' : '=;substring;',
			 'not contain' : '!=;substring;',
			 'greater than' : '>;binary;',
			 'greater/equal' : '>=;binary;',
			 'less than' : '<;binary;',
			 'less/equal' : '<=;binary;',
			 'starts with' : '=;binaryprefix;',
			 'not start with' : '!=;binaryprefix;',
			 'regex match' : '=;regexstring;',
			 'regex not match' : '!=;regexstring;'
			
			};
			
			return conversion[text] || '';
			 
		 }
		 
		 var andConditions = [];
		 
		 for(var c in data.conditions){			 
			
			if(typeof data.conditions[c] == 'object'){
				
				var orConditions = [];
		
				for(var d in data.conditions[c]){
				
					if(!data.conditions[c][d].columnsMetric || !data.conditions[c][d].columnsMetricComparator || !columnsMetricComparatorSymbol(data.conditions[c][d].columnsMetricComparator)){
					
						res.json(200, {error:'One or more of your metric conditions is missing or is invalid.'});
						return;	
						
					}
					
					if(typeof data.conditions[c][d].typeValue == 'undefined') data.conditions[c][d].typeValue='';
					
					if(data.conditions[c][d].typeValue.indexOf(orSeparator) !== -1 || data.conditions[c][d].typeValue.indexOf(andSeparator) !== -1){
					
						res.json(200, {error:'One or more metric value is invalid.'});
						return;	
						
					}
					
					if(typeof data.conditions[c][d].typeValue == 'object'){
						
						if(data.conditions[c][d].columnsMetricComparator == 'is'){
						
							for(var i in data.conditions[c][d].typeValue){
							
								orConditions.push(data.conditions[c][d].columnsMetric+columnsMetricComparatorSymbol(data.conditions[c][d].columnsMetricComparator)+data.conditions[c][d].typeValue[i].trim());
								
							}
						
						}else{
							
							for(var i in data.conditions[c][d].typeValue){
							
								andConditions.push([data.conditions[c][d].columnsMetric+columnsMetricComparatorSymbol(data.conditions[c][d].columnsMetricComparator)+data.conditions[c][d].typeValue[i].trim()]);
								
							}
							
						}
						
					}else{
					
						orConditions.push(data.conditions[c][d].columnsMetric+columnsMetricComparatorSymbol(data.conditions[c][d].columnsMetricComparator)+data.conditions[c][d].typeValue.trim());
						
					}
					
					
				}
				
				orConditions = orConditions.join(orSeparator)
				andConditions.push(orConditions);
			}
			
			
		}
		
		andConditions = andConditions.join(andSeparator);
		
		data.conditions = andConditions;
	
		var mainConditions = data.conditions.split(andSeparator);
				
		var conditionsObj = getConditionObj(mainConditions);
										
		var columns = {};	
			
		var Stream = require('stream');
		var stream = new Stream();
		
		var c=0;
		var a=[];
		
		stream.on('data', function(obj) {
			++c;
			if(obj.id)a.push({id: obj.id, i: obj.filterIndex, l: obj.l, v: obj.v});
			if(c==batchEmitSize){
				c=0;
				socket.emit('data', a);
				a=[];
			}
		})
		
		stream.on('count', function(count) {
			socket.emit('count', {m: data.m, count: count});
		})
	
		var scannerGetList = function(scanId, filterIndex, isLastFilter,initScan,i,allConditionsArrayLength){
						  
			db.getScannerRows(scanId, scanBatchSize, function(err, scanData){
				
				if(err){
					console.log(err);
				
				}
					console.log('scan: '+scanData.length);
				for(var r=0, l=scanData.length;r<l;r++){
										
					var entryCountCol = _.findWhere(scanData[r].columnValues, {qualifier: 'entrycount'});
					var bonusEntryCountCol = _.findWhere(scanData[r].columnValues, {qualifier: 'bonusentrycount'});
					
					if(entryCountCol){
					
						scanData[r].columnValues.push({family: 'formdatastats', qualifier: 'entrycount', value: parseInt(convertCharStr2CP(entryCountCol.value, 'none', true, 'dec').replace(/ /g,''),10)});
												
					}
					
					if(bonusEntryCountCol){
					
						scanData[r].columnValues.push({family: 'formdatastats', qualifier: 'bonusentrycount', value: parseInt(convertCharStr2CP(bonusEntryCountCol.value, 'none', true, 'dec').replace(/ /g,''),10)})
						
					}
																						
					if(scanData[r].columnValues[0].family == 'formdatastats' && (scanData[r].columnValues[0].qualifier == 'entrycount' || scanData[r].columnValues[0].qualifier == 'bonusentrycount')){
												
						scanData[r].columnValues[0].value = parseInt(convertCharStr2CP(scanData[r].columnValues[0].value, 'none', true, 'dec').replace(/ /g,''),10);
						
					}else{
					
						stream.emit('data', {filterIndex: filterIndex, id: scanData[r].row, v: scanData[r].columnValues})
						
					}
											
				}
					
				if(scanData.length == 0 && isLastFilter){
					
					console.log('scan complete');
					socket.emit('complete', {id: data.m, columns: columns});
					console.log('socket complete emitted');
					return;
					
				}
				if(scanData.length == 0 && !isLastFilter){
									
					db.closeScanner(scanId);
					return initScan(++i);	
					
				}
				
				scannerGetList(scanId, filterIndex, isLastFilter,initScan,i,allConditionsArrayLength);
				
			});
			
		};
				
		db.get('promobuilder',  new HBaseTypes.TGet({row:  'promo:'+data.pid, columns: [new HBaseTypes.TColumn({family: 'pages', qualifier: null})] }), function(err,pagesData) {
																								
			for(var i in pagesData.columnValues){
				
				var key = pagesData.columnValues[i].qualifier;
								
				var itemKey = key.split(':')[3];

				if(itemKey == 'id'){
													
					var newKey = key.split(':');
					
					newKey.pop();								
					
					var label = _.findWhere(pagesData.columnValues, {qualifier: newKey.join(':')+':label'});
					
					if(label){
												
						columns[pagesData.columnValues[i].value] = {value: label.value, qualifier: pagesData.columnValues[i].qualifier};
					}
					
				} 
				
			}

			tScan(conditionsObj, {query: data, withData: true}, null, scannerGetList, socket);
			
			
		})
	
		
	}

	
	var getMetric = function(data,socket){
		
		if(!socket.handshake.headers.cookie || !require('cookie').parse(socket.handshake.headers.cookie).user){
		
			socket.emit('error', {error:'No cookies detected.'});
			return;
			
		}	

		if(!data.conditions){
		
			socket.emit('error', {error:'No conditions requested.'});
			return;
			
		}	
		
		var mainConditions = data.conditions.split(andSeparator);
				
		var conditionsObj = getConditionObj(mainConditions);
				
		var dataLength = {};
		
		dataLength[data.m] = 0;
	
		var Stream = require('stream');
		var stream = new Stream();
		
		stream.on('data', function(obj) {
			if(obj.id)socket.emit('data', {m: data.m, id: obj.id, filterIndex: obj.filterIndex, l: obj.l});
		})
		
		stream.on('count', function(count) {
			socket.emit('count', {m: data.m, count: count});
		})
	
		var scannerGetList = function(scanId, filterIndex, isLastFilter,initScan,i,allConditionsArrayLength){
			
			var isSingleFilter = (allConditionsArrayLength === 1);
			  
			db.getScannerRows(scanId, scanBatchSize, function(err, scanData){
				
				if(err){
					console.log(err);
				
				}
				
				if(!isSingleFilter){
											
					for(var r=0;r<scanData.length;r++){
							
						stream.emit('data', {filterIndex: filterIndex, id: scanData[r].row, l: allConditionsArrayLength})
												
					}
				
				}else{
										
					dataLength[data.m] += scanData.length;
					stream.emit('count', dataLength[data.m])
					
				}
									
				if(scanData.length == 0 && isLastFilter){
					
					console.log('scan complete');
					socket.emit('complete', {id: data.m});
					return;
					
				}
				if(scanData.length == 0 && !isLastFilter){
									
					db.closeScanner(scanId);
					return initScan(++i);	
					
				}
				
				scannerGetList(scanId, filterIndex, isLastFilter,initScan,i,allConditionsArrayLength);
				
			});
			
		};
												
		tScan(conditionsObj, {query: data}, null, scannerGetList, socket);
	
		
	}

	
	var getMetricDetail = function(data, socket){
		
		if(!socket.handshake.headers.cookie || !require('cookie').parse(socket.handshake.headers.cookie).user){
		
			socket.emit('charterror', {error:'No cookies detected.'});
			return;
			
		}	
		
		if(!data.conditions){
		
			socket.emit('charterror', {error:'No conditions requested.'});
			return;
			
		}	
		
		var mainConditions = data.conditions.split(andSeparator);
				
		var conditionsObj = getConditionObj(mainConditions);
		
		var jsonData = {};
		jsonData.unique = [];
		jsonData.complete = [];
		
		var dateKeys = {};
		
		var Stream = require('stream');
		var stream = new Stream();
		
		stream.on('chartdata', function(obj) {
			if(obj.id)socket.emit('chartdata', {m: data.m, id: obj.id, t: obj.timestamp, filterIndex: obj.filterIndex, l: obj.l});
		})
		
		var scannerGetList = function(scanId, filterIndex, isLastFilter,initScan,i,allConditionsArrayLength){
			
			var isSingleFilter = (allConditionsArrayLength === 1);
			  
			db.getScannerRows(scanId, scanBatchSize, function(err, scanData){
				
				if(err){
					console.log(err);
				
				}
							
				scanData.forEach(function(element, index, array){

					for(var e in element.columnValues){
												
						var decoder = new (require('string_decoder').StringDecoder)('utf-8');
						element.columnValues[e].timestamp = parseInt(element.columnValues[e].timestamp.toString());
						
						var timestamp = new Date(element.columnValues[e].timestamp);
						
						var key = new Date(timestamp.getFullYear(), timestamp.getMonth(), timestamp.getDate()).getTime();
						
						if(!isSingleFilter){
																				
							stream.emit('chartdata', {filterIndex: filterIndex, timestamp: key, id: element.row, l: allConditionsArrayLength})
																					
						}else{
																											
							if(typeof dateKeys[key] == 'undefined')dateKeys[key] = {unique: 0, complete: 0};
							dateKeys[key].complete++;
							if(i==0)dateKeys[key].unique++;
							
						}

					}
					
						
				})
				
									
				if(scanData.length == 0 && isLastFilter){
					
					for(d in dateKeys){
						
						d=parseInt(d);
					
						jsonData.unique.push([d, dateKeys[d].unique]);
						jsonData.complete.push([d, dateKeys[d].complete])	
						
					}
					if(isSingleFilter)socket.emit('chart', jsonData);
						else socket.emit('chart', {multiFilter: true})
					return;
					
				}
				if(scanData.length == 0 && !isLastFilter){
																
					db.closeScanner(scanId);
					return initScan(++i);	
					
				}
				
				scannerGetList(scanId, filterIndex, isLastFilter,initScan,i,allConditionsArrayLength);
				
			});	
			
		};
												
		tScan(conditionsObj, {query: data}, null, scannerGetList, socket);
	
								
	};
	
	
	
	/**** FUNCTIONS ****/
	
	function setHeader(res){
		
		res.header('Access-Control-Allow-Credentials', true);
		res.header('Access-Control-Allow-Origin',    /*  req.headers.origin || */ 'https://promocms.dja.com');
		res.header('Access-Control-Allow-Methods',     'GET');
		res.header('Access-Control-Allow-Headers',     'X-Requested-With, X-HTTP-Method-Override, Content-Type, Accept');
			
	}
	
	function tScan(conditionsObj, req, res, scannerGetList, socket){
			
		var allConditionsArray = [];
		
		for(var c in conditionsObj){
			
			var mainConditionFilterArray = [];
			
			for(var i in conditionsObj[c]){
				
				var comparator = conditionsObj[c][i].comparator;
				
				var comparatorType = conditionsObj[c][i].comparatorType || 'binary';
				
				if(!comparatorType || !comparator){
					var error = {error: 'Filter incorrectly constructed.'};
					if(socket) socket.emit('error', error);
						else res.json(400, error);
					return;
				}
				
				if(conditionsObj[c][i].colPrefix == 'Platform'){
				
					var subConditionFilterString = "(QualifierFilter(=, 'substring::platform:') AND ValueFilter("+comparator+", '"+comparatorType+":"+conditionsObj[c][i].fieldValue+"'))";
					
				}else{
					
					var subConditionFilterString = "(ColumnPrefixFilter('"+conditionsObj[c][i].colPrefix+"') AND ValueFilter("+comparator+", '"+comparatorType+":"+conditionsObj[c][i].fieldValue+"'))";
				
				}
				
				mainConditionFilterArray.push(subConditionFilterString);
				
			}
			
			var mainConditionFilterString = '('+mainConditionFilterArray.join(' OR ')+')';
			
			allConditionsArray.push(mainConditionFilterString);
			
		}		 
		 
		 
		
		var allConditionsString = '('+ allConditionsArray.join(' OR ') + ')';
						
		var initScan = function(i){
			
			var allConditionsString = allConditionsArray[i] || "";
						
			var flt = "PrefixFilter('"+req.query.pid+"') " +(!req.withData ? " AND KeyOnlyFilter() " : " AND (FamilyFilter(=, 'binary:formdata') OR FamilyFilter(=, 'binary:formdatastats')) ") + (allConditionsString ? " AND " + allConditionsString : "");	
																	
			var scanVars = {"columns" : null, "filterString" : flt,  "batchSize": scanBatchSize, "caching": scanBatchSize};
			
			if(req.query.start > 0 && req.query.end > 0){
				scanVars.timeRange = new HBaseTypes.TTimeRange({minStamp: parseInt(req.query.start || 0), maxStamp: parseInt(req.query.end || 4102358400000)});	
			}
																				
			var scan = new HBaseTypes.TScan(scanVars);
			
			var lastFilter = ((i+1) === allConditionsArray.length) || allConditionsArray.length == 0;
			
			db.openScanner('consumer-data', scan, function(err,scanId) {
									
				if(err){
					console.log(err);
					
				}
				if(scanId){
																	
					scannerGetList(scanId, i, lastFilter, initScan, i, allConditionsArray.length);					
					
				}else{
				
				}
				
				
			})
			
		}
		
		initScan(0);
		

			
	}
	
	function getConditionObj(mainConditions){
		
		var conditionsObj = [];
	
		for(var m in mainConditions){
				
			var orConditions = mainConditions[m].split(orSeparator);
			
			var subConditions = [];
			
			for(var o in orConditions){
				
				if(!orConditions[o])continue;
				
				var fieldId = orConditions[o].split(comparatorRegex).filter(Boolean)[0];
				var comparator = comparatorRegexExec.exec(orConditions[o])[0];
								
				var comparatorType = orConditions[o].replace(fieldId+comparator+';', '').split(';')[0];
				var fieldValue = orConditions[o].replace(fieldId+comparator+';'+comparatorType+';', '');
				
				var colKeyPrefix = fieldId;
								
				subConditions.push({fieldValue: fieldValue, comparator: comparator, comparatorType: comparatorType, colPrefix: colKeyPrefix});
				
			}
			
			if(subConditions.length >0)conditionsObj.push(subConditions)
			
			
		}	
		
		return conditionsObj;
	
	}
		

}






var  convertCharStr2CP  = function( textString, preserve, pad, type ) { 
	// converts a string of characters to code points, separated by space
	// textString: string, the string to convert
	// preserve: string enum [ascii, latin1], a set of characters to not convert
	// pad: boolean, if true, hex numbers lower than 1000 are padded with zeros
	// type: string enum[hex, dec, unicode, zerox], whether output should be in hex or dec or unicode U+ form
	var haut = 0;
	var n = 0;
	var CPstring = '';
	var afterEscape = false;
	for (var i = 0; i < textString.length; i++) {
		var b = textString.charCodeAt(i); 
		if (b < 0 || b > 0xFFFF) {
			CPstring += 'Error in convertChar2CP: byte out of range ' + dec2hex(b) + '!';
			}
		if (haut != 0) {
			if (0xDC00 <= b && b <= 0xDFFF) { //alert('12345'.slice(-1).match(/[A-Fa-f0-9]/)+'<');
				//if (CPstring.slice(-1).match(/[A-Za-z0-9]/) != null) { CPstring += ' '; }
				if (afterEscape) { CPstring += ' '; }
				if (type == 'hex') { 
					CPstring += dec2hex(0x10000 + ((haut - 0xD800) << 10) + (b - 0xDC00)); 
					}
				else if (type == 'unicode') { 
					CPstring += 'U+'+dec2hex(0x10000 + ((haut - 0xD800) << 10) + (b - 0xDC00)); 
					}
				else if (type == 'zerox') { 
					CPstring += '0x'+dec2hex(0x10000 + ((haut - 0xD800) << 10) + (b - 0xDC00)); 
					}
				else { 
					CPstring += 0x10000 + ((haut - 0xD800) << 10) + (b - 0xDC00); 
					}
				haut = 0;
				continue;
				afterEscape = true;
				}
			else {
				CPstring += 'Error in convertChar2CP: surrogate out of range ' + dec2hex(haut) + '!';
				haut = 0;
				}
			}
		if (0xD800 <= b && b <= 0xDBFF) {
			haut = b;
			}
		else {
			if (b <= 127 && preserve == 'ascii') {
				CPstring += textString.charAt(i);
				afterEscape = false;
				}
			else if (b <= 255 && preserve == 'latin1') {
				CPstring += textString.charAt(i);
				afterEscape = false;
				}
			else { 
				//if (CPstring.slice(-1).match(/[A-Za-z0-9]/) != null) { CPstring += ' '; }
				if (afterEscape) { CPstring += ' '; }
				if (type == 'hex') { 
					cp = dec2hex(b); 
					if (pad) { while (cp.length < 4) { cp = '0'+cp; } }
					}
				else if (type == 'unicode') { 
					cp = dec2hex(b); 
					if (pad) { while (cp.length < 4) { cp = '0'+cp; } }
					CPstring += 'U+'; 
					}
				else if (type == 'zerox') { 
					cp = dec2hex(b); 
					if (pad) { while (cp.length < 4) { cp = '0'+cp; } }
					CPstring += '0x'; 
					}
				else { 
					cp = b;
					}
				CPstring += cp; 
				afterEscape = true;
				}
			}
		}
	//return CPstring.substring(0, CPstring.length-1);
	return CPstring;
	}
	
	