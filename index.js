'use strict';

const debug = require('debug')('CommonDao');
const createCommonDao = require('common-dao');
const SqlString = require('sqlstring');
const async = require('async');
const TYPE_COLUMN = 1;
const TYPE_PK = 2;
const TYPE_INDEX = 3;

function* loadMetadata(client, table, caseSensitive){
  var pos = table.indexOf('.');
  var schema = null;
  if ( pos >= 0 ) {
    schema = table.substring(0, pos);
    table = table.substring(pos+1);
  }
  if ( !schema ) schema = client.config.database;
  var colQuery = 'select COLUMN_NAME from information_schema.columns where TABLE_SCHEMA=:schema and TABLE_NAME=:table';
  var idxQuery = 'select COLUMN_NAME, INDEX_NAME from information_schema.statistics where TABLE_SCHEMA=:schema and TABLE_NAME=:table';
  var params = {schema: schema, table: table};
  var colResult = yield function(cb){client.query(colQuery, params, cb)};
  var idxResult = yield function(cb){client.query(idxQuery, params, cb)};
  var columns = {};
  var columnsString = '';
  if ( colResult ) {
    colResult[0].forEach((row)=>{
      var col = row.COLUMN_NAME;
      if ( !caseSensitive ) col = col.toLowerCase();
      columns[col] = TYPE_COLUMN;
      if ( columnsString ) columnsString += ',';
      columnsString += col;
    })
  }
  if ( idxResult ) {
    idxResult[0].forEach((row)=>{
      var col = row.COLUMN_NAME;
      var type = row.INDEX_NAME;
      if ( !caseSensitive ) col = col.toLowerCase();
      if ( !columns[col] ) return;
      if ( 'PRIMARY' == type ) columns[col] = TYPE_PK;
      else columns[col] = TYPE_INDEX;
    });
  }
  return {columns: columns, columnsString: columnsString};
}


function queryFormat(query, values) {
	if ( !values ) return query;
	if ( values instanceof Array ) {
		return SqlString.format.apply(mysql, arguments);
	}
	return query.replace(/\:(\w+)/g, function (txt, key) {
		if (values.hasOwnProperty(key)) {
			return SqlString.escape(values[key]);
		}
		return txt;
	});
};

class MysqlDaoAdapter {
  constructor(table, client, options){
    this.table = table;
    this.options = options || {};
    if ( typeof client == 'function' ) {
      this.getConnection = client;
    }
    else {
      this.getConnection = function(){
        return client;
      }
    }
    var client = this.getConnection();
    if ( client.config.queryFormat != queryFormat ) {
      client.config.queryFormat = queryFormat;
    }
  }

  *ensureLoad(){
    if ( !this.tableInfo ) {
      this.tableInfo = yield loadMetadata(this.getConnection(), this.table, this.options.caseSensitive);
    }
  }

  isPrimaryKey(col){
    return this.tableInfo.columns[col] === TYPE_PK;
  }

  isIndex(col) {
    return this.tableInfo.columns[col] === TYPE_INDEX;
  }

  exists(col){
    return this.tableInfo.columns[col] != null;
  }

  selectColumns(columns){
		return columns ? "`"+columns.join("`,`")+"`" : this.tableInfo.columnsString;
	}

  columnsPair(columns){
		return columns.map((col)=>{
      return (col +'=:'+col);
    }).join(',');
	}

  wherePair(field, value){
    if ( value instanceof Array ){
      return field+' in :'+field;
    }
    else {
      return field+'=:'+field;
    }
  }

  execute(sql, params, options) {
    var client = this.getConnection();
    return function mysqlExecute(callback){
      client.query(sql, params, function(err, rows, fields){
  			if ( err ) {
  				return callback(err);
  			}
  			callback(err, rows, [rows, fields]);
  		});
    }
	}

  batch(scripts) {
    var client = this.getConnection();
    return function mysqlFakeBatch(callback){
      async.each(scripts, function(script, done){
        client.query(script[0], script[1], function(err, rows, fields){
    			if ( err ) {
    				return done(err);
    			}
    			done(null, rows);
    		});
      }, callback);
    }
  }
}

module.exports = function createMysqlDao(table, client, options){
  return createCommonDao(table, {
    adapter: new MysqlDaoAdapter(table, client)
  });
}
