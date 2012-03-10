/*jslint node: true, es5: true, nomen: true, indent: 2*/
/*global emit*/
"use strict";

var express = require('express');
var assert = require('assert');
var mongo = require('mongodb');
var crypto = require('crypto');
var fs = require('fs');
var doSync = require('sync');
var app = {};

// private functions
var _conf = {};
var _db = {};
var _def = function (v) {
  return (typeof v !== 'undefined');
};
var _exists = function (v) {
  return _def(v) && v !== null;
};
var _safe = function (v, d) {
  return _def(v) ? v : d;
};
var _secureMethod = function (m) {
  return function (req, res) {
    var s = req.header('x-datakit-secret', null);
    if (_exists(s) && s === _conf.secret) {
      return m(req, res);
    }
    res.header('WWW-Authenticate', 'datakit-secret');
    res.send(401);
  };
};
var _createRoutes = function (path) {
  var m = function (p) {
    return path + '/' + _safe(p, '');
  };
  app.get(m(), exports.info);
  app.get(m('public/:obj'), exports.public);
  app.post(m('publish'), _secureMethod(exports.publishObject));
  app.post(m('save'), _secureMethod(exports.saveObject));
  app.post(m('delete'), _secureMethod(exports.deleteObject));
  app.post(m('refresh'), _secureMethod(exports.refreshObject));
  app.post(m('query'), _secureMethod(exports.query));
  app.post(m('index'), _secureMethod(exports.index));
};
var _parseMongoException = function (e) {
  var lastErr = e.lastErrorObject;
  if (_exists(lastErr)) {
    return {'status': lastErr.code, 'message': lastErr.err};
  }
  return null;
};
var _e = function (res, snm, err) {
  var eo, me;
  eo = {'status': snm[0], 'message': snm[1]};
  me = _parseMongoException(err);
  if (me !== null) {
    eo.err = me.message;
  } else if (_exists(err)) {
    eo.err = String(err);
  }
  return res.json(eo, 400);
};
var _c = {
  red: '\u001b[31m',
  green: '\u001b[32m',
  yellow: '\u001b[33m',
  blue: '\u001b[34m',
  purple: '\u001b[34m',
  reset: '\u001b[0m'
};
var _ERR = {
  ENTITY_NOT_SET: [100, 'Entity not set'],
  ENTITY_KEY_NOT_SET: [101, 'Entity key not set'],
  OBJECT_ID_NOT_SET: [102, 'Object ID not set'],
  OBJECT_ID_INVALID: [103, 'Object ID invalid'],
  SAVE_FAILED: [200, 'Save failed'],
  SAVE_FAILED_DUPLICATE_KEY: [201, 'Save failed because of a duplicate key'],
  DELETE_FAILED: [300, 'Delete failed'],
  REFRESH_FAILED: [400, 'Refresh failed'],
  QUERY_FAILED: [500, 'Query failed'],
  INDEX_FAILED: [600, 'Index failed']
};
var _copyKeys = function (s, t) {
  var key;
  for (key in s) {
    if (s.hasOwnProperty(key)) {
      t[key] = s[key];
    }
  }
};
var _traverse = function (o, func) {
  var i;
  for (i in o) {
    if (o.hasOwnProperty(i)) {
      func.apply(o, [i, o[i]]);
      if (typeof (o[i]) === 'object') {
        _traverse(o[i], func);
      }
    }
  }
};
var _decodeDkObj = function (o) {
  _traverse(o, function (key, value) {
    if (key === 'dk:data') {
      this[key] = new Buffer(value, 'base64');
    }
  });
};
var _encodeDkObj = function (o) {
  _traverse(o, function (key, value) {
    if (key === 'dk:data') {
      this[key] = value.toString('base64');
    }
  });
};
var _generateNextSequenceNumber = function (entity) {
  var col, doc;
  col = _db.collection.sync(_db, 'DataKit:Sequence');
  col.insert.sync(col, {'_id': entity, 'seq': new mongo.Long(0)});
  doc = col.findAndModify.sync(
    col,
    {'_id': entity},
    [],
    {'$inc': {'seq': 1}},
    {new: true}
  );

  return doc.seq;
};
// exported functions
exports.run = function (c) {
  doSync(function runSync() {
    var pad, nl, buf, srv, db;
    pad = '--------------------------------------------------------------------------------';
    nl = '\n';
    console.log(nl + pad + nl + 'DATAKIT' + nl + pad);
    _conf.db = _safe(c.db, 'datakit');
    _conf.dbhost = _safe(c.dbhost, 'localhost');
    _conf.dbport = _safe(c.dbport, mongo.Connection.DEFAULT_PORT);
    _conf.path = _safe(c.path, '');
    _conf.port = _safe(c.port, process.env.PORT || 3000);
    _conf.secret = _safe(c.secret, null);
    _conf.cert = _safe(c.cert, null);
    _conf.key = _safe(c.key, null);
    _conf.express = _safe(c.express, function (app) {});

    if (_exists(_conf.cert) && _exists(_conf.key)) {
      app = express.createServer({
        'key': fs.readFileSync(_conf.key),
        'cert': fs.readFileSync(_conf.cert)
      });
    } else {
      app = express.createServer();
    }
    app.use(express.bodyParser());

    if (_conf.secret === null) {
      buf = crypto.randomBytes.sync(crypto, 32);
      _conf.secret = buf.toString('hex');
      console.log(_c.red + 'WARN:\tNo secret found in config, generated new one.\n',
                  '\tCopy this secret to your DataKit iOS app and server config!\n\n',
                  _c.yellow,
                  '\t' + _conf.secret, nl, nl,
                  _c.red,
                  '\tTerminating process.',
                  _c.reset);
      process.exit(1);
    }
    if (_conf.secret.length !== 64) {
      console.log(_c.red, '\nSecret is not a hex string of length 64 (256 bytes), terminating process.\n', _c.reset);
      process.exit(2);
    }

    console.log('CONF:', JSON.stringify(_conf, undefined, 2), nl);

    // Create API routes
    _createRoutes(_conf.path);
    _conf.express(app);

    // Connect to DB and run
    srv = new mongo.Server(_conf.dbhost, _conf.dbport, {});
    db = new mongo.Db(_conf.db, srv);
    try {
      _db = db.open.sync(db);
      app.listen(_conf.port, function appListen() {
        console.log(_c.green + 'DataKit started on port', _conf.port, _c.reset);
      });
    } catch (e) {
      console.error(e);
    }
  });
};
exports.info = function (req, res) {
  res.send('datakit', 200);
};
exports.public = function (req, res) {
  doSync(function publicSync() {
    var obj, objBuf, decipher, dec, c, entity, oid, fields, collection, result;
    obj = req.param('obj', null);
    if (_exists(obj)) {
      obj = obj.replace(/-/g, '+').replace(/_/g, '/');
      objBuf = new Buffer(obj, 'base64');
      decipher = crypto.createDecipher('aes-256-cbc', _conf.secret);
      dec = decipher.update(objBuf, 'binary', 'utf8');
      dec += decipher.final('utf8');

      if (_exists(dec) && dec.length > 0) {
        c = dec.split(':');
        if (c.length > 1) {
          entity = c[0];
          oid = null;
          try {
            oid = new mongo.ObjectID(c[1]);
          } catch (e) {
            // ignore invalid oid errors, check for null oid in next stepx
          }
          if (oid !== null && entity.length > 0) {
            fields = [];
            if (c.length > 2) {
              fields = c.splice(2, c.length - 1);
            }
            try {
              collection = _db.collection.sync(_db, entity);
              result = collection.findOne.sync(collection, {'_id': oid}, fields);
              if (_exists(result)) {
                delete result._id;
                // if only one field was requested we return the data of that field
                // directly instead of the JSON representation
                if (fields.length === 1) {
                  res.send(result[fields[0]], 200);
                } else {
                  res.json(result, 200);
                }
                return;
              }
            } catch (e2) {
              console.error(e2);
            }
          }
        }
      }
    }
    res.send(404);
  });
};
exports.publishObject = function (req, res) {
  doSync(function publishSync() {
    var entity, oid, fields, str, cipher, enc, base64, urlSafeBase64;
    entity = req.param('entity', null);
    if (!_exists(entity)) {
      return _e(res, _ERR.ENTITY_NOT_SET);
    }
    oid = req.param('oid', null);
    if (!_exists(oid)) {
      return _e(res, _ERR.OBJECT_ID_INVALID);
    }
    fields = req.param('fields', null);
    str = entity + ':' + oid;
    if (fields !== null && fields.length > 0) {
      str += ':' + fields.join(':');
    }
    cipher = crypto.createCipher('aes-256-cbc', _conf.secret);
    enc = cipher.update(str, 'utf8', 'hex');
    enc += cipher.final('hex');
    base64 = new Buffer(enc, 'hex').toString('base64');
    urlSafeBase64 = base64.replace(/\+/g, '-').replace(/\//g, '_');

    res.json(urlSafeBase64, 200);
  });
};
exports.saveObject = function (req, res) {
  doSync(function saveSync() {
    var i, entities, results, errors, ent, entity, oidStr, fset, funset, finc, fpush, fpushAll, faddToSet, fpop, fpullAll, oid, ts, collection, doc, isNew, opts, update, ats, key;
    entities = req.body;
    results = [];
    errors = [];

    for (i in entities) {
      if (entities.hasOwnProperty(i)) {
        ent = entities[i];
        entity = _safe(ent.entity, null);
        if (!_exists(entity)) {
          return _e(res, _ERR.ENTITY_NOT_SET);
        }
        oidStr = _safe(ent.oid, null);
        fset = _safe(ent.set, {});
        funset = _safe(ent.unset, null);
        finc = _safe(ent.inc, null);
        fpush = _safe(ent.push, null);
        fpushAll = _safe(ent.pushAll, null);
        faddToSet = _safe(ent.addToSet, null);
        fpop = _safe(ent.pop, null);
        fpullAll = _safe(ent.pullAll, null);
        oid = null;

        _decodeDkObj(fset);
        _decodeDkObj(fpush);
        _decodeDkObj(fpushAll);
        _decodeDkObj(faddToSet);
        _decodeDkObj(fpullAll);

        if (_exists(oidStr)) {
          oid = new mongo.ObjectID(oidStr);
          if (!_exists(oid)) {
            return _e(res, _ERR.OBJECT_ID_INVALID);
          }
        }
        try {
          ts = parseInt((new Date().getTime()) / 1000, 10);
          collection = _db.collection.sync(_db, entity);
          isNew = (oid === null);

          // Automatically insert the update timestamp
          fset._updated = ts;

          // Insert new object
          if (isNew) {
            // Generate new sequence number         
            fset._seq = _generateNextSequenceNumber(entity);
            doc = collection.insert.sync(collection, fset);
            oid = doc[0]._id;
          }

          // Update instead if oid exists, or an operation needs to be executed
          // that requires an insert first.
          opts = {'upsert': true, 'new': true};
          update = {};
          if (_exists(fset) && !isNew) {
            update.$set = fset;
          }
          if (_exists(funset)) {
            update.$unset = funset;
          }
          if (_exists(finc)) {
            update.$inc = finc;
          }
          if (_exists(fpush)) {
            update.$push = fpush;
          }
          if (_exists(fpushAll)) {
            update.$pushAll = fpushAll;
          }
          if (_exists(faddToSet)) {
            ats = {};
            for (key in faddToSet) {
              if (faddToSet.hasOwnProperty(key)) {
                ats[key] = {'$each': faddToSet[key]};
              }
            }
            update.$addToSet = ats;
          }
          if (_exists(fpop)) {
            update.$pop = fpop;
          }
          if (_exists(fpullAll)) {
            update.$pullAll = fpullAll;
          }

          // Find and modify
          if (!isNew || (isNew && Object.keys(update).length > 0)) {
            doc = collection.findAndModify.sync(collection, {'_id': oid}, [], update, opts);
          }

          if (doc.length > 0) {
            doc = doc[0];
          }

          _encodeDkObj(doc);

          results.push(doc);
        } catch (e) {
          console.error(e);
          errors.push(e);
        }
      }
    }
    if (errors.length > 0) {
      return _e(res, _ERR.SAVE_FAILED, errors.pop());
    }
    res.json(results, 200);
  });
};
exports.deleteObject = function (req, res) {
  doSync(function deleteSync() {
    var entity, oidStr, oid, collection, result;
    entity = req.param('entity', null);
    oidStr = req.param('oid', null);
    if (!_exists(entity)) {
      return _e(res, _ERR.ENTITY_NOT_SET);
    }
    if (!_exists(oidStr)) {
      return _e(res, _ERR.OBJECT_ID_NOT_SET);
    }
    oid = new mongo.ObjectID(oidStr);
    if (!_exists(oid)) {
      return _e(res, _ERR.OBJECT_ID_INVALID);
    }
    try {
      collection = _db.collection.sync(_db, entity);
      result = collection.remove.sync(collection, {'_id': oid}, {'safe': true});
      res.send('', 200);
    } catch (e) {
      console.error(e);
      return _e(res, _ERR.DELETE_FAILED, e);
    }
  });
};
exports.refreshObject = function (req, res) {
  doSync(function refreshSync() {
    var entity, oidStr, oid, collection, result;
    entity = req.param('entity', null);
    oidStr = req.param('oid', null);
    if (!_exists(entity)) {
      return _e(res, _ERR.ENTITY_NOT_SET);
    }
    if (!_exists(oidStr)) {
      return _e(res, _ERR.OBJECT_ID_NOT_SET);
    }
    oid = new mongo.ObjectID(oidStr);
    if (!_exists(oid)) {
      return _e(res, _ERR.OBJECT_ID_INVALID);
    }
    try {
      collection = _db.collection.sync(_db, entity);
      result = collection.findOne.sync(collection, {'_id': oid});
      if (!_exists(result)) {
        throw 'Could not find object';
      }

      _encodeDkObj(result);

      res.json(result, 200);
    } catch (e) {
      console.error(e);
      return _e(res, _ERR.REFRESH_FAILED, e);
    }
  });
};
exports.query = function (req, res) {
  doSync(function querySync() {
    var entity, doFindOne, doCount, mrand, query, opts, or, and, sort, skip, limit, sortValues, order, results, cursor, collection, result, key, resultCount;
    entity = req.param('entity', null);
    if (!_exists(entity)) {
      return _e(res, _ERR.ENTITY_NOT_SET);
    }
    doFindOne = req.param('findOne', false);
    doCount = req.param('count', false);
    mrand = req.param('mrand', 0);
    query = req.param('q', {});
    opts = {};
    or = req.param('or', null);
    and = req.param('and', null);
    sort = req.param('sort', null);
    skip = req.param('skip', null);
    limit = req.param('limit', null);

    if (_exists(or)) {
      query.$or = or;
    }
    if (_exists(and)) {
      query.$and = and;
    }
    if (_exists(sort)) {
      sortValues = [];
      for (key in sort) {
        if (sort.hasOwnProperty(key)) {
          order = (sort[key] === 1) ? 'asc' : 'desc';
          sortValues.push([key, order]);
        }
      }
      opts.sort = sortValues;
    }
    if (_exists(skip)) {
      opts.skip = parseInt(skip, 10);
    }
    if (_exists(limit)) {
      opts.limit = parseInt(limit, 10);
    }

    // replace oid strings with oid objects
    _traverse(query, function (key, value) {
      if (key === '_id') {
        this[key] = new mongo.ObjectID(value);
      }
    });

    try {
      // TODO: remove debug query log
      console.log('query', entity, '=>', JSON.stringify(query), JSON.stringify(opts));

      collection = _db.collection.sync(_db, entity);

      if (mrand > 0) {
        results = collection.mapReduce.sync(
          collection,
          function map() {
            emit(0, {k: this, v: Math.random()});
          },
          function reduce(k, v) {
            var a = [];
            v.forEach(function (x) {
              a = a.concat(x.a || x);
            });
            return {a: a.sort(function (a, b) {
              return a.v - b.v;
            }).slice(0, mrand)};
          },
          {
            'finalize': function finalize(k, v) {
              return v.a ? v.a.map(function (x) {
                return x.k;
              }) : [v.k];
            },
            'out': {'inline': 1},
            'scope': {'mrand': mrand}
          }
        );
        results = results[0].value;
      } else if (doFindOne) {
        result = collection.findOne.sync(collection, query, opts);
        results = [result];
      } else {
        cursor = collection.find.sync(collection, query, opts);

        if (doCount) {
          results = cursor.count.sync(cursor);
        } else {
          results = cursor.toArray.sync(cursor);
          resultCount = Object.keys(results).length;

          if (resultCount > 1000) {
            console.log(_c.yellow + 'warning: query',
                        entity,
                        '->',
                        query,
                        'returned',
                        resultCount,
                        'results, may impact server performance negatively. try to optimize the query!',
                        _c.reset);
          }
        }
      }

      _encodeDkObj(results);

      return res.json(results, 200);
    } catch (e) {
      console.error(e);
      return _e(res, _ERR.QUERY_FAILED, e);
    }
  });
};
exports.index = function (req, res) {
  doSync(function indexSync() {
    var entity, key, unique, drop, opts, collection, cursor;
    entity = req.param('entity', null);
    key = req.param('key', null);
    unique = req.param('unique', false);
    drop = req.param('drop', false);
    if (!_exists(entity)) {
      return _e(res, _ERR.ENTITY_NOT_SET);
    }
    if (!_exists(key)) {
      return _e(res, _ERR.ENTITY_KEY_NOT_SET);
    }
    try {
      opts = {
        'safe': true,
        'unique': unique,
        'dropDups': drop
      };
      collection = _db.collection.sync(_db, entity);
      cursor = collection.ensureIndex.sync(collection, {key: 1}, opts);

      return res.send('', 200);
    } catch (e) {
      return _e(res, _ERR.INDEX_FAILED, e);
    }
  });
};