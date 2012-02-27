var express = require("express"),
    assert = require("assert"),
    mongo = require("mongodb"),
    crypto = require("crypto"),
    fs = require("fs"),
    doSync = require("sync"),
    app = {};

// Private functions
var _conf = {};
var _db = {};
var _createRoutes = function(path) {
  app.get(path + "/", exports.info);
  app.post(path + "/save", exports.saveObject);
  app.post(path + "/delete", exports.deleteObject);
  app.post(path + "/refresh", exports.refreshObject);
}
var _e = function(res, snm, err) {
  var eo = {"status": snm[0], "message": snm[1]};
  if (_exists(err)) {
    eo.err = String(err);
  }
  return res.json(eo, 400);
}
var _c = {
  red: "\u001b[31m",
  green: "\u001b[32m",
  yellow: "\u001b[33m",
  blue: "\u001b[34m",
  purple: "\u001b[34m",
  reset: "\u001b[0m"
}
var _ERR = {
  ENTITY_NOT_SET: [100, "Entity not set"],
  OBJECT_ID_NOT_SET: [101, "Object ID not set"],
  OBJECT_ID_INVALID: [102, "Object ID invalid"],
  SAVE_FAILED: [200, "Save failed"],
  DELETE_FAILED: [300, "Delete failed"],
  REFRESH_FAILED: [400, "Refresh failed"]
}
var _def = function(v) {
  return (typeof v !== "undefined");
}
var _exists = function(v) {
  return _def(v) && v !== null;
}
var _safe = function(v, d) {
  return _def(v) ? v : d;
}

// Exported functions
exports.run = function(c) {
  doSync(function() {
    var pad = "--------------------------------------------------------------------------------";
    var nl = "\n";
    console.log(nl + pad + nl + "DATAKIT" + nl + pad);
    _conf.db = _safe(c.db, "datakit");
    _conf.dbhost = _safe(c.dbhost, "localhost");
    _conf.dbport = _safe(c.dbport, mongo.Connection.DEFAULT_PORT);
    _conf.path = _safe(c.path, "");
    _conf.port = _safe(c.port, process.env.PORT || 3000);
    _conf.secret = _safe(c.secret, null);
    _conf.cert = _safe(c.cert, null);
    _conf.key = _safe(c.key, null);
    
    if (_exists(_conf.cert) && _exists(_conf.key)) {
      app = express.createServer({
        "key": fs.readFileSync(_conf.key),
        "cert": fs.readFileSync(_conf.cert)
      });
      console.log("SSL Secured")
    }
    else {
      app = express.createServer()
    }
    app.use(express.bodyParser());
    
    if (_conf.secret == null) {
      var buf = crypto.randomBytes.sync(crypto, 32);
      _conf.secret = buf.toString("hex");
      console.log(_c.red + "WARN:\tNo secret found in config, generated new one.\n",
                  "\tCopy this secret to your DataKit iOS app and server config!\n\n",
                  _c.yellow,
                  "\t" + _conf.secret, nl, nl,
                  _c.red,
                  "\tTerminating process.",
                  _c.reset);
      process.exit(code=1);
    }
    if (_conf.secret.length !== 64) {
      console.log(_c.red, "\nSecret is not a hex string of length 64 (256 bytes), terminating process.\n", _c.reset);
      process.exit(code=2);
    }

    console.log("CONF:", JSON.stringify(_conf, undefined, 2), nl);

    // Create API routes
    _createRoutes(_conf.path);

    // Connect to DB and run
    var srv = new mongo.Server(_conf.dbhost, _conf.dbport, {});
    var db = new mongo.Db(_conf.db, srv);
    try {
      _db = db.open.sync(db);
      app.listen(_conf.port, function() {
        console.log(_c.green + "DataKit started on port", _conf.port, _c.reset);
      });
    }
    catch (e) {
      console.error(e);
    }
  });
}
exports.info = function(req, res) {
  res.send("datakit", 200);
}
exports.saveObject = function(req, res) {
  doSync(function() {
    var entity = req.param("entity", null);
    if (!_exists(entity)) {
      return _e(res, _ERR.ENTITY_NOT_SET);
    }
    var oidStr = req.param("oid", null);
    var fset = req.param("set", null);
    var funset = req.param("unset", null);
    var oid = null;
    if (_exists(oidStr)) {
      oid = new mongo.ObjectID(oidStr);
      if (!_exists(oid)) {
        return _e(res, _ERR.OBJECT_ID_INVALID);
      }
    }
    try {
      var collection = _db.collection.sync(_db, entity);
      var doc;
      if (oid !== null) {
        var opts = {"upsert": true, "new": true};
        var update = {};
        if (_exists(fset)) update["$set"] = fset;
        if (_exists(funset)) update["$unset"] = funset;
        doc = collection.findAndModify.sync(collection, {"_id": oid}, [], update, opts);
      }
      else {
        doc = collection.insert.sync(collection, fset);
      }
      if (doc.length > 0) {
        doc = doc[0];
      }
      res.json(doc, 200);
    }
    catch (e) {
      console.error(e);
      return _e(res, _ERR.SAVE_FAILED, e);
    }
  })
}
exports.deleteObject = function(req, res) {
  doSync(function() {
    var entity = req.param("entity", null);
    var oidStr = req.param("oid", null);
    if (!_exists(entity)) {
      return _e(res, _ERR.ENTITY_NOT_SET);
    }
    if (!_exists(oidStr)) {
      return _e(res, _ERR.OBJECT_ID_NOT_SET);
    }
    var oid = new mongo.ObjectID(oidStr);
    if (!_exists(oid)) {
      return _e(res, _ERR.OBJECT_ID_INVALID);
    }
    try {
      var collection = _db.collection.sync(_db, entity);
      var result = collection.remove.sync(collection, {"_id": oid}, {"safe": true});
      res.send('', 200);
    }
    catch (e) {
      console.error(e);
      return _e(res, _ERR.DELETE_FAILED, e);
    }
  })
}
exports.refreshObject = function(req, res) {
  doSync(function() {
    var entity = req.param("entity", null);
    var oidStr = req.param("oid", null);
    if (!_exists(entity)) {
      return _e(res, _ERR.ENTITY_NOT_SET);
    }
    if (!_exists(oidStr)) {
      return _e(res, _ERR.OBJECT_ID_NOT_SET);
    }
    var oid = new mongo.ObjectID(oidStr);
    if (!_exists(oid)) {
      return _e(res, _ERR.OBJECT_ID_INVALID);
    }
    try {
      var collection = _db.collection.sync(_db, entity);
      var result = collection.findOne.sync(collection, {"_id": oid});
      if (!_exists(result)) {
        throw "Could not find object";
      }
      res.send(result, 200);
    }
    catch (e) {
      console.error(e);
      return _e(res, _ERR.REFRESH_FAILED, e);
    }
  })
}