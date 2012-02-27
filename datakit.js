var express = require("express"),
    assert = require("assert"),
    mongo = require("mongodb"),
    crypto = require("crypto"),
    doSync = require("sync"),
    app = express.createServer();

// Middleware
app.use(express.bodyParser());

// Private functions
var _conf = {};
var _db = {};
var _createRoutes = function(path) {
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
  return _def(v) && v != null;
}
var _safe = function(v, d) {
  return _def(v) ? v : d;
}

// Exported functions
exports.run = function(c) {
  doSync(function() {
    var pad = "----------------------------------------";
    var nl = "\n";
    console.log(nl + pad + nl + "DATAKIT" + nl + pad);
    _conf.db = _safe(c.db, "datakit");
    _conf.path = _safe(c.path, "");
    _conf.port = _safe(c.port, process.env.PORT || 3000);
    _conf.secret = _safe(c.secret, null);
    if (_conf.secret == null) {
      var buf = crypto.randomBytes.sync(crypto, 32);
      _conf.secret = buf.toString("hex");
      console.log("INFO: no secret found in config, generated new one");
    }
    if (_conf.secret.length !== 64) {
      throw "Secret is not a hex string of length 64 (256 bytes)"
    }

    console.log("CONF: =>", _conf);

    // Create API routes
    _createRoutes(_conf.path);

    // Connect to DB and run
    var srv = new mongo.Server("localhost", mongo.Connection.DEFAULT_PORT, {});
    var db = new mongo.Db(_conf.db, srv);
    try {
      _db = db.open.sync(db);
      app.listen(_conf.port, function() {
        console.log("datakit started on port", _conf.port);
      });
    }
    catch (e) {
      console.error(e);
    }
  });
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