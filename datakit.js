var express = require("express"),
    assert = require("assert"),
    mongo = require("mongodb"),
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
  OBJECT_NOT_SET: [101, "Object not set"],
  OBJECT_ID_NOT_SET: [102, "Object ID not set"],
  OBJECT_ID_INVALID: [103, "Object ID invalid"],
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
    _conf.db = _safe(c.db, "datakit");
    _conf.path = _safe(c.path, "");
    _conf.port = _safe(c.port, process.env.PORT || 3000);

    console.log("conf =>", _conf);

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
    var obj = req.param("obj", null);
    if (!_exists(entity)) {
      return _e(res, _ERR.ENTITY_NOT_SET);
    }
    if (!_exists(obj)) {
      return _e(res, _ERR.OBJECT_NOT_SET);
    }
    var oid = null;
    if (_exists(obj._id)) {
      oid = new mongo.ObjectID(obj._id);
      delete obj["_id"];
      if (!_exists(oid)) {
        return _e(res, _ERR.OBJECT_ID_INVALID);
      }
    }
    try {
      var collection = _db.collection.sync(_db, entity);
      var doc;
      if (oid !== null) {
        var opts = {"upsert": true, "new": true};
        doc = collection.findAndModify.sync(collection, {"_id": oid}, [], {"$set": obj}, opts);
      }
      else {
        doc = collection.insert.sync(collection, obj);
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
      console.log("refresh result =>", result);
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