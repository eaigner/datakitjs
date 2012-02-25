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
}
var _e = function(m, s) {
  return {"status": s, "message": m};
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
      return res.json(_e("Entity name not set", 100), 400);
    }
    if (!_exists(obj)) {
      return res.json(_e("Object data not set", 101), 400);
    }
    var oid = null;
    if (_exists(obj._id)) {
      oid = new mongo.ObjectID(obj._id);
      delete obj["_id"];
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
      res.json(_e("Could not save object", 102), 400);
    }
  })
}