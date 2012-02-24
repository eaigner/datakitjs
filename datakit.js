var express = require("express"),
    assert = require("assert"),
    Mongolian = require("mongolian"),
    app = express.createServer(),
    conf = {},
    dbServer = null,
    db = null;

// Install middleware
app.use(express.bodyParser());

// Exported functions
exports.run = function(c) {
  conf.db = dkSafeValue(c.db, "datakit");
  conf.path = dkSafeValue(c.path, "");
  conf.port = dkSafeValue(c.port, process.env.PORT || 3000);
  
  assert.notEqual(conf.db, null, "database connection string cannot be empty");
  
  // Create API routes
  dkCreateRoutes(conf.path);
  
  // Connect to DB
  dbServer = new Mongolian;
  db = dbServer.db(conf.db);
  
  // Run app
  app.listen(conf.port, function() {
    console.log("datakit started on port", conf.port);
  });
}

// Internal functions
var dkSaveObject = function(req, res) {
  var entity = req.param("entity", null);
  var obj = req.param("obj", null);
  
  if (entity == null) {
    return res.json(dkError("Entity name not set", 100), 400);
  }
  if (obj == null) {
    return res.json(dkError("Object data not set", 101), 400);
  }
  
  // Get collection
  var col = db.collection(entity).insert(obj, function(err, result) {
    if (err != null) {
      console.log("error:", e);
      res.json(dkError("Could not save object", 102), 400);
    }
    else {
      console.log("result =>", result);
      res.json(dkSanitizeDocId(result), 200);
    }
  });
}

var dkSanitizeDocId = function(obj) {
  if (obj != null && typeof obj._id !== "undefined") {
    if (typeof obj._id.bytes !== "undefined") {
      obj._id = obj._id.bytes.toString("hex");
    }
  }
  return obj;
}

var dkError = function(message, status) {
  return {"status": status, "message": message};
}

var dkSafeValue = function(value, def) {
  return (typeof value !== "undefined") ? value : def;
}

var dkCreateRoutes = function(path) {
  app.post(path + "/save", dkSaveObject);
}