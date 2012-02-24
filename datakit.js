var express = require("express"),
    mongoose = require("mongoose"),
    assert = require("assert"),
    app = express.createServer();

// install middleware
app.use(express.bodyParser());

exports.run = function(config) {
  var db = dkSafeValue(config.db, null);
  var endpoint = dkSafeValue(config.endpoint, "");
  var port = dkSafeValue(config.port, process.env.PORT || 3000);
  assert(db !== null);
  
  // connect mongoose
  mongoose.connect(config.db);
  
  // create API routes
  dkCreateRoutes(endpoint);
  
  // run app
  app.listen(port, function() {
    console.log("datakit started on port", port);
  });
}

var dkCreateObject = function(req, res) {
  console.log("createObject");
}

var dkSafeValue = function(value, def) {
  return (typeof value !== "undefined") ? value : def;
}

var dkCreateRoutes = function(endpoint) {
  app.post(endpoint + "/create", dkCreateObject);
}