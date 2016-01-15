// Sandstorm Blackrock
// Copyright (c) 2015-2016 Sandstorm Development Group, Inc.
// All Rights Reserved

Package.describe({
  summary: "Blackrock payments integrations",
  version: "0.1.0"
});

Npm.depends({
    'stripe': '3.6.0',
});

Package.onUse(function (api) {
  api.use("sandstorm-db", "server");
  api.use(["mongo", "reactive-var", "templating"], "client");

  api.addFiles([
    "billingSettings.html",
    "billingPrompt.html",
    "billingSettings.js",
    "billingPrompt.js",
    "payments-client.js"
  ], "client");
  api.addFiles(["payments-server.js",], "server");
  api.addFiles(["checkout.html", "sandstorm-purplecircle.png"], "server", {isAsset: true});

  api.export(["BlackrockPayments", "makePaymentsConnectHandler"]);
});

// TODO(test): tests
