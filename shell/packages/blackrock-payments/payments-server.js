// Sandstorm - Personal Cloud Sandbox
// Copyright (c) 2015 Sandstorm Development Group, Inc. and contributors
// All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

var Crypto = Npm.require("crypto");
var HOSTNAME = process.env.ROOT_URL;
var stripe = Npm.require("stripe")(Meteor.settings.stripe_key);

BlackrockPayments = function (db) {
  this.db = db;
  this.methods = {
    addCardForUser: this.addCardForUser.bind(this),
    deleteCardForUser: this.deleteCardForUser.bind(this),
    makeCardPrimary: this.makeCardPrimary.bind(this),
    getStripeData: this.getStripeData.bind(this),
    updateUserSubscription: this.updateUserSubscription.bind(this),
    createUserSubscription: this.createUserSubscription.bind(this),
  };
}

var serveCheckout = Meteor.bindEnvironment(function (res) {
  res.writeHead(200, { "Content-Type": "text/html" });
  res.end(Assets.getText("checkout.html").replace("$STRIPE_KEY", Meteor.settings.public.stripe_public_key));
});

var serveSandcat = Meteor.bindEnvironment(function (res) {
  res.writeHead(200, { "Content-Type": "image/png" });
  // Meteor's buffer isn't a real buffer, so we have to do a copy
  res.end(new Buffer(Assets.getBinary("sandstorm-purplecircle.png")));
});

function hashId(id) {
  return Crypto.createHash("sha256").update(HOSTNAME + ":" + id).digest("base64");
}

function findOriginalId(hashedId, customerId) {
  var data = Meteor.wrapAsync(stripe.customers.retrieve.bind(stripe.customers))(customerId);
  if (data.sources && data.sources.data) {
    var sources = data.sources.data;
    for (var i = 0; i < sources.length; i++) {
      if (hashId(sources[i].id) === hashedId) {
        return sources[i].id;
      }
    }
  }

  throw new Meteor.Error(400, "Id not found");
}

BlackrockPayments.prototype.connectHandler = function (req, res, next) {
  if (req.headers.host == this.db.makeWildcardHost("payments")) {
    if (req.url == "/checkout") {
      serveCheckout(res);
    } else if (req.url == "/sandstorm-purplecircle.png") {
      serveSandcat(res);
    } else {
      res.writeHead(404, { "Content-Type": "text/plain" });
      res.end("404 not found: " + req.url);
    }
  } else {
    next();
  }
};

BlackrockPayments.prototype._createUser = function (token, email) {
  var data = Meteor.wrapAsync(stripe.customers.create.bind(stripe.customers))({
    source: token,
    email: email,
    description: Meteor.userId()  // TODO(soon): Do we want to store backrefs to our database in stripe?
  });
  Meteor.users.update({_id: Meteor.userId()}, {$set: {payments: {id: data.id}}});
  return data.id;
}

BlackrockPayments.prototype.addCardForUser = function (token, email) {
  if (!Meteor.userId()) {
    throw new Meteor.Error(403, "Must be logged in to add card");
  }
  check(token, String);
  check(email, String);

  var user = Meteor.user();

  if (user.payments && user.payments.id) {
    Meteor.wrapAsync(stripe.customers.createSource.bind(stripe.customers))(
      user.payments.id,
      {source: token}
    );
  } else {
    this._createUser(token, email);
  }
};

BlackrockPayments.prototype.deleteCardForUser = function (id) {
  if (!Meteor.userId()) {
    throw new Meteor.Error(403, "Must be logged in to delete card");
  }
  check(id, String);

  var customerId = Meteor.user().payments.id;
  var data = Meteor.wrapAsync(stripe.customers.retrieve.bind(stripe.customers))(customerId);
  if (data.sources && data.sources.data && data.subscriptions && data.subscriptions.data) {
    var sources = data.sources.data;
    var subscriptions = data.subscriptions.data;
    if (sources.length == 1 && subscriptions.length > 0) {
      // TODO(soon): handle this better (client-side?)
      throw new Meteor.Error(400, "Can't delete last card if still subscribed");
    }
  }

  id = findOriginalId(id, customerId);

  Meteor.wrapAsync(stripe.customers.deleteCard.bind(stripe.customers))(
    customerId,
    id
  );
};

BlackrockPayments.prototype.makeCardPrimary = function (id) {
  if (!Meteor.userId()) {
    throw new Meteor.Error(403, "Must be logged in to change primary card");
  }
  check(id, String);

  var customerId = Meteor.user().payments.id;
  id = findOriginalId(id, customerId);

  Meteor.wrapAsync(stripe.customers.update.bind(stripe.customers))(
    customerId,
    {default_source: id}
  );
};

BlackrockPayments.prototype.getStripeData = function () {
  if (!Meteor.userId()) {
    throw new Meteor.Error(403, "Must be logged in to get stripe data");
  }
  var payments = Meteor.user().payments;
  if (!payments) {
    return {};
  }
  var customerId = payments.id;
  var data = Meteor.wrapAsync(stripe.customers.retrieve.bind(stripe.customers))(customerId);
  if (data.sources && data.sources.data) {
    var sources = data.sources.data;
    for (var i = 0; i < sources.length; i++) {
      if (sources[i].id === data.default_source) {
        sources[i].isPrimary = true;
      }
      sources[i] = _.pick(sources[i], "last4", "brand", "id", "exp_year", "exp_month", "isPrimary");
      sources[i].id = hashId(sources[i].id);
    }
  }

  var subscription;
  if (data.subscriptions && data.subscriptions.data[0]) {
    // Hack to deal with beta being in our plan names
    subscription = data.subscriptions.data[0].plan.name.replace("-beta", "");
  }
  return {
    email: data.email,
    subscription: subscription,
    sources: data.sources && data.sources.data
  };
};

var plans = {
  standard: "1",
  large: "2",
  mega: "3"
}
BlackrockPayments.prototype.updateUserSubscription = function (newPlan) {
  if (!Meteor.userId()) {
    throw new Meteor.Error(403, "Must be logged in to update subscription");
  }
  check(newPlan, String);

  var payments = Meteor.user().payments;
  if (!payments) {
    throw new Meteor.Error(403, "User must have stripe data already");
  }
  var customerId = payments.id;
  var data = Meteor.wrapAsync(stripe.customers.retrieve.bind(stripe.customers))(customerId);

  if (newPlan === "free") {
    if (data.subscriptions && data.subscriptions.data.length > 0) {
      // TODO(someday): pass in at_period_end and properly handle pending cancelled subscriptions
      stripe.customers.cancelSubscription.bind(stripe.customers)(
        customerId,
        data.subscriptions.data[0].id
      );
    }
    // else: no subscriptions exist so we're already set to free
  } else {
    if (data.subscriptions && data.subscriptions.data.length > 0) {
      stripe.customers.updateSubscription.bind(stripe.customers)(
        customerId,
        data.subscriptions.data[0].id,
        {plan: plans[newPlan]}
      );
    } else {
      stripe.customers.createSubscription.bind(stripe.customers)(
        customerId,
        {plan: plans[newPlan]}
      );
    }
  }
};

BlackrockPayments.prototype.createUserSubscription = function (token, email, plan) {
  if (!Meteor.userId()) {
    throw new Meteor.Error(403, "Must be logged in to update subscription");
  }
  check(token, String);
  check(email, String);
  check(plan, String);

  var payments = Meteor.user().payments;
  var customerId;
  if (!payments || !payments.id) {
    customerId = this._createUser(token, email);
  } else {
    customerId = payments.id;
    this.addCardForUser(token, email);
  }
  stripe.customers.createSubscription.bind(stripe.customers)(
    customerId,
    {plan: plans[plan]}
  );
}
