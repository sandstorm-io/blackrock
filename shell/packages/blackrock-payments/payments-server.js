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
var ROOT_URL = process.env.ROOT_URL;
var HOSTNAME = Url.parse(ROOT_URL).hostname;
var stripe = Npm.require("stripe")(Meteor.settings.stripeKey);

BlackrockPayments = {};

var serveCheckout = Meteor.bindEnvironment(function (res) {
  res.writeHead(200, { "Content-Type": "text/html" });
  res.end(Assets.getText("checkout.html").replace(
      "$STRIPE_KEY", Meteor.settings.public.stripePublicKey));
});

var serveSandcat = Meteor.bindEnvironment(function (res) {
  res.writeHead(200, { "Content-Type": "image/png" });
  // Meteor's buffer isn't a real buffer, so we have to do a copy
  res.end(new Buffer(Assets.getBinary("sandstorm-purplecircle.png")));
});

function hashId(id) {
  return Crypto.createHash("sha256").update(ROOT_URL + ":" + id).digest("base64");
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

function sanitizeSource(source, isPrimary) {
  var result = _.pick(source, "last4", "brand", "exp_year", "exp_month", "isPrimary");
  result.isPrimary = isPrimary;
  result.id = hashId(source.id);
  return result;
}

var inFiber = Meteor.bindEnvironment(function (callback) {
  callback();
});

function renderPrice(amount) {
  var dollars = Math.floor(amount / 100);
  var cents = amount % 100;
  if (cents < 10) cents = "0" + cents;
  return dollars + "." + cents;
}

function handleWebhookEvent(db, event) {
  // WE CANNOT TRUST THE EVENT. We have no proof it came from Stripe.
  //
  // We could tell Stripe te authenticate with HTTP Basic Auth, but that's ugly and
  // introduces a new password that needs to be secured. Instead, we turn right around and
  // fetch the event back from Stripe based on the ID.
  //
  // There is still a problem: if an external user can guess event IDs they can reply old
  // events. Therefore when an event causes us to make a change, we ensure that the event
  // is idempotent and also refuse to process the event if it's timestamp is older than the
  // latest change to the same target.

  // Fetch the event from Stripe.
  event = Meteor.wrapAsync(stripe.events.retrieve.bind(stripe.events))(event.id);

  var timestamp = new Date(event.created * 1000);

  if (event.type === "invoice.payment_succeeded" || event.type === "invoice.payment_failed") {
    var invoice = event.data.object;
    var user = Meteor.users.findOne({"payments.id": invoice.customer});
    if (!user) {
      throw new Error("no such customer");
    }

    if (user.payments.lastInvoiceTime && user.payments.lastInvoiceTime >= event.created) {
      console.log("ignoring duplicate invoice event");
      return;
    }

    var plan = db.getPlan(user.plan || free);
    var planTitle = plan.title || (plan._id.charAt(0).toUpperCase() + plan._id.slice(1));
    var priceText = renderPrice(plan.price);

    // Send an email.
    var email = _.find(SandstormDb.getUserEmails(user), function (email) { return email.primary; });
    if (!email) {
      email = Meteor.wrapAsync(stripe.customers.retrieve.bind(stripe.customers))
          (invoice.customer).email;
    }

    var mailSubject;
    var mailText;
    var mailHtml;
    if (event.type === "invoice.payment_failed") {
      mailSubject = "Invoice from Sandstorm Oasis";
      mailText =
          "You have a new invoice from Sandstorm Oasis:\n" +
          "\n" +
          "1 month " + platTitle + " plan: $" + priceText + "\n" +
          "Beta discount: -$" + priceText + "\n" +
          "======================================================\n" +
          "Total: $0\n" +
          "\n" +
          "This invoice has already been paid using the payment info we have on file.\n" +
          "\n" +
          "Thank you!\n";
      mailHtml =
          "<p>You have a new invoice from Sandstorm Oasis:</p>\n" +
          "<table>\n" +
          "<tr><td>1 month " + platTitle + " plan</td><td>$" + priceText + "</td></tr>\n" +
          "<tr><td>Beta discount</td><td>-$" + priceText + "</td></tr>\n" +
          "======================================================\n" +
          "<tr><td>Total</td><td>$0</td></tr>\n" +
          "</table>\n" +
          "<p>This invoice has already been paid using the payment info we have on file.</p>\n" +
          "<p>Thank you!</p>\n";
    } else {
      mailSubject = "URGENT: Payment failed for Sansdtorm Oasis";
      mailText =
          "We were unable to charge your payment method to renew your " +
          "subscription to Sandstorm Oasis. Your account has been " +
          "demoted to the free plan. Please click on the link below to " +
          "log into your account and update your payment info, then " +
          "switch back to a paid plan.\n" +
          "\n" +
          ROOT_URL + "/account\n";
      mailHtml =
          "<p>We were unable to charge your payment method to renew your " +
          "subscription to Sandstorm Oasis. Your account has been " +
          "demoted to the free plan. Please click on the link below to " +
          "log into your account and update your payment info, then " +
          "switch back to a paid plan.</p>\n" +
          "<p><a href=\"" + ROOT_URL + "/account\">" + ROOT_URL + "/account</a></p>\n";
    }

    if (email) {
      SandstormEmail.send({
        to: emailAddress,
        from: "Sandstorm Oasis <no-reply@" + HOSTNAME + ">",
        subject: "Invoice from Sandstorm Oasis",
        text: mailText,
        html: mailHtml,
      });
    } else {
      console.error("customer has no email address", invoice.customer);
    }

    var mod = {"payments.lastInvoiceTime": event.created};
    if (event.type === "invoice.payment_failed") {
      // Cancel plan.
      // TODO(soon): Some sort of grace period.
      mod.plan = "free";
      var data = Meteor.wrapAsync(
          stripe.customers.retrieve.bind(stripe.customers))(invoice.customer);
      if (data.subscriptions && data.subscriptions.data.length > 0) {
        Meteor.wrapAsync(stripe.customers.cancelSubscription.bind(stripe.customers))(
            invoice.customer, data.subscriptions.data[0].id);
      }
    }

    Meteor.users.update({_id: user._id}, {$set: mod});
  }
}

function processWebhook(db, req, res) {
  var data = "";
  req.on("data", function (chunk) {
    data += chunk;
  });

  req.on("error", function (err) {
    res.writeHead(400, { "Content-Type": "text/plain" });
    res.end("error receiving request");
  });

  req.on("end", function () {
    inFiber(function () {
      try {
        handleWebhookEvent(db, JSON.parse(data));
        res.writeHead(200, { "Content-Type": "text/plain" });
        res.end("success");
      } catch (err) {
        console.error("error processing Stripe webhook:", err.stack, "\ndata:", data);
        res.writeHead(500, { "Content-Type": "text/plain" });
        res.end("internal server error");
      }
    });
  });
}

BlackrockPayments.makeConnectHandler = function (db) {
  return function (req, res, next) {
    if (req.headers.host == db.makeWildcardHost("payments")) {
      if (req.url == "/checkout") {
        serveCheckout(res);
      } else if (req.url == "/sandstorm-purplecircle.png") {
        serveSandcat(res);
      } else if (req.url === "/webhook") {
        processWebhook(db, req, res);
      } else {
        res.writeHead(404, { "Content-Type": "text/plain" });
        res.end("404 not found: " + req.url);
      }
    } else {
      next();
    }
  };
}

function createUser(token, email) {
  var data = Meteor.wrapAsync(stripe.customers.create.bind(stripe.customers))({
    source: token,
    email: email,
    description: Meteor.userId()  // TODO(soon): Do we want to store backrefs to our database in stripe?
  });
  Meteor.users.update({_id: Meteor.userId()}, {$set: {payments: {id: data.id}}});
  return data;
}

var methods = {
  addCardForUser: function (token, email) {
    if (!this.userId) {
      throw new Meteor.Error(403, "Must be logged in to add card");
    }
    check(token, String);
    check(email, String);

    var user = Meteor.user();

    if (user.payments && user.payments.id) {
      return sanitizeSource(Meteor.wrapAsync(stripe.customers.createSource.bind(stripe.customers))(
        user.payments.id,
        {source: token}
      ), false);
    } else {
      var data = createUser(token, email);
      if (data.sources && data.sources.data && data.sources.data.length >= 1) {
        return sanitizeSource(data.sources.data[0], true);
      }
    }
  },

  deleteCardForUser: function (id) {
    if (!this.userId) {
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
  },

  makeCardPrimary: function (id) {
    if (!this.userId) {
      throw new Meteor.Error(403, "Must be logged in to change primary card");
    }
    check(id, String);

    var customerId = Meteor.user().payments.id;
    id = findOriginalId(id, customerId);

    Meteor.wrapAsync(stripe.customers.update.bind(stripe.customers))(
      customerId,
      {default_source: id}
    );
  },

  getStripeData: function () {
    if (!this.userId) {
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
        sources[i] = sanitizeSource(sources[i], sources[i].id === data.default_source);
      }
    }

    var subscription;
    if (data.subscriptions && data.subscriptions.data[0]) {
      // Plan names end with "-beta".
      subscription = data.subscriptions.data[0].plan.id.split("-")[0];
    }
    return {
      email: data.email,
      subscription: subscription,
      sources: data.sources && data.sources.data,
      credit: -(data.account_balance || -0)
    };
  },

  updateUserSubscription: function (newPlan) {
    if (!this.userId) {
      throw new Meteor.Error(403, "Must be logged in to update subscription");
    }
    check(newPlan, String);

    var planInfo = this.connection.sandstormDb.getPlan(newPlan);

    if (planInfo.hidden) {
      throw new Meteor.Error(403, "Can't choose discontinued plan.");
    }

    var payments = Meteor.user().payments;
    if (payments) {
      var customerId = payments.id;
      var data = Meteor.wrapAsync(stripe.customers.retrieve.bind(stripe.customers))(customerId);

      if (newPlan === "free") {
        if (data.subscriptions && data.subscriptions.data.length > 0) {
          // TODO(soon): pass in at_period_end and properly handle pending cancelled subscriptions
          Meteor.wrapAsync(stripe.customers.cancelSubscription.bind(stripe.customers))(
            customerId,
            data.subscriptions.data[0].id
          );
        }
        // else: no subscriptions exist so we're already set to free
      } else {
        if (data.subscriptions && data.subscriptions.data.length > 0) {
          Meteor.wrapAsync(stripe.customers.updateSubscription.bind(stripe.customers))(
            customerId,
            data.subscriptions.data[0].id,
            {plan: newPlan + "-beta"}
          );
        } else {
          Meteor.wrapAsync(stripe.customers.createSubscription.bind(stripe.customers))(
            customerId,
            {plan: newPlan + "-beta"}
          );
        }
      }
    } else {
      if (newPlan !== "free") {
        throw new Meteor.Error(403, "User must have stripe data already");
      }
    }

    Meteor.users.update({_id: this.userId}, {$set: { plan: newPlan }});
  },

  createUserSubscription: function (token, email, plan) {
    if (!this.userId) {
      throw new Meteor.Error(403, "Must be logged in to update subscription");
    }
    check(token, String);
    check(email, String);
    check(plan, String);

    var payments = Meteor.user().payments;
    var customerId;
    var sanitizedSource;
    if (!payments || !payments.id) {
      var data = createUser(token, email);
      customerId = data.id;
      if (data.sources && data.sources.data && data.sources.data.length >= 1) {
        sanitizedSource = sanitizeSource(data.sources.data[0]);
      }
    } else {
      customerId = payments.id;
      sanitizedSource = methods.addCardForUser.bind(this)(token, email);
    }
    Meteor.wrapAsync(stripe.customers.createSubscription.bind(stripe.customers))(
      customerId,
      {plan: plan + "-beta"}
    );
    Meteor.users.update({_id: this.userId}, {$set: { plan: plan }});
    return sanitizedSource;
  }
};
Meteor.methods(methods);

Meteor.publish("plans", function () {
  return this.connection.sandstormDb.listPlans();
});

function getAllStripeCustomers() {
  var hasMore = true;
  var results = [];

  var req = {limit: 100};
  while (hasMore) {
    var next = Meteor.wrapAsync(stripe.customers.list.bind(stripe.customers))(req);
    results = results.concat(next.data);
    hasMore = next.has_more;
    if (hasMore) {
      req.starting_after = results.slice(-1)[0].id;
    }
  }
  return results;
}

SandstormDb.paymentsMigrationHook = function (SignupKeys, plans) {
  var db = this;
  var customers = getAllStripeCustomers();
  console.log("got customers", customers.length);
  if (!customers) throw new Error("missing customers");

  var byEmail = {};
  for (var i in customers) {
    var customer = customers[i];
    byEmail[customer.email] = byEmail[customer.email] || [];
    byEmail[customer.email].push(customer);
  }

  var byQuota = {};
  for (var i in plans) {
    if (plans[i]._id !== "free") {
      byQuota[plans[i].storage] = plans[i]._id;
    }
  }

  function getCustomerByEmail(email, quota) {
    var customer = (byEmail[email] || []).shift();
    if (customer) {
      var plan;
      if (customer.subscriptions && customer.subscriptions.data[0]) {
        // Plan names end with "-beta".
        plan = customer.subscriptions.data[0].plan.id.split("-")[0];
      } else {
        plan = "free";
      }
      return {plan: plan, payments: {id: customer.id}};
    }
  }

  Meteor.users.find({quota: {$exists: true}}).forEach(function (user) {
    if (user.signupEmail) {
      var customer = getCustomerByEmail(user.signupEmail, user.quota);
      if (customer) {
        console.log("user", user._id, user.signupEmail, "=>", JSON.stringify(customer));
        Meteor.users.update({_id: user._id}, {$set: customer});
      } else {
        console.error("ERROR: missing customer for email (user):", email, user._id);
      }
    } else {
      console.warn("WARNING: user was not invited by email:",
          user._id, SandstormDb.getUserIdentities(user));
    }
  });

  SignupKeys.find({quota: {$exists: true}, used: false}).forEach(function (signupKey) {
    if (signupKey.email) {
      var customer = getCustomerByEmail(signupKey.email, signupKey.quota);
      if (customer) {
        console.log("invite", signupKey.email, "=>", JSON.stringify(customer));
        SignupKeys.update({_id: signupKey._id}, {$set: customer});
      } else {
        console.error("ERROR: missing customer for email (invite):", email, signupKey._id);
      }
    } else {
      console.warn("WARNING: non-email invite:", signupKey.note);
    }
  });

  for (var email in byEmail) {
    byEmail[email].forEach(function (customer) {
      console.error("ERROR: customer not used:", customer.id, email);
    });
  }
}
