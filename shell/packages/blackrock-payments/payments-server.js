// Sandstorm Blackrock
// Copyright (c) 2015-2016 Sandstorm Development Group, Inc.
// All Rights Reserved

// The Meteor.user().payments object contains:
//   id: Stripe customer ID.
//   lastInvoiceTime: Timestamp (integer, from event.created) of the last time an invoice was
//       successfully paid.
//   bonuses: Object describing bonuses applied to the user's quota:
//       mailingList: Boolean, true if user is subscribed to mailing list.
//       metadata: A structure like plan.bonus but representing bonuses from Stripe metadata.

var Crypto = Npm.require("crypto");
var Url = Npm.require('url');
var ROOT_URL = process.env.ROOT_URL;
var HOSTNAME = Url.parse(ROOT_URL).hostname;

stripe = Npm.require("stripe")(Meteor.settings.stripeKey);

BlackrockPayments = {};

MailchimpSubscribers = new Mongo.Collection("mailchimpSubscribers");
// List of mailing list subscribers. We keep a copy of this rather than hit Mailchimp in real time
// because Mailchimp is sllooowwwww. We keep it up to date with webhooks.
//
// Each contains:
//     _id: An email address, exactly as stored in Mailchimp.
//     canonical: _id canonicalized (lower-cased, +suffixes removed, etc), for searchability.
//     subscribed: True if subscribed, false if not (e.g. if explicitly unsubscribed).
//     lastChanged: Last-change Date of this subscriber according to Mailchimp. Not present if
//         this entry was recently added artificially and isn't necessarily in Mailchimp yet.
//         The main purpose of this field is to allow us to discover what the latest event we know
//         about is, so that we can ask Mailchimp for newer events.

// Sandstorm icon, for embedding in emails.
var ICON_BASE64 = new Buffer(
    "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" +
    "A96gVsLcJMOhAAAAAElFTkSuQmCC", "base64");

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

hashSourceId = (id) => {
  return Crypto.createHash("sha256").update(ROOT_URL + ":" + id).digest("base64");
};

findOriginalSourceId = (hashedId, customerId) => {
  var data = Meteor.wrapAsync(stripe.customers.retrieve.bind(stripe.customers))(customerId);
  if (data.sources && data.sources.data) {
    var sources = data.sources.data;
    for (var i = 0; i < sources.length; i++) {
      if (hashSourceId(sources[i].id) === hashedId) {
        return sources[i].id;
      }
    }
  }

  throw new Meteor.Error(400, "Id not found");
};

sanitizeSource = (source, isPrimary) => {
  var result = _.pick(source, "last4", "brand", "exp_year", "exp_month", "isPrimary");
  result.isPrimary = isPrimary;
  result.id = hashSourceId(source.id);
  return result;
};

var inFiber = Meteor.bindEnvironment(function (callback) {
  callback();
});

function renderPrice(amount) {
  var prefix = " $";
  if (amount < 0) {
    prefix = "-$";
    amount *= -1;
  }

  var dollars = Math.floor(amount / 100);
  var cents = amount % 100;
  if (cents < 10) cents = "0" + cents;
  return prefix + dollars + "." + cents;
}

function sendEmail(db, user, mailSubject, mailText, mailHtml, config) {
  const iconCid = Random.id();

  // Add surrounding box.
  // TODO(someday): Make the logo image and title configurable by alternate hosts.
  mailHtml =
        '<div style="border: 1px solid #bbb; margin: 32px auto; max-width: 520px;">' +
        '  <div style="background-color: #eee; padding: 8px 32px; font-size: 25px; line-height: 34px;">' +
        '    <img src="cid:'+iconCid+'" style="width: 48px; vertical-align: bottom;"> Sandstorm.io' +
        '  </div>' +
        '  <div style="margin: 32px">' +
        mailHtml +
        '  </div>' +
        '</div>';

  let email = _.find(SandstormDb.getUserEmails(user), function (email) { return email.primary; });
  if (email) {
    email = email.email;
  } else {
    email = Meteor.wrapAsync(stripe.customers.retrieve.bind(stripe.customers))
        (user.payments.id).email;
  }

  if (email) {
    SandstormEmail.send({
      to: email,
      from: config.acceptorTitle + " <" + config.returnAddress + ">",
      subject: mailSubject,
      text: mailText,
      html: mailHtml,
      attachments: [
        {
          filename: "sandstorm-logo.png",
          contents: ICON_BASE64,
          contentType: "image/png",
          cid: iconCid
        },
      ],
    });
  } else {
    console.error("customer has no email address", user.payments.id);
  }
}

sendInvoice = (db, user, invoice, config) => {
  let total = 0;
  invoice.items.forEach(item => total += item.amountCents);

  const mailSubject = "Invoice from " + config.acceptorTitle;
  const priceColStyle = "text-align: right; white-space: nowrap;";
  const mailText =
      "You have a new invoice from " + config.acceptorTitle + ":\n" +
      "\n" +
      invoice.items.map(item => {
        return renderPrice(item.amountCents) + "  " + item.title.defaultText + "\n";
      }).join("") +
      "-----------------------------------------------\n" +
      renderPrice(total) + "  Total\n" +
      "\n" +
      "This invoice has already been paid using the payment info we have on file.\n" +
      "\n" +
      "To modify your subscription, visit:\n" +
      config.settingsUrl + "\n" +
      "\n" +
      "Thank you!\n";
  const mailHtml =
      '<h2>You have a new invoice from '+config.acceptorTitle+':</h2>\n' +
      '<table style="width: 100%">\n' +
      invoice.items.map(item => {
        return '  <tr><td>' + item.title.defaultText +
            '</td><td style="'+priceColStyle+'">' + renderPrice(item.amountCents) +
            '</td></tr>\n'
      }).join("") +
      '  <tr><td colspan="2"><hr style="border-style: none; border-top-style: solid; border-color: #bbb;"></td></tr>\n' +
      '  <tr><td><b>Total</b></td><td style="'+priceColStyle+'">' + renderPrice(total) + '</td></tr>\n' +
      '</table>\n' +
      '<p>This invoice has already been paid using the payment info we have on file.</p>\n' +
      '<p>To update your settings, visit:<br>\n' +
      '  <a href="' + config.settingsUrl + '">' + config.settingsUrl + '</a></p>\n' +
      '<p>Thank you!</p>\n';

  sendEmail(db, user, mailSubject, mailText, mailHtml, config);
}

function handleWebhookEvent(db, untrustedEvent) {
  // WE CANNOT TRUST THE EVENT. We have no proof it came from Stripe.
  //
  // We could tell Stripe to authenticate with HTTP Basic Auth, but that's ugly and
  // introduces a new password that needs to be secured. Instead, we turn right around and
  // fetch the event back from Stripe based on the ID.
  //
  // There is still a problem: if an external user can guess event IDs they can replay old
  // events. Therefore when an event causes us to make a change, we ensure that the event
  // is idempotent and also refuse to process the event if it's timestamp is older than the
  // latest change to the same target.

  // Fetch the event from Stripe.
  ev = Meteor.wrapAsync(stripe.events.retrieve.bind(stripe.events))(untrustedEvent.id);

  if (ev.type === "invoice.payment_succeeded" || ev.type === "invoice.payment_failed") {
    var invoice = ev.data.object;
    var user = Meteor.users.findOne({"payments.id": invoice.customer});
    if (!user) {
      console.error("Stripe event didn't match any user: " + ev.id);
      return;
    }

    if (user.payments.lastInvoiceTime && user.payments.lastInvoiceTime >= ev.created) {
      console.log("Ignoring duplicate Stripe event: " + ev.id);
      return;
    }

    console.log("Processing Stripe webhook " + ev.id + ": " + ev.type +
                " for user " + user._id);

    const config = {
      acceptorTitle: globalDb.getServerTitle(),
      returnAddress: db.getReturnAddress(),
      settingsUrl: ROOT_URL + "/account",
    };

    // Send an email.
    if (ev.type === "invoice.payment_failed") {
      const mailSubject = "URGENT: Payment failed for " + config.acceptorTitle;
      const mailText =
          "We were unable to charge your payment method to renew your " +
          "subscription to " + config.acceptorTitle + ". Your account has been " +
          "demoted to the free plan. Please click on the link below to " +
          "log into your account and update your payment info, then " +
          "switch back to a paid plan.\n" +
          "\n" +
          ROOT_URL + "/account\n";
      const mailHtml =
          "<p>We were unable to charge your payment method to renew your " +
          "subscription to " + config.acceptorTitle + ". Your account has been " +
          "demoted to the free plan. Please click on the link below to " +
          "log into your account and update your payment info, then " +
          "switch back to a paid plan.</p>\n" +
          "<p><a href=\"" + ROOT_URL + "/account\">" + ROOT_URL + "/account</a></p>\n";
      sendEmail(db, user, mailSubject, mailText, mailHtml, config);
    } else {
      const plan = db.getPlan(user.plan || free);
      const planTitle = plan.title || (plan._id.charAt(0).toUpperCase() + plan._id.slice(1));

      const sandstormInvoice = {
        items: [
          { title: { defaultText: "1 month " + planTitle + " plan" }, amountCents: plan.price },
          { title: { defaultText: "Beta discount" }, amountCents: -plan.price },
        ],
      };

      sendInvoice(db, user, sandstormInvoice, config);
    }

    var mod = {"payments.lastInvoiceTime": ev.created};
    if (ev.type === "invoice.payment_failed") {
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
  if (req.method !== "POST") {
    res.writeHead(405, { "Content-Type": "text/plain" });
    res.end("This endpoint is POST-only.\n");
    return;
  }

  var data = "";
  req.on("data", function (chunk) {
    data += chunk;
  });

  req.on("error", function (err) {
    res.writeHead(400, { "Content-Type": "text/plain" });
    res.end("error receiving request\n");
  });

  req.on("end", function () {
    inFiber(function () {
      try {
        handleWebhookEvent(db, JSON.parse(data));
        res.writeHead(200, { "Content-Type": "text/plain" });
        res.end("success\n");
      } catch (err) {
        console.error("error processing Stripe webhook:", err.stack, "\ndata:", data);
        res.writeHead(500, { "Content-Type": "text/plain" });
        res.end("internal server error\n");
      }
    });
  });
}

function mailchimpDate(date) {
  // Return "YYYY-MM-DD HH:mm:ss"
  var str = date.toISOString();
  return str.slice(0, 10) + " " + str.slice(11, 19);
}

function canonicalizeEmail(email) {
  // We canonicalize foo+bar@baz to foo@baz, and we lower-case the whole address. Neither of these
  // transformations are guaranteed to be safe, but we only use this for deciding whether someone
  // is on the mailing list. Some fudging here is OK, especially if it mostly results in false
  // positives.

  return email.replace(/\+.*@/, "@").toLowerCase();
}

function updateMailchimp(db) {
  var listId = Meteor.settings.mailchimpListId;
  var key = Meteor.settings.mailchimpKey;
  if (!listId || !key) throw new Error("Mailchimp not configured!");
  var shard = key.split("-")[1];

  var lastChanged =
      (MailchimpSubscribers.findOne({}, {sort: {lastChanged: -1}}) || {}).lastChanged;

  var count = 100;
  var retry = false;
  for(;;) {
    var url = "https://"+shard+".api.mailchimp.com/3.0/lists/" + listId +
        "/members?fields=total_items,members.email_address,members.status,members.last_changed" +
        "&count=" + count;
    if (lastChanged) {
      url += "&since_last_changed=" + mailchimpDate(lastChanged);
    }

    console.log("Mailchimp: Fetching updates:", url);

    var result = HTTP.get(url, {
      headers: { "Authorization": "apikey " + key },
      timeout: 60000
    });

    if (result.data.total_items <= count) break;

    if (retry) {
      throw new Error("Mailchimp: Retry had too many results too: " +
                      result.data.total_items + " > " + count);
    }

    console.log("Mailchimp: Query wasn't exhaustive. Trying again.", result.data.total_items);
    count = result.data.total_items + 100;
    retry = true;
  }

  (result.data.members || []).forEach(function (member) {
    check(member, {email_address: String, status: String, last_changed: String});
    MailchimpSubscribers.upsert({_id: member.email_address}, {$set: {
      canonical: canonicalizeEmail(member.email_address),
      subscribed: member.status === "subscribed",
      lastChanged: new Date(member.last_changed + " GMT")
    }});
    var count = db.findAccountsByEmail(member.email_address).map(updateBonuses).length;
    console.log("Mailchimp:", member.email_address, member.status, "(" + count + " users)");
  });
}

function processMailchimpWebhook(db, req, res) {
  // For now, we interpret Mailchimp hits as only a hint to check for updates from Mailchimp.
  // We ignore the POST payload because it's totally non-trustworthy anyhow, and because it's
  // more robust for us to search for all changes since the last we know about.

  inFiber(function () {
    try {
      updateMailchimp(db);
      res.writeHead(200, { "Content-Type": "text/plain" });
      res.end("success\n");
    } catch (err) {
      console.error("error processing Mailchimp webhook:", err.stack);
      res.writeHead(500, { "Content-Type": "text/plain" });
      res.end("internal server error\n");
    }
  });
}

BlackrockPayments.makeConnectHandler = function (db) {
  return function (req, res, next) {
    if (req.headers.host === db.makeWildcardHost("payments")) {
      if (req.url === "/checkout") {
        serveCheckout(res);
      } else if (req.url === "/sandstorm-purplecircle.png") {
        serveSandcat(res);
      } else if (req.url === "/webhook") {
        processWebhook(db, req, res);
      } else if (req.url === "/mailchimp" || req.url.lastIndexOf("/mailchimp?", 0) === 0) {
        processMailchimpWebhook(db, req, res);
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
      } else {
        throw new Meteor.Error(500, "Stripe created new user with no payment sources");
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
      if (sources.length === 1 && subscriptions.length > 0) {
        // TODO(soon): handle this better (client-side?)
        throw new Meteor.Error(400, "Can't delete last card if still subscribed");
      }
    }

    id = findOriginalSourceId(id, customerId);

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
    id = findOriginalSourceId(id, customerId);

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
    if (!payments || !payments.id) {
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
    if (payments && payments.id) {
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
  },

  unsubscribeMailingList: function () {
    var listId = Meteor.settings.mailchimpListId;
    var key = Meteor.settings.mailchimpKey;
    if (!listId || !key) throw new Error("Mailchimp not configured!");
    var shard = key.split("-")[1];

    var emails = SandstormDb.getUserEmails(Meteor.user()).filter(function (entry) {
      return entry.verified;
    }).map(function (entry) {
      return canonicalizeEmail(entry.email);
    });

    MailchimpSubscribers.find({canonical: {$in: emails}, subscribed: true})
        .forEach(function (entry) {
      var hash = Crypto.createHash("md5").update(entry._id).digest("hex");
      var url = "https://"+shard+".api.mailchimp.com/3.0/lists/" + listId + "/members/" + hash;

      console.log("Mailchimp: unsubscribing", entry._id);
      HTTP.call("PATCH", url, {
        data: {status: "unsubscribed"},
        headers: { "Authorization": "apikey " + key },
        timeout: 10000
      });

      MailchimpSubscribers.update({_id: entry._id}, {$set: {subscribed: false}});
    });

    updateBonuses(Meteor.user());
  },

  subscribeMailingList: function () {
    var listId = Meteor.settings.mailchimpListId;
    var key = Meteor.settings.mailchimpKey;
    if (!listId || !key) throw new Error("Mailchimp not configured!");
    var shard = key.split("-")[1];

    var emails = SandstormDb.getUserEmails(Meteor.user()).filter(function (entry) {
      return entry.primary;
    });

    if (emails.length === 0) {
      throw new Meteor.Error(400, "User has no verified email addresses to subscribe.");
    }

    var email = emails[0].email;
    var hash = Crypto.createHash("md5").update(email).digest("hex");
    var url = "https://"+shard+".api.mailchimp.com/3.0/lists/" + listId + "/members/" + hash;

    if (MailchimpSubscribers.find({_id: email}).count() > 0) {
      // User already exists in Mailchimp.
      console.log("Mailchimp: re-subscribing", email);
      HTTP.call("PATCH", url, {
        data: {status: "subscribed"},
        headers: { "Authorization": "apikey " + key },
        timeout: 10000
      });
    } else {
      console.log("Mailchimp: subscribing", email);
      HTTP.call("PUT", url, {
        data: {email_address: email, status: "subscribed"},
        headers: { "Authorization": "apikey " + key },
        timeout: 10000
      });
    }

    MailchimpSubscribers.upsert({_id: email},
        {$set: {canonical: canonicalizeEmail(email), subscribed: true}});
    updateBonuses(Meteor.user());
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

SandstormDb.bonusesMigrationHook = function () {
  var db = new SandstormDb();  // HACK: this only needs to work once

  updateMailchimp(db);

  Meteor.users.find({
    loginIdentities: {$exists: true},
    expires: {$exists: false},
  }).forEach(function (user) {
    console.log("Applying bonuses:", user._id);

    var bonuses = updateBonuses(user);

    if (!bonuses.mailingList && user.hasCompletedSignup) {
      Notifications.upsert({
        userId: user._id,
        mailingListBonus: true,
      }, {
        userId: user._id,
        mailingListBonus: true,
        timestamp: new Date(),
        isUnread: true,
      });
    }
  });
}

function getStripeBonus(user, paymentsBonuses) {
  var bonus = {};

  if (user.payments && user.payments.id) {
    var customer = Meteor.wrapAsync(stripe.customers.retrieve.bind(stripe.customers))
        (user.payments.id);

    var meta = customer.metadata;
    if (meta) {
      if (meta.bonusStorage) {
        bonus.storage = parseFloat(customer.metadata.bonusStorage) || 0;
      }
      if (meta.bonusCompute) {
        bonus.compute = parseFloat(customer.metadata.bonusCompute) || 0;
      }
      if (meta.bonusGrains) {
        bonus.grains = parseFloat(customer.metadata.bonusGrains) || 0;
      }
    }
  }

  if (paymentsBonuses) paymentsBonuses.metadata = bonus;
  return bonus;
}

function getMailchimpBonus(user, paymentsBonuses) {
  var emails = SandstormDb.getUserEmails(user).filter(function (entry) {
    return entry.verified;
  }).map(function (entry) {
    return canonicalizeEmail(entry.email);
  });
  if (emails.length > 0 &&
      MailchimpSubscribers.find({canonical: {$in: emails}, subscribed: true}).count() > 0) {
    if (paymentsBonuses) paymentsBonuses.mailingList = true;
    return { storage: MAILING_LIST_BONUS };
  } else {
    if (paymentsBonuses) paymentsBonuses.mailingList = false;
    return {};
  }
}

function updateBonuses(user) {
  var paymentsBonuses = {};
  var bonus = {};
  [getStripeBonus, getMailchimpBonus].forEach(function (f) {
    var b = f(user, paymentsBonuses);
    for (var field in b) {
      bonus[field] = (bonus[field] || 0) + b[field];
    }
  });

  if (!_.isEqual(user.planBonus, bonus) ||
      !user.payments ||
      !_.isEqual(user.payments.bonuses, paymentsBonuses)) {
    Meteor.users.update(user._id, {$set: {planBonus: bonus, "payments.bonuses": paymentsBonuses}});
  }

  return paymentsBonuses;
}

Meteor.publish("myBonuses", function () {
  if (!this.userId) return [];

  updateBonuses(Meteor.users.findOne({_id: this.userId}));
  return Meteor.users.find({_id: this.userId}, {fields: {"payments.bonuses": 1}});
});
