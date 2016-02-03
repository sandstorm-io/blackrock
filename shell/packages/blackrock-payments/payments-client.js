// Sandstorm Blackrock
// Copyright (c) 2015-2016 Sandstorm Development Group, Inc.
// All Rights Reserved

StripeCustomerData = new Mongo.Collection(null);  // see getStripeData for where this is produced
StripeCards = new Mongo.Collection(null);

updateStripeData = function (cb) {
  Meteor.call("getStripeData", function (err, data) {
    if (err) {
      alert(err); // TODO(soon): make this UI better
    } else {
      StripeCustomerData.upsert({_id: '0'}, {
          email: data.email,
          subscription: data.subscription ||
              (Meteor.user().plan === "free" ? "free" : undefined),
          credit: data.credit});
      if (data.sources) {
        sources = data.sources;
        for (var i = 0; i < sources.length; i++) {
          StripeCards.upsert({_id: sources[i].id}, sources[i]);
        };
      }
      if (cb) {
        cb();
      }
    }
  });
}

BlackrockPayments = function (db) {
  this.db = db
}

// Client-side method simulations.
Meteor.methods({
  unsubscribeMailingList: function () {
    Meteor.users.update({_id: Meteor.userId()}, {$set: {"payments.bonuses.mailingList": false}});
  },
  subscribeMailingList: function () {
    Meteor.users.update({_id: Meteor.userId()}, {$set: {"payments.bonuses.mailingList": true}});
  }
});
