// Sandstorm Blackrock
// Copyright (c) 2015-2016 Sandstorm Development Group, Inc.
// All Rights Reserved

var messageListener = function (showPrompt, template, ev) {
  if (ev.origin !== window.location.protocol + "//" + makeWildcardHost("payments")) {
    return;
  }

  if (ev.data.id !== template.id) {
    return;
  }

  if (ev.data.showPrompt) {
    showPrompt.set(true);
    return;
  }

  if (ev.data.token) {
    Meteor.call("addCardForUser", ev.data.token.id, ev.data.token.email, function (err) {
      if (err) alert(err); // TODO(soon): make this UI better

      updateStripeData();
    });
  }

  if (ev.data.error || ev.data.token) {
    showPrompt.set(false);
  }
};

Template.billingSettings.onCreated(function () {
  updateStripeData();
  this.addCardPrompt = new ReactiveVar(false);
  this.listener = messageListener.bind(this, this.addCardPrompt, this);
  this.id = Math.random();
  window.addEventListener("message", this.listener, false);
});

Template.billingSettings.onDestroyed(function () {
  window.removeEventListener("message", this.listener, false);
});

Template.billingSettings.events({
  "click .add-card": function (ev) {
    var frame = ev.target.parentElement.parentElement.querySelector("iframe");
    console.log(ev.target);
    frame.contentWindow.postMessage({openDialog: true}, "*");
  },
  "click .delete-card": function (ev) {
    var id = this.id;
    var template = Template.instance();
    Meteor.call("deleteCardForUser", id, function (err) {
      if (err) {
        alert(err); // TODO(soon): make this UI better
      } else {
        StripeCards.remove({_id: id});
      }

      updateStripeData();
    });
  },
  "click .make-primary-card": function (ev) {
    var template = Template.instance();
    StripeCards.update({isPrimary: true}, {$set: {isPrimary: false}});
    StripeCards.update({_id: this.id}, {$set: {isPrimary: true}});
    Meteor.call("makeCardPrimary", this.id, function (err) {
      if (err) alert(err); // TODO(soon): make this UI better

      updateStripeData();
    });
  }
});

Template.billingSettings.helpers({
  cards: function () {
    return StripeCards.find();
  },
  subscription: function () {
    var data = StripeCustomerData.findOne();
    return (data && data.subscription) || "free";
  },
  titleCase: function (text) {
    return text.slice(0, 1).toUpperCase() + text.slice(1);
  },
  credit: function () {
    var data = StripeCustomerData.findOne();
    return data && data.credit;
  },
  onChangePlanFunc: function () {
    var template = Template.instance();
    return function () {
      // TODO(someday): Anything we need to do here?
    };
  },
  checkoutData: function () {
    var template = Template.instance();
    var data = StripeCustomerData.findOne();
    if (!data) return;
    var primaryEmail = _.findWhere(SandstormDb.getUserEmails(Meteor.user()), {primary: true});
    if (!primaryEmail) return;
    return encodeURIComponent(JSON.stringify({
      name: 'Sandstorm Oasis',
      panelLabel: "Add Card",
      email: primaryEmail.email,
      id: template.id
    }));
  },
  paymentsUrl: function () {
    return window.location.protocol + "//" + makeWildcardHost("payments");
  },
  showPrompt: function () {
    return Template.instance().addCardPrompt.get();
  },
  renderCents: function (price) {
    return Math.floor(price / 100) + "." + ("00" + (price % 100)).slice(-2);
  },
});
