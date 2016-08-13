// Sandstorm Blackrock
// Copyright (c) 2016 Sandstorm Development Group, Inc.
// All Rights Reserved

Template.stripePaymentAcceptorPowerboxConfiguration.events({
  "submit form"(event) {
    event.preventDefault();

    this.powerboxRequest.completeNewFrontendRef({
      stripePaymentAcceptor: {
        acceptorTitle: event.currentTarget.acceptorTitle.value,
        returnAddress: event.currentTarget.returnAddress.value,
        settingsUrl: event.currentTarget.settingsUrl.value,
      },
    });
  }
});

let counter = 0;

Template.stripeAddPaymentSourcePowerboxConfiguration.onCreated(function () {
  // TODO(cleanup): There's a lot of repeated code between this and the billing settings, but
  //   factoring out the common parts looked hard so I punted. Probably we should eventually
  //   replace the whole thing with a form that we render ourselves, rather than rely on Stripe's
  //   checkout.js.

  updateStripeData();
  this.addCardPrompt = new ReactiveVar(false);
  this.id = "stripe-powerbox-add-card-" + (counter++);
  this.listener = event => {
    console.log(event);
    if (event.origin !== window.location.protocol + "//" + makeWildcardHost("payments")) {
      return;
    }

    if (event.data.id !== this.id) {
      return;
    }

    if (event.data.showPrompt) {
      // ignore
      return;
    }

    if (event.data.token) {
      Meteor.call("addCardForUser", event.data.token.id, event.data.token.email, (err, source) => {
        if (err) {
          this.data.powerboxRequest.failRequest(err);
        } else {
          this.data.powerboxRequest.completeNewFrontendRef({
            stripePaymentSource: {
              source: source.id
            },
          });
        }
      });
    }

    if (event.data.error) {
      this.data.powerboxRequest.cancelRequest();
    }
  };

  window.addEventListener("message", this.listener, false);
});

Template.stripeAddPaymentSourcePowerboxConfiguration.onDestroyed(function () {
  window.removeEventListener("message", this.listener, false);
});

Template.stripeAddPaymentSourcePowerboxConfiguration.helpers({
  paymentsUrl: function () {
    return window.location.protocol + "//" + makeWildcardHost("payments");
  },

  checkoutData: function () {
    var template = Template.instance();
    var primaryEmail = _.findWhere(SandstormDb.getUserEmails(Meteor.user()), {primary: true});
    if (!primaryEmail) return;
    return encodeURIComponent(JSON.stringify({
      name: 'Sandstorm Oasis',
      panelLabel: "Add Card",
      email: primaryEmail.email,
      id: template.id,
      openNow: true,
    }));
  },
});