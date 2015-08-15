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

var messageListener = function (template, event) {
  if (event.origin !== window.location.protocol + "//" + makeWildcardHost("payments")) {
    return;
  }

  if (event.data.id != template.id) {
    return;
  }

  if (event.data.showPrompt) {
    Session.set("plan-" + template.id, event.data.plan);
    return;
  }

  if (event.data.token) {
    Meteor.call("createUserSubscription", event.data.token.id, event.data.token.email, event.data.plan, function (err) {
      if (err) {
        alert(err); // TODO(soon): make this UI better);
        return;
      }

      updateStripeData();
      Meteor.setTimeout(updateStripeData, 3000); // Stripe is slow to update sometimes...
    });
  }

  if (event.data.error || event.data.token) {
    template.showPrompt.set(false);
    if (template.data.onDismiss) {
      template.data.onDismiss();
    }
  }
};

Template.billingPrompt.onCreated(function () {
  this.showPrompt = new ReactiveVar(true);
  this.showFullscreen = new ReactiveVar(null);
  this.listener = messageListener.bind(this, this);
  this.id = Math.random();
  window.addEventListener("message", this.listener, false);
});

Template.billingPrompt.onDestroyed(function () {
  window.removeEventListener("message", this.listener, false);
});

Template.billingPrompt.helpers({
  showPrompt: function () {
    return Template.instance().showPrompt.get();
  },
  onDismiss: function () {
    var self = this;
    return function () {
      if (self.onDismiss) self.onDismiss();
      return "remove";
    }
  },
  popupData: function () {
    return {promptId: Template.instance().id};
  }
});

Template._billingPromptBody.onCreated(function () {
  this.checkoutPlan = new ReactiveVar(null);
  this.isSelectingPlan = new ReactiveVar(null);
  this.subscribe("stripeCustomerData");
  updateStripeData();
});

function clickPlanHelper(ev, planName) {
  var template = Template.instance();
  var data = StripeCards.find();
  if (data.count() > 0) {
    template.isSelectingPlan.set(planName);
    Meteor.call("updateUserSubscription", planName, function (err) {
      if (err) {
        alert(err); // TODO(soon): make this UI better;
        return;
      }

      // Sometimes stripe doesn't update their DB immediately and this will return old results.
      updateStripeData(function () {
        template.isSelectingPlan.set(null);
        // TODO(soon): check that the data actually updated before clearing the spinner
      });
      Meteor.setTimeout(updateStripeData, 3000);
    });
  } else {
    var frame = ev.currentTarget.querySelector("iframe");
    frame.contentWindow.postMessage({openDialog: true}, "*");
  }
}

Template._billingPromptBody.events({
  "click .standard": function (ev) {
    clickPlanHelper(ev, "standard");
  },
  "click .large": function (ev) {
    clickPlanHelper(ev, "large");
  },
  "click .mega": function (ev) {
    clickPlanHelper(ev, "mega");
  },
  "click .free": function (ev) {
    clickPlanHelper(ev, "free");
  },
});

Template._billingPromptBody.helpers({
  standardFullscreen: function () {
    return Session.get("plan-" + this.promptId) === "standard";
  },
  largeFullscreen: function () {
    return Session.get("plan-" + this.promptId) === "large";
  },
  megaFullscreen: function () {
    return Session.get("plan-" + this.promptId) === "mega";
  },
  standardCheckoutData: function () {
    return JSON.stringify({
      name: 'Sandstorm Oasis Subscription',
      description: "Standard Plan",
      amount: 600,
      panelLabel: "{{amount}} / Month",
      id: this.promptId,
      planName: "standard"
    });
  },
  largeCheckoutData: function () {
    var template = Template.instance();
    return JSON.stringify({
      name: 'Sandstorm Oasis Subscription',
      description: "Large Plan",
      amount: 1200,
      panelLabel: "{{amount}} / Month",
      id: this.promptId,
      planName: "large"
    });
  },
  megaCheckoutData: function () {
    var template = Template.instance();
    return JSON.stringify({
      name: 'Sandstorm Oasis Subscription',
      description: "Mega Plan",
      amount: 2400,
      panelLabel: "{{amount}} / Month",
      id: this.promptId,
      planName: "mega"
    });
  },
  planIsStandard: function () {
    var data = StripeCustomerData.findOne();
    if (!data) return false;
    return data.subscription === "standard";
  },
  planIsLarge: function () {
    var data = StripeCustomerData.findOne();
    if (!data) return false;
    return data.subscription === "large";
  },
  planIsMega: function () {
    var data = StripeCustomerData.findOne();
    if (!data) return false;
    return data.subscription === "mega";
  },
  planIsFree: function () {
    var data = StripeCustomerData.findOne();
    if (!data) return true;
    return !data.subscription;
  },
  isSelectingStandard: function () {
    return Template.instance().isSelectingPlan.get() === "standard";
  },
  isSelectingLarge: function () {
    return Template.instance().isSelectingPlan.get() === "large";
  },
  isSelectingMega: function () {
    return Template.instance().isSelectingPlan.get() === "mega";
  },
  isSelectingFree: function () {
    return Template.instance().isSelectingPlan.get() === "free";
  },
  paymentsUrl: function () {
    return window.location.protocol + "//" + makeWildcardHost("payments");
  }
});
