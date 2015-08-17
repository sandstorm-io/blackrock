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
    var updateData = {
      email: event.data.token.email,
      subscription: event.data.plan
    };
    Meteor.call("createUserSubscription", event.data.token.id,
                event.data.token.email, event.data.plan, function (err) {
      if (err) {
        alert(err); // TODO(soon): make this UI better);
        return;
      }

      StripeCustomerData.upsert({_id: '0'}, updateData);
      template.eventuallyCheckConsistency();
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

  this.eventuallyCheckConsistency = function () {
    // After a few seconds, refresh stripe data from the server. If this method is called again
    // before the refresh, it pushes back the timeout.
    //
    // Stripe's database, like many distributed systems, is "eventually consistent", meaning if
    // we update a customer and then immediately read back the customer data we might not yet see
    // our own update. We could just assume that any successful update call did in fact update
    // the database and update our client-side copy, but this could lead to invisible bugs where
    // we're making the wrong kind of request and don't notice. So, what we do is update our
    // client-side copy but then fire off a request a few seconds later to see if the server has
    // the content we expect.

    if (this.eventualTimeout) {
      Meteor.clearTimeout(this.eventualTimeout);
    }
    var self = this;
    this.eventualTimeout = Meteor.setTimeout(function () {
      delete self.eventualTimeout;
      updateStripeData();
    }, 4000);
  }
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

      // Non-error return means the plan was updated successfully, so update our client-side copy.
      StripeCustomerData.update("0", {subscription: planName === "free" ? undefined : planName});
      template.isSelectingPlan.set(null);

      template.eventuallyCheckConsistency();
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
