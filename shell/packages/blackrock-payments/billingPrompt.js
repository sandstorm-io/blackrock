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

var idCounter = 0;

var messageListener = function (template, event) {
  if (event.origin !== window.location.protocol + "//" + makeWildcardHost("payments")) {
    return;
  }

  if (event.data.id != template.id) {
    return;
  }

  if (event.data.showPrompt) {
    template.promptChoice.set(event.data.plan);
    return;
  }

  if (event.data.token) {
    var updateData = {
      email: event.data.token.email,
      subscription: event.data.plan
    };
    Meteor.call("createUserSubscription", event.data.token.id,
                event.data.token.email, event.data.plan, function (err, source) {
      if (err) {
        alert(err); // TODO(soon): make this UI better);
        return;
      }

      StripeCustomerData.upsert({_id: '0'}, updateData);
      if (source) StripeCards.upsert({_id: source.id}, source);
      template.eventuallyCheckConsistency();

      if (template.data.onComplete) {
        template.data.onComplete(true);
      }
    });
  }

  if (event.data.error || event.data.token) {
    template.promptChoice.set(null);
  }
};

Template.billingPrompt.helpers({
  onDismiss: function () {
    var self = this;
    return function () {
      if (self.onComplete) self.onComplete(false);
      return "remove";
    }
  }
});

Template._billingPromptBody.onCreated(function () {
  this.showFullscreen = new ReactiveVar(null);
  this.promptChoice = new ReactiveVar(null);  // which checkout iframe was clicked
  this.listener = messageListener.bind(this, this);
  this.id = idCounter++;
  window.addEventListener("message", this.listener, false);

  this.checkoutPlan = new ReactiveVar(null);
  this.isSelectingPlan = new ReactiveVar(null);
  this.subscribe("stripeCustomerData");
  this.subscribe("plans");
  updateStripeData();

  this.eventuallyCheckConsistency = function () {
    // After a few seconds, refresh stripe data from the server. If this method is called again
    // before the refresh, it pushes back the timeout.
    //
    // This is meant to catch problems in our ad hoc client-side cache. The update shouldn't ever
    // lead to changes if things are working correctly.

    if (this.eventualTimeout) {
      Meteor.clearTimeout(this.eventualTimeout);
    }
    var self = this;
    this.eventualTimeout = Meteor.setTimeout(function () {
      delete self.eventualTimeout;
      updateStripeData();
    }, 2000);
  }
});

Template._billingPromptBody.onDestroyed(function () {
  window.removeEventListener("message", this.listener, false);
});

Template.billingUsage.onCreated(function () {
  this.subscribe("getMyUsage");
  this._showPrompt = new ReactiveVar(false);
});

Template.billingUsage.helpers({
  showPrompt: function () {
    return Template.instance()._showPrompt.get();
  },
  promptClosed: function () {
    var v = Template.instance()._showPrompt;
    return function () {
      v.set(false);
    };
  }
});

Template.billingUsage.events({
  "click .change-plan": function (event, template) {
    event.preventDefault();
    template._showPrompt.set(true);
  }
});

function clickPlanHelper(context, ev, planName) {
  if (context.isCurrent && Meteor.user().plan === planName) {
    // Ignore click on current plan.
    return;
  }

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
      StripeCustomerData.update("0", {$set: {subscription: planName }});
      template.isSelectingPlan.set(null);

      template.eventuallyCheckConsistency();

      if (template.data.onComplete) {
        template.data.onComplete(true);
      }
    });
  } else {
    var frame = ev.currentTarget.querySelector("iframe");
    frame.contentWindow.postMessage({openDialog: true}, "*");
  }
}

Template._billingPromptBody.events({
  "click .standard": function (ev) {
    clickPlanHelper(this, ev, "standard");
  },
  "click .large": function (ev) {
    clickPlanHelper(this, ev, "large");
  },
  "click .mega": function (ev) {
    clickPlanHelper(this, ev, "mega");
  },
  "click .free": function (ev) {
    clickPlanHelper(this, ev, "free");
  },
});

var helpers = {
  isFullscreen: function () {
    return Template.instance().promptChoice.get() === this._id;
  },
  checkoutData: function () {
    var title = this._id.charAt(0).toUpperCase() + this._id.slice(1);

    return JSON.stringify({
      name: 'Sandstorm Oasis Subscription',
      description: title + " Plan",
      amount: this.price,
      panelLabel: "{{amount}} / Month",
      id: Template.instance().id,
      planName: this._id,
      email: Meteor.user().profile.email
    });
  },
  plans: function () {
    var plans = this.db.listPlans().fetch();
    var data = StripeCustomerData.findOne();
    var myPlan = (data && data.subscription) || "unknown";
    plans.forEach(function (plan) {
      if (plan._id === myPlan) plan.isCurrent = true;
    });
    return plans;
  },
  renderCu: function (n) {
    return Math.floor(n / 1000000 / 3600);
  },
  renderCents: function (price) {
    return Math.floor(price / 100) + "." + ("00" + (price % 100)).slice(-2);
  },
  renderStorage: function (size) {
    var suffix = "B";
    if (size >= 1000000000) {
      size = size / 1000000000;
      suffix = "GB";
    } else if (size >= 1000000) {
      size = size / 1000000;
      suffix = "MB";
    } else if (size >= 1000) {
      size = size / 1000;
      suffix = "kB";
    }
    return Math.floor(size) + suffix;
  },
  renderStoragePrecise: function (size) {
    var suffix = "B";
    if (size >= 1000000000) {
      size = size / 1000000000;
      suffix = "GB";
    } else if (size >= 1000000) {
      size = size / 1000000;
      suffix = "MB";
    } else if (size >= 1000) {
      size = size / 1000;
      suffix = "kB";
    }
    return size.toPrecision(3) + suffix;
  },
  renderQuantity: function (n) {
    return (n === Infinity) ? "Unlimited" : n.toString();
  },
  renderPercent: function (num, denom) {
    return Math.min(100, Math.max(0, num / denom * 100)).toPrecision(3);
  },
  isSelecting: function () {
    return Template.instance().isSelectingPlan.get() === this._id;
  },
  paymentsUrl: function () {
    return window.location.protocol + "//" + makeWildcardHost("payments");
  },
  involuntary: function () { return this.reason && this.reason !== "voluntary"; },
  outOfGrains: function () { return this.reason === "outOfGrains"; },
  outOfStorage: function () { return this.reason === "outOfStorage"; },
  outOfCompute: function () { return this.reason === "outOfCompute"; },
  customApp: function () { return this.reason === "customApp"; },
  origin: function () { return document.location.protocol + "//" + document.location.host; },
  isDemoUser: function () {
    return this.db.isDemoUser();
  },
  myPlan: function () {
    return this.db.getMyPlan();
  },
  myUsage: function () {
    return this.db.getMyUsage();
  }
};

Template._billingPromptBody.helpers(helpers);
Template._billingPromptPopup.helpers(helpers);
Template.billingUsage.helpers(helpers);
