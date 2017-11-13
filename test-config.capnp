@0xa9101b1fec595220;

using import "/blackrock/master.capnp".MasterConfig;

const vagrant :MasterConfig = (
  workerCount = 2,
  frontendCount = 2,
  frontendConfig = (
    baseUrl = "http://localrock.sandstorm.io",
    wildcardHost = "*.localrock.sandstorm.io",
    allowDemoAccounts = true,
    isTesting = true,
#    stripeKey = "sk_test_???",
#    stripePublicKey = "pk_test_???",
    outOfBeta = true,
    allowUninvited = true,
    replicasPerMachine = 2,
#    mailchimpKey = "???",
#    mailchimpListId = "???",
  ),
  vagrant = ()
);

