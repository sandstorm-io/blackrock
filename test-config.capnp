@0xa9101b1fec595220;

using import "/blackrock/master.capnp".MasterConfig;

const vagrant :MasterConfig = (
  workerCount = 2,
  frontendCount = 2,
  frontendConfig = (
    baseUrl = "http://localrock.sandstorm.io:6080",
    wildcardHost = "*.localrock.sandstorm.io:6080",
    allowDemoAccounts = true,
    isTesting = true,
    isQuotaEnabled = false,
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

