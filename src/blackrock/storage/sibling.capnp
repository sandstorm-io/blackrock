# Sandstorm Blackrock
# Copyright (c) 2015 Sandstorm Development Group, Inc.
# All Rights Reserved

@0xce14f1ce85b2c651;
# Definitions:
# * Each time an object changes, its "version" increases.
# * A "transaction" is one atomic change to an object, normally incrementing the version by one.
# * A "batch" transaction is the result of merging several transactions together into one
#   transaction, normally used when an object replica has fallen behind and needs to replay
#   several versions at once.
# * A "local" or "single-object" transaction is one which affects only one object.
# * A "distributed" or "multi-object" transaction is one which is coordinated between multiple
#   objects. The infrastructure needed for distributed transactions is a superset of that needed
#   for local transactions.
# * A "replica" is a copy of an object stored on one machine. Different replicas of an object
#   are stored on different machines. Every object has the same number of replicas, but the set
#   of machines hosting those replicas is chosen differently for every object.
# * A "leader" is chosen from among an object's replicas to coordinate operations on the object.
#   All other replicas become "followers" of that leader.
# * A "nominee" is a replica attempting to become leader.
# * A nominee must be accepted as leader by a "quorum" of followers before it actually becomes
#   leader. The number of replicas needed to represent a quorum is configurable but must be at
#   least a majority.
# * Every transaction performed by the leader must be accepted by a "quorum" of followers
#   before it is considered completed. Note that the quorum needed to elect a leader and the
#   quorum needed to accept a transaction are actually slightly different: The latter need not
#   necessarily be a majority, but just large enough that there cannot be a quorum of replicas
#   electing one leader at the same time as another quorum of replicas is still accepting a
#   previous leader. That is, the quorum required for election plus the quorum required to
#   complete a transaction must add up to more than the number of replicas per object.
# * We use two-phase commit: A transaction is first "staged" and then "committed" (or "aborted").
#   Each phase requires a quorum before moving on to the next.
# * Only distributed transactions can be "aborted". Local transactions will always be committed
#   after being staged, unless they are lost in a failure before being staged by a quorum.
# * A transaction is "complete" if it has been accepted by a quorum and (for distributed
#   transactions) it has been decided whether to commit or abort.
#
# The sequence of transactions applying to one object must follow these rules:
# 1. A leader must number the transactions it proposes sequentially with no gaps.
# 2. A leader may not stage a transaction until any previous transaction from the same leader
#    has been acknowledged as complete by a quorum of replicas.
# 3. A leader may not ask for acknowledgment of completion from any replica until the transaction
#    has been staged on a quorum of replicas.
# 4. There must be a gap in version numbers between the last transaction completed by a previous
#    leader and the first staged by a new leader, to prevent any overlap in transaction numbers.
#
# A replica becomes leader by the following process:
# 1. A replica is nominated. (The method by which the replica is chosen, and the state of the
#    system at the time of nomination, are irrelevant for correctness, but the choice of strategy
#    could affect performance.)
# 2. If the nominated replica is already a leader or follower, it disconnects from this role.
#    All attempts to call the previous Leader or Follower capability will throw "disconnected"
#    exceptions.
# 3. The nominated replica contacts all other replicas and asks them to follow it, passing each
#    a capability to the nominee's new Leader interface.
# 4. Each replica receiving a follow request similarly disconnects from its previous role,
#    creates a new Follower interface, and returns it to the nominee. The follower also tells
#    the nominee what version its copy of the object is at (as of the last transaction it knows
#    to have been completed) and gives the nominee a copy of any transaction that was staged
#    by the previous leader but not completed.
# 5. If any follower claims to be at a version which is newer than the nominee's version before
#    a quorum is gathered, then the nominee immediately concedes the election and disconnects
#    from the leadership role. The process should be restarted with the newer replica as the
#    nominee.
# 6. If *all* replicas reply (not just a quorum), they are all at the same version, and none have
#    leftover staged transactions, then we can take the fast path: The leader picks up right where
#    the last left off. Skip to step 8.
# 7. If some replicas time out, are at inconsistent versions, or have leftover staged transactions,
#    but at least a quorum do in fact respond, then the nominee must work to fix them.
#   7a. The nominee chooses a version for its initial transaction that is at least two more than
#       the version of any observed transaction (completed or staged) and any `startVersion`
#       returned by any of the followers.
#   7b. The nominee calls `setStartVersion()` on each follower to inform them of its planned start
#       version and waits for at least a quorum of these calls to succeed. This ensures that if
#       the current recovery attempt does not complete (does not commit any transactions), a future
#       recovery attempt will know to use a later version number, avoiding possible ambiguity.
#   7c. If any uncommitted staged transactions were reported (with version numbers higher than any
#       observed committed transaction), the nominee chooses the one with the highest version
#       number to restore.
#   7d. For each follower in the quorum (including the nominee itself), the nominee stages a
#       transaction which represents the merge of all committed transactions between the follower's
#       version and the nominee's version as well as the staged transaction chosen in 7c (if any).
#       This batch transaction's version number is the start version chosen in 7a. Note that the
#       nominee stages such a transaction on itself, too!
#   7e. The transaction from 7d is staged and committed in the same manner transactions normally
#       are.
# 8. The nominee is now officially the leader. It may now respond to the getObject() method,
#    returning the high-level storage object for use by a client.
# 9. When followers disconnect, the leader attempts to reconnect and then replay missed
#    transactions. If, upon reconnect, the leader finds that the follower has advanced beyond the
#    leader's version, then the leader immediately abdicates. This makes it impossible for two
#    leaders to make progress concurrently: for a new leader to be elected, at least one foller
#    of the old leader would have to defect. The new leader would commit its initial transaction
#    to the follower, bringing its version number past that of the old leader, while the old leader
#    would be stuck without a quorum doing nothing. If the follewer somehow returned to the old
#    leader, the old leader would see the verison number andgive up.
# 10. When all clients disconnect from the high-level object, the leader steps down. The next time
#     the object is opened, a new leader will be chosen.
#
# All of the above should be straightforward except 7c. Why is it safe to apply just the newest
# of the straggler staged transactions? To answer, we break it down into several possibilities,
# based on when exactly the previous leader was deposed:
# * If the previous leader was deposed between transactions, then the new leader will not observe
#   any incomplete staged transactions.
# * If the previous leader was deposed after having partially staged a transaction, the new leader
#   may or may not observe that staged transaction. However, since the staging was incomplete, it
#   is safe both to replay the transaction or to ignore it.
# * If the previous leader was deposed after staging a transaction to a quorum but before having
#   sent commit() to the entire quorum, then the new leader may or may not observe that commit()
#   occurred, but the new leader definitely will at least observe the transaction as staged and
#   the transaction before it as committed, and will not observe any other staged transactions.
#   So all the staged transactions it sees are copies, and it can choose any of them.
# * If the previous leader was deposed _during step 7_ (i.e. it was itself performing recovery),
#   it may have staged its initial transaction on some replicas while a previous staged transaction
#   was still present on other replicas. Since this previous leader was itself following the rules
#   for recovery, we know that any transaction it produced is sufficient for full recovery, having
#   already merged in any necessary staged transactions from leaders before it. Therefore if we
#   see any copies of the previous leader's staged transaction attempt, we should use them.
#   If we don't see any such copies, then we know for sure that the previous leader did not
#   commit its initial transaction, and therefore it's safe for us to pretend it never happened
#   and instead look for the previous previous leader's leftovers. Note that our use of
#   setStartVersion() prevents any two recovery attempts from choosing the same version number
#   for their initial transaction, because before staging the transaction at all, the leader
#   ensures that the next leader has the information it needs to choose a higher version number.

$import "/capnp/c++.capnp".namespace("blackrock::storage");
using Storage = import "/blackrock/storage.capnp";
using FsStorage = import "/blackrock/fs-storage.capnp";
using ObjectId = FsStorage.StoredObjectId;
using ObjectKey = FsStorage.StoredObjectKey;
using OwnedStorage = Storage.OwnedStorage;

interface Sibling {
  # Interface which storage nodes use to talk to each other.

  createObject @0 (key :ObjectKey) -> (factory :Storage.StorageFactory);
  # Create a new object with the given key -- presumably newly-generated by the caller. The
  # returned factory is good for exactly one call, which will create an object with the desired
  # key.

  getReplica @1 (id :ObjectId) -> (replica :Replica);
  # Get the replica of the given object maintained by this node. Not valid to call if this node is
  # not a member of the object's replica set.
}

interface Replica {
  # A replica of a particular object. Each object has N replicas, where N is a system configuration
  # option that can be adjusted according to the reliability of the underlying storage. The
  # replicas of an object are assigned to storage machines based on a pure function of the
  # object ID.

  getLeader @0 () -> (leader :Leader);
  # Determine which replica is the leader for this object and return a capability to it.

  getLeaderStatus @1 () -> (version :UInt64, isLeading :Bool);
  # Called by one of the replicas of the given object when it needs to open the object but does
  # not know who the current leader is. The results are used only as hints for the caller to decide
  # whether it should attempt to become leader or defer to some other replica's getLeader().

  follow @2 (leader :Leader) -> (follower :Follower, version :UInt64,
                                 maybeStaged :RawTransaction, startVersion :UInt64);
  # Tells the callee to begin following the caller.
  #
  # The returned `version` indicates the follower's object version as of the last committed
  # transaction it saw. The leader must start by replaying any later transactions to catch the
  # follower up. If the follower's version is somehow newer than the leader's, then the leader
  # must immediately abdicate leadership.
  #
  # `maybeStaged`, if not null, is a transaction which had been staged on this replica previously
  # but neither committed nor aborted.
  #
  # `startVersion` is the suggested version number for the leader's first transaction. The leader
  # must choose an initial version that is greater than or equal to `startVersion` for a quorum's
  # worth of followers. Normally `startVersion` is equal to the version of the most-recently-staged
  # transaction plus one, but will be different if a previous leader called setStartVersion()
  # on the Follower and failed to actually stage any transaction after that.
}

interface Leader {
  # Interface exposed by leaders to followers.

  getObject @0 (key :ObjectKey) -> (cap :OwnedStorage);
  # Get the high-level representation of the object.

  startTransaction @1 () -> (builder :TransactionBuilder);
  # Starts a transaction on the object. Throws "disconnected" if there is another transaction
  # already occurring.

  cleanupTransaction @2 (id :TransactionId, aborted :Bool);
  # Requests that the callee please complete the given transaction if it is still staged. This is
  # used only when StagedTransaction.commit() or .abort() failed to return successfully. On
  # successfully returning from this call, the callee makes the same guarantee as if commit() or
  # abort() had successfully completed: that it will never call getTransactionState() for this
  # transaction. Note that if the callee knows nothing about the transaction, it should immediately
  # return success on the assumption that the transaction was already completed and cleaned up.

  getTransactionState @3 (id :UInt64) -> (aborted :Bool);
  # Get the state of a transaction for which the callee is the coordinator. If the named
  # transaction is still staged but neither committed nor aborted, waits for it to reach either
  # the commit or the abort state.
  #
  # This call travels in the opposite direction of stageTransaction() and cleanup() and is only
  # used to recover in the case of a crash / partition.
}

interface Follower {
  # Interface that an object leader uses to broadcast transactions to replicas. A `Replica`
  # is associated with a specific object, not a machine.

  stage @0 (txn :RawTransaction, version :UInt64) -> (staged :StagedTransaction);
  # Stage a transaction. Upon committing this transaction, the object will reach the given version.
  #
  # A quorum of replicas must respond successfully to this call before the transaction can be
  # safely committed. Otherwise, it's possible that the caller is no longer leader, therefore
  # lacks the authority to commit transactions. Therefore, if this transaction is being replicated
  # as part of a stageTransaction() call, that call cannot return success until a quorum of
  # followers have staged the transaction.

  free @1 (id :UInt64);
  # Indicates that all transactions through the given one have been committed by all replicas and
  # therefore can now be deleted from the transaction log.

  setStartVersion @2 (version :UInt64);
  # Pre-informs the follower what the version of the first transaction staged by this leader will
  # be. This is only used in rare recovery paths, where there is concern about possible later
  # ambiguity if another failure occurs while the leader is staging its first transaction. The
  # follower replica saves the version specified and makes sure that future calls to `follow()`
  # return at least this version plus one for `startVersion`.

  # TODO(soon): write/sync for non-transactional access
}

interface TransactionBuilder {
  getTransactional @0 [T] (object :T) -> (transactionalObject :T);
  # Get a transactional wrapper around the given object. `object` must be a capability representing
  # one of the facets of the storage object with which this Transaction is associated. For exmaple,
  # when transacting on an Assingable, `object` may be a capability to the `Assignable` itself or
  # to an `Assignable.Setter` pointing to the same underlying object.
  #
  # Once either `stage()` is called or the `Transaction` is dropped, `transactionalObject` will be
  # revoked.

  addOps @1 (ops :List(RawTransaction.Op));
  # Add some direct low-level ops.

  stage @2 (id :TransactionId) -> (staged :StagedTransaction);
  # Prepare to commit the transaction. If this succeeds, then the underlying object has been
  # locked. All other transactions will be rejected until this transaction is either committed or
  # aborted.
}

interface StagedTransaction {
  # A transaction that has been staged and is ready to commit. While the transaction is staged,
  # no conflicting transactions are allowed to be created, thus ensuring that the transaction
  # can be committed whenever the coordinator is ready.
  #
  # If a StagedTransaction becomes disconnected without commit() or abort() successfully being
  # called, then the callee will need to independently determine whether the commit or abort
  # condition was reached by contacting other nodes. This is an ususual case -- normally, either
  # commit() or abort() is called and the callee is absolved of the need to verify.
  #
  # Once commit() or abort() returns successfully, the callee guarantees that it will not under
  # any circumstances call back to ask the transaction's status, therefore the caller can
  # potentially clean up any information it was keeping about the transaction. If these methods
  # throw an exception instead (especially "disconnected") then the caller will need to keep state
  # so that it can respond to such callbacks. It may periodically inquire as to whether it is safe
  # to throw away said state.

  commit @0 ();
  # Asserts that the transaction's commit condition has been reached. The callee need not
  # independently verify.

  abort @1 ();
  # Asserts that the transaction's abort condition has been reached. The callee need not
  # independently verify. It is illegal to call abort() on a single-object transaction (one with
  # no TransactionId). The only way such a transaction can be aborted is if the transaction fails
  # to be staged on a quorum of replicas and the next leader happens to be elected by a quorum
  # composed only of replicas that never staged the transaction.
  #
  # An aborted transaction still updates the version number.
}

struct TransactionId {
  coordinator @0 :ObjectId;
  # Identities the "lead object" of this transaction. The storage nodes responsible for tracking
  # this object are also responsible for deciding when the transaction has completed.

  id @1 :UInt64;
  # The ID of this transaction among those associated with the coordinator object. Note that an
  # ID can be reused so long as the old ID has been globally forgotten.
}

struct RawTransaction {
  id @0 :TransactionId;
  # The distributed transaction ID, or null for a single-object transaction.
  #
  # Single-object transactions cannot be explicitly aborted, and are guaranteed committed once
  # they have been staged on a majority of replicas; see StagedTransaction.abort().
  #
  # Distributed transactions *can* be aborted by the transaction coordinator, even after being
  # staged on any number of replicas.

  version @1 :UInt64;
  # Version we'll transition to if this transaction goes through.
  #

  fromVersion @2 :UInt64;
  # The transaction can be safely applied to any object whose version is at least `fromVersion`,
  # in order to bring it to `toVersion`.
  #
  # Note that it is possible to merge any two transaction A and B where A.version == B.fromVersion
  # into a merged transaction C where C.fromVersion == A.fromVersion and C.version == B.version --
  # i.e. applying both the transactions -- without any external knowledge, because all of the
  # operations are idempotent.

  ops @3 :List(Op);

  struct Op {
    union {
      create @0 :UInt8;
      # Type is ObjectType enum from basics.h.

      setContent @1 :Data;

      delete @2 :Void;

      becomeReadOnly @3 :Void;

      setAccountedBlockCount @4 :UInt32;

      setTransitiveBlockCount @5 :UInt64;

      setParent @6 :ObjectId;

      backburnerModifyTransitiveBlockCount @7 :Int64;
      # Schedules a backburner task to eventually modify this object's transitive block count and
      # recurse to its parent. The value is applied as a delta.

      backburnerRecursivelyDelete @8 :Void;
      # Schedules a backburner task to eventually delete this object and recurse to its children.

      clearBackburner @9 :Void;
      # Remove all backburner tasks attached to this object.
    }
  }
}

struct StorageConfig {
  replicasPerObject @0 :UInt8;
  # Number of replicas of each object.

  quorumSize @1 :UInt8;
  # How many votes does it take to elect a leader?
  #
  # This need not be strictly "half the replicas plus one". When a leader broadcasts a transaction
  # to followers, it needs some number of followers N to acknowledge the transaction before the
  # object leader can signal acceptance to the transaction coordinator. This N is the minimum
  # number such than N + quorumSize > replicasPerObject.
  #
  # In particular this means that when quorumSize == replicasPerObject, then the leader can
  # immediately accept a transaction without consulting any followers. However, it means that if
  # any machine in the replica set is unreachable then no writes will be allowed at all until
  # the machine comes back up. (And the permanent loss of one replica's storage could mean data
  # loss.)
}
