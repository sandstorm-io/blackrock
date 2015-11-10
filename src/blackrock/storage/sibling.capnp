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
#   object.
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
#   before it is considered "complete". Note that the quorum needed to elect a leader and the
#   quorum needed to accept a transaction are actually slightly different: The latter need not
#   necessarily be a majority, but just large enough that there cannot be a quorum of replicas
#   electing one leader at the same time as another quorum of replicas is still accepting a
#   previous leader. That is, the quorum required for election plus the quorum required to
#   complete a transaction must add up to more than the number of replicas per object.
# * Multi-object transaction require two-phase commit: A transaction is first "staged" and then
#   "committed" (or "aborted"). The staging phase requires a quorum of followers of each affected
#   object to acknowledge the transaction before it can be safely committed; if such consensus
#   cannot be achieved, the transaction must be aborted.
# * A "term" is the time during which a particular leader is leading. Each term has a start time
#   expressed as a vector clock. Since a term requires a quorum to start, there is always some
#   overlap between any two terms' vector clock start times, making them easily comparable. Every
#   write is associated with a term; the term during which an object's most-recent write occurred
#   is tracked by each replica.
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
#    to have been completed), in what term that version was written, and whether it is "dirty"
#    (meaning non-transactional writes may have occurred since the last version bump).
# 5. The leader waits for a quorum before going any further, then waits an additional timeout for
#    the rest of the replicas to join.
# 6. The leader forms its term time vector based on the replicas that have responded.
# 7. Among all the responding followers, the leader looks for the one which was most-recently
#    written. It first compares replicas by term in which they were last written (since terms
#    are strictly orderable), and then by version number, and finally prefers clean replicas over
#    dirty ones (technically dirty ones are newer, but their changes have not been synced and can
#    therefore be discarded; a clean replica is preferred due to the next step).
# 8. If any followers are *not* at the same version as the chosen replica, then those followers
#    must be completely replaced with a copy of the chosen replica. Note that any replica marked
#    "dirty" cannot be determined to be at exactly the same version as *any* other replica; if the
#    chosen replica is dirty then all others must be cloned from it. If the leader must replace
#    a replica, it does so as part of the first transaction it commits.
#
# Note that there is no requirement that followers are kept in lockstep. The leader will not
# acknowledge a transaction to the client until a quorum accepts it, but it is perfectly allowable
# for one follower to be several transactions ahead of another. If there is a failure and then
# the next leader starts with a quorum that doesn't include the replica that was ahead, then when
# that replica rejoins later its content will be deleted and reinitialized, thus losing the
# excess writes. That's fine because those writes were never acknowledged anyway.
#
# This design is important because we are optimizing for volume I/O to be fast, meaning in
# particular write()s and sync()s need to be fast. It is reasonably normal for apps to issue
# overlapping sync()s, e.g. due to separate threads performing fdatasync() on independent files.
# If we serialized them at the back-end, we could hurt performance.
#
# On the other hand, you could imagine a strategy in which a quorum of followers must acknowledge
# each version bump before the leader can schedule the next one on any of them. This strategy has
# the advantage that if each replica stored the most-recent transaction as a diff and only
# committed it upon the next arriving, then it would always be possible to roll back the most
# recent transaction without rewriting the whole content, which would possibly reduce the frequency
# with which such full replacements are needed during recovery. In practice, though, the only
# cases where full replacements are particularly onerous are volumes (which can be large), and
# volumes are not written transactionally anyway, so we don't gain anything.

$import "/capnp/c++.capnp".namespace("blackrock::storage");
using Storage = import "/blackrock/storage.capnp";
using FsStorage = import "/blackrock/fs-storage.capnp";
using ObjectId = FsStorage.StoredObjectId;
using ObjectKey = FsStorage.StoredObjectKey;
using OwnedStorage = Storage.OwnedStorage;

const maxVersionSkew :UInt64 = 1024;
# How many versions ahead one follower is allowed to be compared to the quorum. The leader must
# stop committing to a follower that gets this far ahead while waiting for the others to catch up.
#
# When a new leader is elected, it starts by incrementing all followers' versions by at least this
# amount (without making actual changes to the data) in order to prove its leadership.

interface Sibling {
  # Interface which storage nodes use to talk to each other.

  createObject @0 (key :ObjectKey) -> (factory :Storage.StorageFactory);
  # Create a new object with the given key -- presumably newly-generated by the caller. The
  # returned factory is good for exactly one call, which will create an object with the desired
  # key.

  getReplica @1 (id :ObjectId) -> (replica :Replica);
  # Get the replica of the given object maintained by this node. Not valid to call if this node is
  # not a member of the object's replica set.

  struct Time {
    # A monotonically-increasing value, maintained per-sibling. Aside from being
    # monotonically-increasing, this is not related to real time.
    #
    # Time values are designed to be used in a vector clock. A vector time can be represented as
    # List(Time).

    siblingId @0 :UInt32;
    # Specifies which sibling this time came from. Times from different siblings are not
    # comparable.

    generation @1 :UInt32;
    # Increments each time the sibling process starts / restarts.

    tick @2 :UInt64;
    # Increments each time the sibling's clock is observed. Resets to zero when the process
    # restarts.
  }

  getTime @2 () -> (time :Time);
}

interface Replica {
  # A replica of a particular object. Each object has N replicas, where N is a system configuration
  # option that can be adjusted according to the reliability of the underlying storage. The
  # replicas of an object are assigned to storage machines based on a pure function of the
  # object ID.

  getStatus @0 () -> (version :UInt64, maybeLeader :Leader);
  # Returns this replica's version of the object as well as the leader it is following (or null
  # if it is not currently following any leader).

  follow @1 (leader :WeakLeader) -> (follower :InitFollower, version :UInt64, dirty :Bool,
                                     time :Sibling.Time, lastTerm :List(Sibling.Time));
  # Tells the callee to begin following the caller.
  #
  # `version` is the version number as of the last committed transaction or sync. If this is not
  # the newest version, the leader will need to overwrite this follower with data from the newest
  # replica. A version number of zero indicates that the replica has no data at all about this
  # object.
  #
  # If `data` is true, then it is possible that some non-transactional write()s have occurred which
  # are not reflected by `version`. This is unfortunate as it means that the replica will either
  # need to be completely overwritten with data from another replica, or the data from this
  # replica will need to be used to completely replace data from all other replicas.
  #
  # `time` is the current time on the replica. `lastTerm` is the vector clock representing the
  # start time of the term of the leader who most recently wrote to this replica.
}

interface Leader {
  # Interface exposed by leaders to followers.

  getObject @0 (key :ObjectKey) -> (cap :OwnedStorage);
  # Get the high-level representation of the object.

  startTransaction @1 () -> (builder :TransactionBuilder);
  # Starts a transaction on the object.

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

interface WeakLeader {
  # A weak reference to a Leader. Each Follower holds one of these. It has to be a weak reference
  # because otherwise it would be cyclic.

  get @0 () -> (leader :Leader);
}

interface InitFollower {
  # Before a Leader can issue any write to a Follower, it must specify the term start time.
  # This class enforces that.

  startTerm @0 (vectorTime :List(Sibling.Time)) -> (follower :Follower);
}

interface Follower {
  # Interface that an object leader uses to broadcast transactions to replicas. A `Replica`
  # is associated with a specific object, not a machine.

  commit @0 (version :UInt64, txn :RawTransaction);
  # Atomically apply a transaction.
  #
  # A quorum of replicas must respond successfully to this call before the change can be
  # acknowledged to the client.
  #
  # If a failure occurs with less that a quorum having committed the transaction, then it is
  # possible that the transaction will be reverted on recovery, if the next leader happens to be
  # elected by a quorum not including the ones who committed the transaction.

  stageDistributed @1 (id :TransactionId, version :UInt64, txn :RawTransaction)
                   -> (staged :StagedTransaction);
  # Stage a multi-object transaction. Subsequent commit()s will be blocked until this transaction
  # is either committed or aborted. If `staged` is dropped without calling either commit() or
  # abort(), then the replica will contact the transaction coordinator directly to ask it
  # whether the transaction has completed.

  write @2 (offset :UInt64, data :Data);
  # Perform a non-transactional write.
  #
  # Performing a write also implicitly switches the follower into "direct mode", which is only
  # allowed when no transaction is staged. Staging a transaction changes the follower back into
  # "transactional mode".
  #
  # It is a bad idea to disconnect from the follower while still in direct mode, because expensive
  # recovery may be needed to ensure consistency between replicas the next time the object is
  # opened. Therefore, a leader should always commit a no-op transaction to return the object to
  # a clean state before dropping it.

  sync @3 (version :UInt64);
  # Ensures all previous writes have completed, then sets the object version to `version`. The
  # object remains in direct mode.

  replace @4 () -> (replacer :Replacer);
  # Initiates replacing the entire Follower contents with new data. The content will stream in over
  # the `Replacer` capability.

  interface Replacer {
    write @0 (offset :UInt64, data :Data);
    commit @1 (version :UInt64, changes :RawTransaction);
  }
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

  applyRaw @1 (changes :RawTransaction);
  # Add some direct low-level modifications.

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
  # ID can be reused so long as the old ID has been globally forgotten. The ID is not related to
  # any other numbers and is not necessarily sequential.
}

struct RawTransaction {
  create @0 :UInt8 = 0;
  # Type is ObjectType enum from basics.h. 0 means to open, not create.

  setContent @1 :Data;
  # Overwrite content. null = don't overwrite.

  shouldDelete @2 :Bool = false;

  shouldBecomeReadOnly @3 :Bool = false;

  adjustTransitiveBlockCount @4 :Int64 = 0;
  # Delta vs. current value.

  setParent @5 :ObjectId;
  # null = don't change

  backburnerModifyTransitiveBlockCount @6 :Int64 = 0;
  # Schedule backburner task to modify this object's transitive block count and recurse to its
  # parent.

  backburnerRecursivelyDelete @7 :Bool = false;
  # Schedule backburner task to eventually delete this object and recurse to its children.

  shouldClearBackburner @8 :Bool = false;
  # Remove all backburner tasks attached to this object.
}

struct TermInfo {
  startTime @0 :List(Sibling.Time);
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
