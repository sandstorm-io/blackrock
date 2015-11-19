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
using ChangeSet = import "basics.capnp".ChangeSet;

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
  # key. (No object is created on-disk until a method of the factory is invoked.)

  getReplica @1 (id :ObjectId) -> (replica :Replica);
  # Get the replica of the given object maintained by this node. Not valid to call if this node is
  # not a member of the object's replica set. *Is* valid to call for a new object being created:
  # the replica will report that the object is at version 0 and has no data.

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

  getState @0 () -> (time :Sibling.Time, dataState :DataState, followState :FollowState);
  # Returns this replica's current state.

  struct DataState {
    union {
      doesntExist @0 :Void;
      # No object with this ID exists.

      exists @1 :ReplicaState;
    }
  }

  struct FollowState {
    union {
      idle @0 :Voter;
      # Not currently following any leader. The given capability can be used to make this replica
      # follow a new leader. The capability is revoked if the replica is asked to follow any
      # other leader in the meantime.

      following :group {
        leader @1 :WeakLeader;
        term @2 :TermInfo;
      }
    }
  }
}

interface Leader {
  # Interface exposed by leaders to followers.

  getObject @0 (key :ObjectKey) -> (cap :OwnedStorage);
  # Get the high-level representation of the object.

  startTransaction @1 () -> (builder :TransactionBuilder);
  # Starts a transaction on the object.

  flushTransaction @2 (id :TransactionId);
  # Ask the Leader to please make sure that the given transaction, which was possibly staged
  # previously, has been fully-applied. After this call returns successfully, it is safe for the
  # transaction coordinator to forget about the transaction and return false from
  # `getTransactionState()`.

  getTransactionState @3 (id :TransactionId) -> (committed :Bool);
  # Get the state of a transaction for which the callee is the coordinator. If the named
  # transaction is still staged but neither committed nor aborted, waits for it to reach either
  # the commit or the abort state. If nothing about the transaction is known, returns false; but
  # this only happens if the coordinator previously ensured tha the transaction had been committed.
  #
  # This call travels in the opposite direction of stageTransaction() and flushTransaction() and is
  # only used to recover in the case of a crash / partition.
}

interface WeakLeader {
  # A weak reference to a Leader. Each Follower holds one of these. It has to be a weak reference
  # because otherwise it would be cyclic.

  get @0 () -> (leader :Leader);
}

interface Voter {
  vote @0 (leader :WeakLeader, term :TermInfo, writeQuorumSize :UInt8) -> (follower :Follower);
  # If successful, the voter has voted for this leader and is now following it.
}

interface Follower {
  # Interface that an object leader uses to broadcast transactions to replicas. A `Replica`
  # is associated with a specific object, not a machine.

  commit @0 (version :UInt64, changes :ChangeSet);
  # Atomically apply a transaction.
  #
  # A quorum of replicas must respond successfully to this call before the change can be
  # acknowledged to the client.
  #
  # If a failure occurs with less that a quorum having committed the transaction, then it is
  # possible that the transaction will be reverted on recovery, if the next leader happens to be
  # elected by a quorum not including the ones who committed the transaction.

  stageDistributed @1 (txn :DistributedTransactionInfo)
                   -> (staged :StagedTransaction);
  # Stage a multi-object transaction. Subsequent commit()s will be blocked until this transaction
  # is either committed or aborted. If `staged` is dropped without calling either commit() or
  # abort(), then the replica will contact the transaction coordinator directly to ask it
  # whether the transaction has completed.
  #
  # The follower state only transitions to `version` if the transaction commits, not if it aborts.
  # This is important because if the follower stages the transaction and then goes offline, a
  # quorum of other followers might successfully commit the transaction and then the coordinator
  # might erase the transaction record, giving the offline follower no way to discover whether the
  # transaction committed or aborted once it comes back. The follower will assume abort, but if
  # it actually committed then other followers will be at a newer version and this follower will
  # be recovered accordingly.

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
    commit @1 (version :UInt64, changes :ChangeSet);
  }

  copyTo @5 (replacer :Replacer, version :UInt64);
  # Copy the entire contents of this follower *into* the designated replacer.

  # TODO(now): Way to add a new transaction that we're coordinating (probably as option to
  #   commit()).
  # TODO(now): Way to update the quorum size.
}

interface TransactionBuilder {
  getTransactional @0 [T] (object :T) -> (transactionalObject :T);
  # Get a transactional wrapper around the given object. `object` must be a capability representing
  # one of the facets of the storage object with which this Transaction is associated. For exmaple,
  # when transacting on an Assingable, `object` may be a capability to the `Assignable` itself or
  # to an `Assignable.Setter` pointing to the same underlying object.
  #
  # Once either `stage()` is called or the `TransactionBuilder` is dropped, `transactionalObject`
  # will be revoked.

  applyRaw @1 (changes :ChangeSet);
  # Add some direct low-level modifications.

  stage @2 (coordinator :ObjectId, id :TransactionId) -> (staged :StagedTransaction);
  # Prepare to commit the transaction. If this succeeds, then the underlying object has been
  # locked. All other transactions will be blocked/rejected until this transaction is either
  # committed or aborted.
}

interface StagedTransaction {
  # A transaction that has been staged and is ready to commit. While the transaction is staged,
  # no conflicting transactions are allowed to be created, thus ensuring that the transaction
  # can be committed whenever the coordinator is ready.
  #
  # If a StagedTransaction becomes disconnected without commit() or abort() successfully being
  # called, then the callee will need to independently determine whether the commit or abort
  # condition was reached by calling back to the coordinator and using
  # `Leader.getTransactionState()`. The coordinator itself may attempt to force the issue by
  # calling `Leader.flushTransactions()` on the target object. None of this usually happens,
  # though, because normally commit() or abort() returns successfully.
  #
  # Once commit() or abort() returns successfully, the callee guarantees that the transaction's
  # effects are durable and the coordinator can forget about the transaction itself. Any future
  # call to Leader.getTransactionState() querying about this transaction could only be from a
  # straggler follower who is about to undergo recovery anyway; the coordinator may return `false`
  # for such queries even if the transaction was actually committed.

  commit @0 ();
  # Asserts that the transaction's commit condition has been reached. The callee need not
  # independently verify.

  abort @1 ();
  # Asserts that the transaction's abort condition has been reached. The callee need not
  # independently verify.
  #
  # An aborted transaction does NOT update the version number.
}

struct TransactionId {
  # Globally-unique ID for a specific multi-object transaction.

  id0 @0 :UInt64;
  id1 @1 :UInt64;
}

struct TermInfo {
  startTime @0 :List(Sibling.Time);

  # TODO:
  # - leader info?
  # - object storage version from which we started?
}

struct ReplicaState {
  # State of a single object replica as of the last time it was written. This includes pieces from
  # Xattr, TemporaryXattr, and the recoverable temporary file.

  type @0 :UInt8;
  isReadOnly @1 :Bool;
  isDirty @2 :Bool;
  version @3 :UInt64;
  transitiveBlockCount @4 :UInt64;
  owner @5 :ObjectId;
  # From Xattr.

  pendingRemoval @6 :Bool;
  # If true, a backburner task is scheduled to delete this.

  pendingSizeUpdate @7 :Int64;
  # If non-zero, a backburner task is scheduled to update the size.

  extended @8 :Extended;
  # Extended state -- this is serialized into a temporary file, hence being a separate struct.

  struct Extended {
    # The body of the temporary file storing the object state. We try to put things here that don't
    # change often so that we don't have to transact on this file too much.

    term @0 :TermInfo;
    # The leadership term under which the last write was made.

    staged @1 :DistributedTransactionInfo;
    # If present, a distributed transaction is staged and awaiting notification of commit or abort.
    # This is added between writes and cleared on every version bump.

    coordinated @2 :List(TransactionId);
    # Recent distributed transactions for which this object was the coordinator which were
    # successfully committed.

    effectiveWriteQuorumSize @3 :UInt8;
    # The last completed transaction was written to at least this many replicas.
    #
    # Any transaction can reduce this number (assuming that the election quorum size has already
    # been increased as needed), and can be committed under the new requirement.
    #
    # Increasing this size requires first completing a transaction under the new requirement, before
    # the new requirement is actually stored in the state or the election quorum size reduced. This
    # ensures that once the new value hits disk on a single follower, the state of the full system
    # already complies with it.
  }
}

struct DistributedTransactionInfo {
  coordinator @0 :ObjectId;
  id @1 :TransactionId;
  version @2 :UInt64;
  changes @3 :ChangeSet;

  # TODO(now): Should the coordinator be identified by a SturdyRef rather than ObjectId and
  #   TransactionId?
}

struct StorageConfig {
  replicasPerObject @0 :UInt8;
  # Number of replicas of each object.
  #
  # It is safe to decrement this size if writeQuorumSize is decremented at the same time, and
  # remains non-zero.
  #
  # It is safe to increase this size if electionQuorumSize is incremented at the same time.

  electionQuorumSize @1 :UInt8;
  # How many votes does it take to elect a leader? This number must be at least more than half of
  # replicasPerObject.

  writeQuorumSize @2 :UInt8;
  # How many replicas must accept a write before it can be acknowledged to the client?
  #
  # This number must be at least enough so that electionQuorumSize + writeQuorumSize is greater
  # than replicasPerObject.
  #
  # Increasing writeQuorumSize requires a transition period in which a leader is elected for each
  # object using the old electionQuorumSize but the new writeQuorumSize, and that leader must
  # successfully acknowledge at least one write.

  # TODO: How do we express transitioning between two configs?
  # TODO: How do we express transitioning to a new sibling count?
  # TODO: How do we express rolling updates with fewer than three replicas per object? Should we
  #   always require booting 2x replicas in that case?
}
