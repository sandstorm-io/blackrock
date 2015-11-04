// Sandstorm Blackrock
// Copyright (c) 2015 Sandstorm Development Group, Inc.
// All Rights Reserved

#include "bundle.h"
#include <unistd.h>
#include <sched.h>
#include <syscall.h>
#include <sys/mount.h>
#include <errno.h>
#include <limits.h>
#include <sys/sendfile.h>
#include <sys/signal.h>
#include <sys/types.h>
#include <grp.h>
#include <sandstorm/util.h>
#include <sandstorm/spk.h>

namespace blackrock {

#define BUNDLE_PATH "/blackrock/bundle"

void createSandstormDirectories() {
  kj::StringPtr paths[] = {
    "/var/blackrock",
    "/var/blackrock/bundle",
    "/var/blackrock/bundle/sandstorm",
    "/var/blackrock/bundle/sandstorm/socket",
    "/var/blackrock/bundle/mongo",
    "/var/blackrock/bundle/log",
    "/var/blackrock/bundle/pid",
    "/tmp/blackrock-bundle"
  };

  if (access("/tmp/blackrock-bundle", F_OK) >= 0) {
    sandstorm::recursivelyDelete("/tmp/blackrock-bundle");
  }
  for (auto path: paths) {
    mkdir(path.cStr(), (path.startsWith("/tmp/") ? S_ISVTX | 0770 : 0750));
    KJ_SYSCALL(chown(path.cStr(), 1000, 1000));
  }
}

void enterSandstormBundle() {
  // Set up a small sandbox located inside the Sandstorm (i.e. non-Blackrock) bundle, for running
  // things like the front-end and Mongo.
  //
  // TODO(cleanup): Extend Subprocess to support a lot of these things?

  // Enter mount namespace so that we can bind stuff in.
  KJ_SYSCALL(unshare(CLONE_NEWNS));

  KJ_SYSCALL(chdir(BUNDLE_PATH));

  // To really unshare the mount namespace, we also have to make sure all mounts are private.
  // The parameters here were derived by strace'ing `mount --make-rprivate /`.  AFAICT the flags
  // are undocumented.  :(
  KJ_SYSCALL(mount("none", "/", nullptr, MS_REC | MS_PRIVATE, nullptr));

  // Make sure that the current directory is a mount point so that we can use pivot_root.
  KJ_SYSCALL(mount(".", ".", nullptr, MS_BIND | MS_REC, nullptr));

  // Now change directory into the new mount point.
  char cwdBuf[PATH_MAX + 1];
  if (getcwd(cwdBuf, sizeof(cwdBuf)) == nullptr) {
    KJ_FAIL_SYSCALL("getcwd", errno);
  }
  KJ_SYSCALL(chdir(cwdBuf));

  // Bind /proc for the global pid namespace in the chroot.
  KJ_SYSCALL(mount("/proc", "proc", nullptr, MS_BIND | MS_REC, nullptr));

  // Bind /var and /tmp.
  KJ_SYSCALL(mount("/tmp/blackrock-bundle", "tmp", nullptr, MS_BIND, nullptr));
  KJ_SYSCALL(mount("/var/blackrock/bundle", "var", nullptr, MS_BIND, nullptr));

  // Bind desired devices from /dev into our chroot environment.
  KJ_SYSCALL(mount("/dev/null", "dev/null", nullptr, MS_BIND, nullptr));
  KJ_SYSCALL(mount("/dev/zero", "dev/zero", nullptr, MS_BIND, nullptr));
  KJ_SYSCALL(mount("/dev/random", "dev/random", nullptr, MS_BIND, nullptr));
  KJ_SYSCALL(mount("/dev/urandom", "dev/urandom", nullptr, MS_BIND, nullptr));

  // Mount a tmpfs at /etc and copy over necessary config files from the host.
  // Note that unlike regular Sandstorm, we don't bother bind-mounting in the host etc, because
  // we don't expect to have to deal with dynamic network configs.
  KJ_SYSCALL(mount("tmpfs", "etc", "tmpfs", MS_NOSUID | MS_NOEXEC,
                   kj::str("size=2m,nr_inodes=128,mode=755,uid=0,gid=0").cStr()));
  {
    auto files = sandstorm::splitLines(sandstorm::readAll("host.list"));

    // Now copy over each file.
    for (auto& file: files) {
      if (access(file.cStr(), R_OK) == 0 && !sandstorm::isDirectory(file)) {
        auto in = sandstorm::raiiOpen(file, O_RDONLY);
        auto out = sandstorm::raiiOpen(kj::str(".", file), O_WRONLY | O_CREAT | O_EXCL);
        ssize_t n;
        do {
          KJ_SYSCALL(n = sendfile(out, in, nullptr, 1 << 20));
        } while (n > 0);
      }
    }
  }

  // pivot_root into the frontend dir. (This is just a fancy more-secure chroot.)
  KJ_SYSCALL(syscall(SYS_pivot_root, ".", "tmp"));
  KJ_SYSCALL(chdir("/"));
  KJ_SYSCALL(umount2("tmp", MNT_DETACH));

  // Drop privileges. Since we own the machine we can choose any UID, just don't want it to be 0.
  KJ_SYSCALL(setresgid(1000, 1000, 1000));
  KJ_SYSCALL(setgroups(0, nullptr));
  KJ_SYSCALL(setresuid(1000, 1000, 1000));

  // Clear signal mask. Not strictly a sandboxing measure, just cleanup.
  // TODO(cleanup): We should probably discard any signals in this mask which are currently pending
  //   before we unblock them. We should probably fix this in Sandstorm as well.
  sigset_t sigset;
  KJ_SYSCALL(sigemptyset(&sigset));
  KJ_SYSCALL(sigprocmask(SIG_SETMASK, &sigset, nullptr));
}

kj::Maybe<kj::String> checkPgpSignatureInBundle(
    kj::StringPtr appIdString, sandstorm::spk::Metadata::Reader metadata) {
  createSandstormDirectories();

  auto pipe = sandstorm::Pipe::make();

  sandstorm::Subprocess child([&]() -> int {
    enterSandstormBundle();

    pipe.readEnd = nullptr;

    KJ_IF_MAYBE(s, sandstorm::checkPgpSignature(appIdString, metadata)) {
      kj::FdOutputStream(pipe.writeEnd.get()).write(s->begin(), s->size());
    }

    return 0;
  });

  pipe.writeEnd = nullptr;
  kj::String result = sandstorm::readAll(pipe.readEnd);

  child.waitForSuccess();
  if (result == nullptr) {
    return nullptr;
  } else {
    return kj::mv(result);
  }
}

} // namespace blackrock

