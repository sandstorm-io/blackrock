# Sandstorm Blackrock
# Copyright (c) 2015 Sandstorm Development Group, Inc.
# All Rights Reserved

# Because Blackrock messes with kernel knobs that can break the system (like
# nbd), it's preferable to run non-unit tests inside a VM with Vagrant.
#
# Cheat sheet:
#    vagrant up        Initializes and starts a VM, with the source directory
#                      mapped read-only at /vagrant.
#    vagrant ssh       SSHes into the VM.
#    vagrant destroy   Shuts down and deletes the VM. I recommend this over
#                      `vagrant halt` to keep your dev environment clean.

VAGRANTFILE_API_VERSION = "2"

Vagrant.configure(VAGRANTFILE_API_VERSION) do |config|
  # TODO(soon): Use something more-recent. We want the latest and greatest
  #   kernel features.
  config.vm.box = "ubuntu/trusty64"

  # We build Blackrock outside of Vagrant, so there's no reason for the VM
  # to be modifying the source directory. Mount it read-only.
  config.vm.synced_folder ".", "/vagrant", :mount_options => ["ro"]

  # Don't check for image updates on every run; could be slow.
  config.vm.box_check_update = false
end
