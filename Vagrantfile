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

  config.vm.provider "virtualbox" do |v|
    v.memory = 1024
    v.cpus = 1
  end

  config.vm.define "storage0" do |storage0|
    config.vm.network "private_network", ip: "172.28.128.10"
  end
  config.vm.define "worker0" do |worker0|
    config.vm.network "private_network", ip: "172.28.128.20"
  end
  config.vm.define "worker1" do |worker1|
    config.vm.network "private_network", ip: "172.28.128.21"
  end
  config.vm.define "worker2" do |worker2|
    config.vm.network "private_network", ip: "172.28.128.22"
  end
  config.vm.define "worker3" do |worker3|
    config.vm.network "private_network", ip: "172.28.128.23"
  end
  config.vm.define "coordinator0" do |coordinator0|
    config.vm.network "private_network", ip: "172.28.128.30"
  end
  config.vm.define "frontend0" do |frontend0|
    config.vm.network "private_network", ip: "172.28.128.31"
  end
  config.vm.define "frontend1" do |frontend1|
    config.vm.network "private_network", ip: "172.28.128.32"
  end
end
