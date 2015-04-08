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

  # Note: If you get an error about "DHCP server already exists", run the
  # following command once (on the host):
  #     VBoxManage dhcpserver remove --netname HostInterfaceNetworking-vboxnet0
  # See:
  #     https://github.com/mitchellh/vagrant/issues/3083
  config.vm.network "private_network", type: "dhcp"

  config.vm.define "storage0" do |storage0|
  end
  config.vm.define "worker0" do |worker0|
  end
  config.vm.define "worker1" do |worker1|
  end
  config.vm.define "worker2" do |worker2|
  end
  config.vm.define "worker3" do |worker3|
  end
  config.vm.define "coordinator0" do |coordinator0|
  end
  config.vm.define "frontend0" do |frontend0|
  end
  config.vm.define "frontend1" do |frontend1|
  end
end
