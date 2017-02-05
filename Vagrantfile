# Sandstorm Blackrock
# Copyright (c) 2015 Sandstorm Development Group, Inc.
# All Rights Reserved
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Because Blackrock messes with kernel knobs that can break the system (like
# nbd), it's preferable to run non-unit tests inside a VM with Vagrant.
#
# Cheat sheet:
#    vagrant up        Initializes and starts a VM, with the source directory
#                      mapped read-only at /blackrock.
#    vagrant ssh       SSHes into the VM.
#    vagrant destroy   Shuts down and deletes the VM. I recommend this over
#                      `vagrant halt` to keep your dev environment clean.

VAGRANTFILE_API_VERSION = "2"

Vagrant.configure(VAGRANTFILE_API_VERSION) do |config|
  config.vm.box = "sandstorm/debian-jessie64"

  # We build Blackrock outside of Vagrant, so there's no reason for the VM
  # to be modifying the source directory. Mount it read-only.
  config.vm.synced_folder ".", "/blackrock", :mount_options => ["ro"]

  # The directory ".local" should contain two ext4 disk images: "storage"
  # and "mongo". Server state will be stored in these so that
  # "vagrant destroy"ing all VMs and bringing them back up doesn't mean
  # wiping storage. To create this directory, do something like:
  #
  #     mkdir .local
  #     truncate -s 10737418240 .local/storage
  #     truncate -s 10737418240 .local/mongo
  #     /sbin/mkfs.ext4 .local/storage
  #     /sbin/mkfs.ext4 .local/mongo
  config.vm.synced_folder ".local", "/blackrock-local"

  # Don't check for image updates on every run; could be slow.
  config.vm.box_check_update = false

  config.vm.provider "virtualbox" do |v|
    v.memory = 1024
    v.cpus = 1
  end

  config.vm.define "storage0" do |storage0|
    storage0.vm.network "private_network", ip: "172.28.128.10"

    storage0.vm.provision "shell",
        inline: "mkdir -p /var/blackrock/storage && mount /blackrock-local/storage /var/blackrock/storage",
        run: "always"
  end
  config.vm.define "worker0" do |worker0|
    worker0.vm.network "private_network", ip: "172.28.128.20"
  end
  config.vm.define "worker1" do |worker1|
    worker1.vm.network "private_network", ip: "172.28.128.21"
  end
  config.vm.define "worker2" do |worker2|
    worker2.vm.network "private_network", ip: "172.28.128.22"
  end
  config.vm.define "worker3" do |worker3|
    worker3.vm.network "private_network", ip: "172.28.128.23"
  end
  config.vm.define "coordinator0" do |coordinator0|
    coordinator0.vm.network "private_network", ip: "172.28.128.30"
  end
  config.vm.define "frontend0" do |frontend0|
    frontend0.vm.network "private_network", ip: "172.28.128.40"
  end
  config.vm.define "frontend1" do |frontend1|
    frontend1.vm.network "private_network", ip: "172.28.128.41"
  end
  config.vm.define "mongo0" do |mongo0|
    mongo0.vm.network "private_network", ip: "172.28.128.50"

    mongo0.vm.provision "shell",
        inline: "mkdir -p /var/blackrock/bundle && mount /blackrock-local/mongo /var/blackrock/bundle",
        run: "always"
  end
end
