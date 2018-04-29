# Blackrock

Blackrock is an extension to [Sandstorm](https://github.com/sandstorm-io/sandstorm) that allows a single instance to scale across a cluster of machines. It is used to power [Sandstorm Oasis](https://oasis.sandstorm.io), the managed hosting service offered by [Sandstorm.io](https://sandstorm.io).

See the [Blackrock roadmap](https://github.com/sandstorm-io/sandstorm/tree/master/roadmap/blackrock) for a design overview.

## Running locally

WARNING: This runs six VMs, and the number may increase in the future. It also allocates disk images totalling 20GB, although they are sparse images so won't actually use that much space on your drive unless you use them a lot.

First, some prep:

* This has only been tested on Debian Sid. On other distros, YMMV. (Patches welcome.)
* You will need Vagrant installed.
* If you want to build with modified version of Sandstorm, make sure that `deps/sandstorm` symlinks to your Sandstorm source tree. Hint: You can symlink `deps` to `..` if Blockrock is checked out next to Sandstorm.
* You may want to edit `test-config.capnp` to add your Stripe test key and Mailchip key, if you want to test those features. Otherwise, leave them commented out.

To run locally:

    make run-local

This will take a very long time the first time it runs, but once all the VMs are up you'll be able to ctrl+C and re-run quickly.

Your instance will be accessible at: http://localrock.sandstorm.io:6080/

To create an admin token:

    make local-admintoken

Then go to: http://localrock.sandstorm.io:6080/setup/token/testtoken

To get a Mongo shell:

    make local-mongo

To shut down:

    make kill-local

## Deploying

Please talk to us on [sandstorm-dev](https://groups.google.com/group/sandstorm-dev).

If you are deploying on Google Compute Engine, this may be relatively easy. On any other infrastructure, a new `ComputeDriver` will be needed. See `src/blackrock/gce.{h,c++}` to see how this is implemented for GCE. Perhaps you'd like to contribute an implementation for another service?
