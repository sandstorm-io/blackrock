# Blackrock

See [Blackrock roadmap](https://github.com/sandstorm-io/roadmap/tree/master/technology/blackrock).

## Running locally

WARNING: This runs six VMs, and the number may increase in the future. It also allocates disk images totalling 20GB, although they are sparse images so won't actually use that much space on your drive unless you use them a lot.

First, make sure that the Sansdtorm "prod" repo is checked out at `../prod`. We need to pull config details from this repo, such as Stripe and Mailchimp keys. These keys should not be committed to the Blackrock repo, in case we want to open source it someday.

To run locally:

    make run-local

Your instance will be accessible at: http://localrock.sandstorm.io:6080/

Note that migrations may take some time on first run (especially Mailchimp), but a disk image of the database and storage are persisted in `.local` and will be reused on future runs.

To create an admin token:

    make local-admintoken

Then go to: http://localrock.sandstorm.io:6080/admin/testtoken

To get a Mongo shell:

    make local-mongo

To shut down:

    make kill-local

## Deploying to Testrock

This section requires having permission to deploy to the `sandstorm-blackrock-testing` GCE project.

To build a new Testrock release:

    ./release.sh test

This builds a new disk image, but does not update nor otherwise disturb the live service.

To deploy it, **from the prod repo**:

    ./push-blackrock.sh test

This will:

1. Replace the service with a "scheduled maintenance" notice.
2. Restart the master on the new version. (The master will in turn recreate all the VMs using the new image.)
3. Tail the log so you can watch progress. Once you see the frontends report migration complete, press ctrl+C. (Note that before then, you'll probably spend 30-60 seconds watching the front-ends repeatedly crash complaining about the Mongo oplog URL not being the replica set leader. This is because Mongo has not yet elected itself leader; be patient.)
4. Take down the maintenance notice, allowing traffic back in. (Note that for the first few minutes as caches warm up, apps may be extremely slow to load.)

If you were deploying to Oasis rather than Testrock, you'd replace `test` with `prod` in the above commands. This requires access to the `sandstorm-oasis` GCE project.
