Contributing to this project
----------

Pull Requests from all are welcomed!

Got a specific problem you want to fix? Great!
First please check in [waffle](https://waffle.io/basho-labs/riak-mesos) to see if
there's already an issue created.

If there's one under `In Progress`, somebody is already working on it.
If it's under `Ready`, please give us a heads-up on the ticket that you are planning on working on it.
If there's no issue created - neat! You found a novel bug - create an issue with any detail
you can provide and let us know that you're planning on working on it.

Once you're done, create a PR from your branch.

If you're not planning on working on it, please raise the issue with any detail you
have. Bonus points for reproducable test cases!

Building
---------

To build a useable tarball of this component, you'll need to have installed:

 - [erlang](http://docs.basho.com/riak/kv/2.1.4/setup/installing/source/erlang/)
 - mesos
 - marathon


One quick way to get a working environment set up is to use our vagrant environment:

```
 $ git clone https://github.com/basho-labs/riak-mesos
 $ cd riak-mesos
 $ vagrant up
 $ vagrant ssh
 $ cd /vagrant/framework/riak-mesos-scheduler
```

Once those are installed, simply run:

```
 $ make tarball
```

This will fetch all locked dependencies, compile and build a release package. The name
of the last built package on your machine will be in `./packages/local.txt`.


Once this package is built, you can reference it in your `config.json` for the framework,
normally in `/etc/riak-mesos/config.json`.


Upgrading a dependency
----------

We use the [rebar_lock_deps_plugin](https://github.com/seth/rebar_lock_deps_plugin) to keep
the exact versions of our dependencies locked down for simplicity.

To upgrade a dependency, simply change its entry in `rebar.config` to point to the new
version, and run:

```
 $ make lock
```

NB: this command deletes all dependencies from the `./deps/` folder, and re-fetches them
according to the new versions.

Once it completes, check what has changed in `rebar.config.lock`:

```
 $ git diff
```

Ideally, only the dependency you just upgraded will have changed in `rebar.config.lock`.
If your new dependency version pulls in newer versions of its own dependencies, check
that they are also changed appropriately.

Once you are happy with the changes, commit them!

```
 $ git add rebar.config rebar.config.lock
 $ git commit -m "Upgrade dependency foo@1.2.3"
```
