Store large objects in memcache or others by slicing them.
 - uses read_multi for fast access
 - returns nil if one slice is missing
 - low performance overhead, only uses single read/write if data is below 1MB

Install
=======

```Bash
gem install large_object_store
```

Usage
=====

```Ruby
Rails.cache.write("a", "a"*10_000_000) # => false -> oops too large

store = LargeObjectStore.wrap(Rails.cache)
store.write("a", "a"*10_000_000)  # => true -> always!
store.read("a").size              # => 10_000_000 using multi_get
store.read("b")                   # => nil
store.fetch("a"){ "something" }   # => "something" executes block on miss
store.write("a" * 10_000_000, compress: true)                # compress when greater than 16k
store.write("a" * 1000, compress: true, compress_limit: 100) # compress when greater than 100
store.write("a" * 1000, raw: true)                           # store as string to avoid marshaling overhead
```

zstd
====

[zstd compression](https://engineering.fb.com/2016/08/31/core-data/smaller-and-faster-data-compression-with-zstandard/), a modern improvement over the venerable zlib compression algorithm, is supported by passing the `zstd` flag when writing items:

```
store.write("a" * 10_000_000, compress: true, zstd: true)
```

For backwards compatibility and to enable safe roll-out of the change in working systems, the `zstd` flag defaults to `false`.

zstd decompression is used when the zstd magic number is detected at the beginning of compressed data, so `zstd: true` does not need to be passed when reading/fetching items.

Author
======

[Ana Martinez](https://github.com/anamartinez)
acemacu@gmail.com
[Michael Grosser](https://github.com/grosser)
michael@grosser.it
License: MIT
[![CI](https://github.com/anamartinez/large_object_store/actions/workflows/actions.yml/badge.svg)](https://github.com/anamartinez/large_object_store/actions/workflows/actions.yml)

### Releasing a new version
A new version is published to RubyGems.org every time a change to `version.rb` is pushed to the `main` branch.
In short, follow these steps:
1. Update `version.rb`,
2. update version in all `Gemfile.lock` files,
3. merge this change into `main`, and
4. look at [the action](https://github.com/zendesk/large_object_store/actions/workflows/publish.yml) for output.

To create a pre-release from a non-main branch:
1. change the version in `version.rb` to something like `1.2.0.pre.1` or `2.0.0.beta.2`,
2. push this change to your branch,
3. go to [Actions → “Publish to RubyGems.org” on GitHub](https://github.com/zendesk/large_object_store/actions/workflows/publish.yml),
4. click the “Run workflow” button,
5. pick your branch from a dropdown.
