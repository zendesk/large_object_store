Store large objects in memcache or others by slicing them.
 - uses read_multi for fast access
 - returns nil if one slice is missing

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
```

Author
======
[Ana Martinez](https://github.com/anamartinez)<br/>
acemacu@gmail.com<br/>
[Michael Grosser](https://github.com/grosser)<br/>
michael@grosser.it<br/>
License: MIT<br/>
[![Build Status](https://travis-ci.org/anamartinez/large_object_store.png)](https://travis-ci.org/anamartinez/large_object_store)
