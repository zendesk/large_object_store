name = "large_object_store"
require "./lib/#{name}/version"

Gem::Specification.new name, LargeObjectStore::VERSION do |s|
  s.summary = "Store large objects in memcache or others"
  s.authors = ["Ana Martinez"]
  s.email = "acemacu@gmail.com"
  s.homepage = "https://github.com/anamartinez/#{name}"
  s.files = `git ls-files lib Readme.md`.split("\n")
  s.license = "MIT"
  s.required_ruby_version = '>= 2.6'
  s.add_runtime_dependency('zstd-ruby', '~> 1.5.5')
end
