$LOAD_PATH.unshift File.expand_path("../lib", __FILE__)
name = "large_object_store"
require "#{name.gsub("-","/")}/version"

Gem::Specification.new name, LargeObjectStore::VERSION do |s|
  s.summary = "Store large objects in memcache or others"
  s.authors = ["Ana Martinez"]
  s.email = "acemacu@gmail.com"
  s.homepage = "http://github.com/anamartinez/#{name}"
  s.files = `git ls-files`.split("\n")
  s.license = "MIT"
  cert = File.expand_path("~/.ssh/gem-private_key.pem")
  if File.exist?(cert)
    s.signing_key = cert
    s.cert_chain = ["gem-public_cert.pem"]
  end
end
