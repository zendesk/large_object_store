require "large_object_store"
require "securerandom"
require "testcontainers"

RSpec.configure do |config|
  config.add_setting :memcached_container

  config.before(:suite) do
    config.memcached_container = Testcontainers::DockerContainer.new("memcached:latest").with_exposed_port(11211)
    config.memcached_container.start
  end

  config.after(:suite) do
    config.memcached_container.stop if config.memcached_container.running?
    config.memcached_container.delete
  end
end
