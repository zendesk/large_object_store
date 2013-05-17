require "spec_helper"

describe LargeObjectStore do
  it "has a VERSION" do
    LargeObjectStore::VERSION.should =~ /^[\.\da-z]+$/
  end
end
