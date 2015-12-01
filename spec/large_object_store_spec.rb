# encoding: utf-8
require "spec_helper"
require "active_support/cache"
require "active_support/cache/dalli_store"

# compression indicator is in first position for single page values and after the uuid for multi page values
RSpec::Matchers.define :be_compressed do
  match do |actual|
    actual[LargeObjectStore::UUID_SIZE] == LargeObjectStore::COMPRESSED || actual[0] == LargeObjectStore::COMPRESSED
  end
end

RSpec::Matchers.define :be_uncompressed do
  match do |actual|
    actual[LargeObjectStore::UUID_SIZE] == LargeObjectStore::NORMAL || actual[0] == LargeObjectStore::NORMAL
  end
end

describe LargeObjectStore do
  let(:cache) { ActiveSupport::Cache::DalliStore.new "localhost:11211" }
  let(:store) { LargeObjectStore.wrap(cache) }
  let(:version) { LargeObjectStore::CACHE_VERSION }

  before { cache.clear }

  it "has a VERSION" do
    LargeObjectStore::VERSION.should =~ /^[\.\da-z]+$/
  end

  it "wraps and returns a wrapper" do
    store.class.should == LargeObjectStore::RailsWrapper
  end

  it "can write/read big objects" do
    store.write("a", "a"*10_000_000).should == true
    store.read("a").size.should == 10_000_000
  end

  it "passes options when caching small" do
    store.store.should_receive(:write).with(anything, anything, :expires_in => 111).and_return(true)
    store.write("a", "a", :expires_in => 111)
  end

  it "passes options when caching big" do
    store.store.should_receive(:write).with(anything, anything, :expires_in => 111, :raw => true).exactly(2).times.and_return(true)
    store.store.should_receive(:write).with("a_#{version}_0", [2, anything], :expires_in => 111).exactly(1).times.and_return(true)
    store.write("a", "a"*1_200_000, :expires_in => 111)
  end

  it "returns false when underlying write fails" do
    store.store.should_not_receive(:write).with(anything, anything, :raw => true)
    store.store.should_receive(:write).with("a_#{version}_0", [2, anything], {}).exactly(1).times.and_return(false)
    store.write("a", "a"*1_200_000).should == false
  end

  it "reads back small objects of various types as they were written" do
    store.write("a", "hello")
    store.read("a").should == "hello"
    store.write("a", 123)
    store.read("a").should == 123
    store.write("a", [1, 2, 3])
    store.read("a").should == [1, 2, 3]
  end

  it "cannot read incomplete objects" do
    store.write("a", ["a"*10_000_000]).should == true
    store.store.delete("a_#{version}_4")
    store.read("a").nil?.should == true
  end

  it "cannot read corrupted keys from parallel processes" do
    store.write("a", "a"*5_000_000)
    store.store.write("a_#{version}_3", 'xxx', raw: true)
    store.read("a").to_s.size.should == 0
  end

  it "can write/read big non-string objects" do
    store.write("a", ["a"*10_000_000]).should == true
    store.read("a").first.size.should == 10_000_000
  end

  it "can read/write objects with encoding" do
    store.write("a", "ß"*10_000_000).should == true
    store.read("a").size.should == 10_000_000
  end

  it "can write/read giant objects" do
    size = 20_000_000 # more then that seems to break local memcached ...
    store.write("a", "a"*size).should == true
    store.read("a").size.should == size
    store.store.read("a_#{version}_1").should be_uncompressed
  end

  describe "compression" do
    it "does not compress small objects" do
      s = "compress me"
      store.write("a", s, :compress => true).should == true
      store.read("a").should == s
      store.store.read("a_#{version}_0").should be_uncompressed
    end

    it "can read/write compressed non-string objects" do
      s = ["x"] * 10000
      store.write("a", s, :compress => true).should == true
      store.read("a").should == s
      store.store.read("a_#{version}_0").should be_compressed
    end

    it "compresses large objects" do
      s = "x" * 25000
      store.write("a", s, :compress => true).should == true
      store.read("a").should == s
      store.store.read("a_#{version}_0").should be_compressed
    end

    it "compresses objects larger than optional compress_limit" do
      s = "compress me"
      len = s.length
      store.write("a", s, :compress => true, :compress_limit => len-1).should == true
      store.read("a").should == s
      store.store.read("a_#{version}_0").should be_compressed
    end

    it "does not compress objects smaller than optional compress limit" do
      s = "don't compress me"
      len = s.length
      store.write("a", s, :compress => true, :compress_limit => len*2).should == true
      store.read("a").should == s
      store.store.read("a_#{version}_0").should be_uncompressed
      end

    it "can read/write giant compressed objects" do
    s = SecureRandom.hex(5_000_000)
    store.write("a", s, :compress => true).should == true
    store.store.read("a_#{version}_0").first.should == 6
    store.store.read("a_#{version}_1").should be_compressed
    store.read("a").size.should == s.size
    end
  end

  it "adjusts slice size for key length" do
    store.write("a", "a"*20_000_000).should == true
    store.store.read("a_#{version}_1").size.should == 1048576 - 100 - 1

    key="a"*250
    store.write(key, "a"*20_000_000).should == true
    store.store.read("#{key}_#{version}_1").size.should == 1048576 - 100 - 250
  end

  it "uses necessary keys" do
    store.write("a", "a"*5_000_000)
    ["a_#{version}_0", "a_#{version}_1", "a_#{version}_2", "a_#{version}_3", "a_#{version}_4", "a_#{version}_5", "a_#{version}_6"].map do |k|
      store.store.read(k).class
    end.should == [Array, String, String, String, String, String, NilClass]
  end

  it "uses 1 key when value is small enough" do
    store.write("a", "a"*500_000)
    ["a_#{version}_0", "a_#{version}_1"].map do |k|
      store.store.read(k).class
    end.should == [String, NilClass]
  end

  it "uses read_multi" do
    store.write("a", "a"*5_000_000)
    expected = store.store.read("a_#{version}_0")
    store.store.should_receive(:read).with("a_#{version}_0").and_return expected
    store.read("a").size.should == 5_000_000
  end

  describe "#fetch" do
    it "executes the block on miss" do
      store.fetch("a"){ 1 }.should == 1
    end

    it "does not execute the block on hit" do
      store.fetch("a"){ 1 }
      store.fetch("a"){ 2 }.should == 1
    end

    it "passes the options" do
      store.should_receive(:write).with(anything, anything, :expires_in => 111)
      store.fetch("a", :expires_in => 111){ 2 }
    end

    it "can fetch false" do
      store.fetch("a"){ false }.should == false
      store.read("a").should == false
    end
  end

  describe "#delete" do
    it "removes all keys" do
      store.write("a", "a"*5_000_000)
      store.read("a").nil?.should == false
      store.delete("a")
      store.read("a").nil?.should == true
    end
  end
end
