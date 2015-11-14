# encoding: utf-8
require "spec_helper"

class TestCache
  def initialize
    @data = {}
  end

  def write(k,v, options={})
    v = Marshal.dump(v)
    return false if v.bytesize > 1024**2
    @data[k] = v
    true
  end

  def read(k)
    real_read(k)
  end

  def real_read(k)
    v = @data[k]
    v.nil? ? nil : Marshal.load(v)
  end

  def read_multi(*keys)
    Hash[keys.map{|k| [k, real_read(k)] }]
  end

  def keys
    @data.keys
  end

  def delete(key)
    @data.delete(key)
  end
end

describe LargeObjectStore do
  let(:cache) { TestCache.new }
  let(:store) { LargeObjectStore.wrap(cache) }

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
    store.store.should_receive(:write).with("a_0", 2, :expires_in => 111).exactly(1).times.and_return(true)
    store.write("a", "a"*1_200_000, :expires_in => 111)
  end

  it "returns false when underlying write fails" do
    store.store.should_receive(:write).with(anything, anything, :raw => true).exactly(2).times.and_return(true)
    store.store.should_receive(:write).with("a_0", 2, {}).exactly(1).times.and_return(false)
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

  it "cannot read corrupted objects" do
    store.write("a", ["a"*10_000_000]).should == true
    store.store.write("a_4", nil)
    store.read("a").should == nil
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
    store.write("a", "a"*100_000_000).should == true
    store.read("a").size.should == 100_000_000
  end

  it "can read/write compressed objects" do
    s = "compress me"
    store.write("a", s, :compress => true).should == true
    store.store.read("a_0").should == Zlib::Deflate.deflate(Marshal.dump(s))
    store.read("a").should == s
  end

  it "can read/write giant compressed objects" do
    s = SecureRandom.hex(5_000_000)
    store.write("a", s, :compress => true).should == true
    store.store.read("a_0").should_not == ["a_0"]
    store.store.read("a_1").should start_with "x" # zlib magic
    store.read("a").should == s
  end

  it "adjusts slice size for key length" do
    store.write("a", "a"*100_000_000).should == true
    store.store.read("a_1").size.should == 1048576 - 100 - 1

    key="a"*250
    store.write(key, "a"*100_000_000).should == true
    store.store.read("#{key}_1").size.should == 1048576 - 100 - 250
  end

  it "uses necessary keys" do
    store.write("a", "a"*5_000_000)
    store.store.keys.sort.should == ["a_0", "a_1", "a_2", "a_3", "a_4", "a_5"]
  end

  it "uses 1 key when value is small enough" do
    store.write("a", "a"*500_000)
    store.store.keys.should == ["a_0"]
  end

  it "uses read_multi" do
    store.write("a", "a"*5_000_000)
    store.store.should_receive(:read).with("a_0").and_return 5
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
      store.read("a").should_not == nil
      store.delete("a")
      store.read("a").should == nil
    end
  end
end
