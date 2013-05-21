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
end

describe LargeObjectStore do
  let(:store) { LargeObjectStore.wrap(TestCache.new) }

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
    store.store.should_receive(:write).with(anything, anything, :expires_in => 111)
    store.write("a", "a", :expires_in => 111)
  end

  it "passes options when caching big" do
    store.store.should_receive(:write).with(anything, anything, :expires_in => 111).exactly(3).times
    store.write("a", "a"*1_200_000, :expires_in => 111)
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

  it "uses necessary keys" do
    store.write("a", "a"*5_000_000)
    store.store.keys.should == ["a_0", "a_1", "a_2", "a_3", "a_4", "a_5"]
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
end
