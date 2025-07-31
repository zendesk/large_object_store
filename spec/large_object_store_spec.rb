require "spec_helper"
require "active_support"
require "yaml"

describe LargeObjectStore do
  # flag is in first position for single page values and after the uuid for multi page values
  def type(data, kind)
    type = case kind
    when :single then data[0]
    when :multi then data[LargeObjectStore::UUID_SIZE]
    else raise "Unknown kind #{kind}"
    end

    case type
    when "0" then :normal
    when "1" then :compressed
    when "2" then :raw
    when "3" then :raw_compressed
    else :unknown
    end
  end

  def with_namespace(store, namespace)
    store.instance_variable_set(:@namespace, namespace)
    yield
    store.instance_variable_set(:@namespace, "")
  end

  stores = [ActiveSupport::Cache::MemoryStore.new]

  begin
    require "active_support/cache/dalli_store"
    stores << ActiveSupport::Cache::DalliStore.new("localhost:#{ENV["MEMCACHED_PORT"] || "11211"}")
    warn "Using ActiveSupport::Cache::DalliStore from dalli v2.x"
  rescue LoadError
    ActiveSupport.cache_format_version = 7.1
    stores << ActiveSupport::Cache::MemCacheStore.new("localhost:#{ENV["MEMCACHED_PORT"] || "11211"}")
  end

  stores.each do |cache_instance|
    describe "with #{cache_instance.class} as the base store" do
      let(:cache) { cache_instance }
      let(:store) { LargeObjectStore.wrap(cache) }
      let(:custom_slice_store) { LargeObjectStore.wrap(cache, max_slice_size: 100_000) }
      let(:version) { LargeObjectStore::CACHE_VERSION }

      before do
        cache.clear
        puts "limit: #{cache.stats.dig("localhost:#{ENV.fetch("MEMCACHED_PORT", "11211")}", "limit_maxbytes")}" if cache.respond_to?(:stats)
      end

      it "has a VERSION" do
        expect(LargeObjectStore::VERSION).to match(/^[\.\da-z]+$/)
      end

      it "wraps and returns a wrapper" do
        expect(store.class).to eq(LargeObjectStore::RailsWrapper)
      end

      it "can write/read big objects" do
        expect(store.write("a", "a" * 10_000_000)).to eq(true)
        expect(store.read("a").size).to eq(10_000_000)
        expect(store.read("a").size).to eq(10_000_000)
      end

      it "can write/read small objects" do
        expect(store.write("a", {})).to eq(true)
        expect(store.read("a")).to eq({})
        expect(store.read("a")).to eq({})
      end

      it "passes options when caching small" do
        expect(store.store).to receive(:write).with(anything, anything, {expires_in: 111}).and_return(true)
        store.write("a", "a", expires_in: 111)
      end

      it "passes options when caching big" do
        expect(store.store).to receive(:write).with(anything, anything, expires_in: 111, raw: true).exactly(2).times.and_return(true)
        expect(store.store).to receive(:write).with("a_#{version}_0", [2, anything], expires_in: 111).exactly(1).times.and_return(true)
        store.write("a", "a" * 1_200_000, expires_in: 111)
      end

      it "returns false when underlying write fails" do
        expect(store.store).not_to receive(:write).with(anything, anything, raw: true)
        expect(store.store).to receive(:write).with("a_#{version}_0", [2, anything]).exactly(1).times.and_return(false)
        expect(store.write("a", "a" * 1_200_000)).to eq(false)
      end

      it "reads back small objects of various types as they were written" do
        store.write("a", "hello")
        expect(store.read("a")).to eq("hello")
        store.write("a", 123)
        expect(store.read("a")).to eq(123)
        store.write("a", [1, 2, 3])
        expect(store.read("a")).to eq([1, 2, 3])
      end

      it "cannot read incomplete objects" do
        expect(store.write("a", ["a" * 10_000_000])).to eq(true)
        store.store.delete("a_#{version}_4")
        expect(store.read("a").nil?).to eq(true)
      end

      it "cannot read corrupted keys from parallel processes" do
        store.write("a", "a" * 5_000_000)
        store.store.write("a_#{version}_3", "xxx", raw: true)
        expect(store.read("a").to_s.size).to eq(0)
      end

      it "can write/read big non-string objects" do
        expect(store.write("a", ["a" * 10_000_000])).to eq(true)
        expect(store.read("a").first.size).to eq(10_000_000)
      end

      it "can read/write objects with encoding" do
        expect(store.write("a", "ß" * 10_000_000)).to eq(true)
        expect(store.read("a").size).to eq(10_000_000)
      end

      it "can write/read giant objects" do
        size = 20_000_000 # more then that seems to break local memcached ...
        expect(store.write("a", "a" * size)).to eq(true)
        expect(store.read("a").size).to eq(size)
        expect(type(store.store.read("a_#{version}_1", raw: true), :multi)).to eq(:normal)
      end

      describe "raw" do
        it "stores non-raw as raw" do
          expect(store.write("a", 1, raw: true)).to eq(true)
          expect(store.read("a")).to eq("1")
        end

        it "can read and write small objects with raw" do
          expect(store.write("a", "a", raw: true)).to eq(true)
          expect(store.read("a").size).to eq(1)
          expect(store.store.read("a_#{version}_0").size).to eq(2)
        end

        it "can read and write large objects with raw" do
          expect(store.write("a", "a" * 10_000_000, raw: true)).to eq(true)
          expect(store.read("a").size).to eq(10_000_000)
          expect(type(store.store.read("a_#{version}_1", raw: true), :multi)).to eq(:raw)
        end

        it "can read and write compressed raw" do
          expect(store.write("a", "a", raw: true, compress: true, compress_limit: 0)).to eq(true)
          expect(store.read("a")).to eq("a")
          expect(type(store.store.read("a_#{version}_0"), :single)).to eq(:raw_compressed)
        end

        it "can read and write compressed zstd raw" do
          expect(store.write("a", "a", raw: true, zstd: true, compress: true, compress_limit: 0)).to eq(true)
          expect(store.read("a")).to eq("a")
          expect(type(store.store.read("a_#{version}_0"), :single)).to eq(:raw_compressed)
        end
      end

      describe "compression" do
        it "does not compress small objects" do
          s = "compress me"
          expect(store.write("a", s, compress: true)).to eq(true)
          expect(store.read("a")).to eq(s)
          expect(type(store.store.read("a_#{version}_0"), :single)).to eq(:normal)
        end

        [false, true].each do |zstd|
          describe "with zstd=#{zstd}" do
            before do
              if zstd
                # Zlib shouldn't be called
                expect(Zlib::Deflate).not_to receive(:deflate)
                expect(Zlib::Inflate).not_to receive(:inflate)
              else
                # Zstd shouldn't be called
                expect(Zstd).not_to receive(:compress)
                expect(Zstd).not_to receive(:decompress)
              end
            end

            it "can read/write compressed non-string objects" do
              s = ["x"] * 10000
              expect(store.write("a", s, compress: true, zstd: zstd)).to eq(true)
              expect(store.read("a")).to eq(s)
              expect(type(store.store.read("a_#{version}_0"), :single)).to eq(:compressed)
            end

            it "compresses large objects" do
              s = "x" * 25000
              expect(store.write("a", s, compress: true, zstd: zstd)).to eq(true)
              expect(store.read("a")).to eq(s)
              expect(type(store.store.read("a_#{version}_0"), :single)).to eq(:compressed)
            end

            it "compresses objects larger than optional compress_limit" do
              s = "compress me"
              len = s.length
              expect(store.write("a", s, compress: true, zstd: zstd, compress_limit: len - 1)).to eq(true)
              expect(store.read("a")).to eq(s)
              expect(type(store.store.read("a_#{version}_0"), :single)).to eq(:compressed)
            end

            it "does not compress objects smaller than optional compress limit" do
              s = "don't compress me"
              len = s.length
              expect(store.write("a", s, compress: true, zstd: zstd, compress_limit: len * 2)).to eq(true)
              expect(store.read("a")).to eq(s)
              expect(type(store.store.read("a_#{version}_0"), :single)).to eq(:normal)
            end

            it "can read/write giant compressed objects" do
              s = SecureRandom.hex(5_000_000)
              expect(store.write("a", s, compress: true, zstd: zstd)).to eq(true)
              expect(store.store.read("a_#{version}_0").first).to be_between(5, 6)
              expect(type(store.store.read("a_#{version}_1", raw: true), :multi)).to eq(:compressed)
              expect(store.read("a").size).to eq(s.size)
            end
          end
        end
      end

      it "adjusts slice size for key length" do
        expect(store.write("a", "a" * 20_000_000)).to eq(true)
        expect(store.store.read("a_#{version}_1", raw: true).size).to eq(1048576 - 100 - 1)

        key = "a" * 250
        expect(store.write(key, "a" * 20_000_000)).to eq(true)
        expect(store.store.read("#{key}_#{version}_1", raw: true).size).to eq(1048576 - 100 - 250)
      end

      it "adjusts slice size for namespace length" do
        with_namespace(store, "los") do
          expect(store.write("a", "a" * 20_000_000)).to eq(true)
          expect(store.store.read("a_#{version}_1", raw: true).size).to eq(1048576 - 100 - 4 - 1)

          key = "a" * 250
          expect(store.write(key, "a" * 20_000_000)).to eq(true)
          expect(store.store.read("#{key}_#{version}_1", raw: true).size).to eq(1048576 - 100 - 250 - 4)
        end
      end

      it "adjusts slice size for custom max_slice_size" do
        expect(custom_slice_store.write("a", "a"*20_000_000)).to eq(true)
        expect(custom_slice_store.store.read("a_#{version}_1", raw:true).size).to eq(100_000 - 100 - 1)

        key="a"*250
        expect(custom_slice_store.write(key, "a"*20_000_000)).to eq(true)
        expect(custom_slice_store.store.read("#{key}_#{version}_1", raw:true).size).to eq(100_000 - 100 - 250)
      end

      it "uses necessary keys" do
        store.write("a", "a" * 5_000_000)
        expect(["a_#{version}_0", "a_#{version}_1", "a_#{version}_2", "a_#{version}_3", "a_#{version}_4", "a_#{version}_5", "a_#{version}_6"].map.with_index do |k, i|
          store.store.read(k, raw: !i.zero?).class
        end).to eq([Array, String, String, String, String, String, NilClass])
      end

      it "uses 1 key when value is small enough" do
        store.write("a", "a" * 500_000)
        expect(["a_#{version}_0", "a_#{version}_1"].map do |k|
          store.store.read(k).class
        end).to eq([String, NilClass])
      end

      it "uses read_multi" do
        store.write("a", "a" * 5_000_000)
        expected = store.store.read("a_#{version}_0")
        expect(store.store).to receive(:read).with("a_#{version}_0").and_return expected
        expect(store.read("a").size).to eq(5_000_000)
      end

      it "handles read_multi returning results in any order" do
        store.write("a", "c" * 5_000_000) # don't use 'a' because it is a valid flag option
        keys = ["a_#{version}_1", "a_#{version}_2", "a_#{version}_3", "a_#{version}_4", "a_#{version}_5"]
        out_of_order_hash = keys.reverse.each_with_object({}) do |k, h|
          h[k] = store.store.read(k, raw: true)
        end
        expect(store.store).to receive(:read_multi).and_return out_of_order_hash
        expect(store.read("a").size).to eq(5_000_000)
      end

      describe "#fetch" do
        it "executes the block on miss" do
          expect(store.fetch("a") { 1 }).to eq(1)
        end

        it "does not execute the block on hit" do
          store.fetch("a") { 1 }
          expect(store.fetch("a") { 2 }).to eq(1)
        end

        it "passes the options" do
          expect(store).to receive(:write).with(anything, anything, expires_in: 111)
          store.fetch("a", expires_in: 111) { 2 }
        end

        it "can fetch false" do
          expect(store.fetch("a") { false }).to eq(false)
          expect(store.read("a")).to eq(false)
        end
      end

      describe "#exist?" do
        it "returns false if it isn't in the cache" do
          expect(store.exist?("a")).to eq(false)
        end

        it "returns true if the 0th key is in the cache" do
          store.write("b", "foo")
          expect(store.exist?("b")).to eq(true)
        end
      end

      describe "#delete" do
        it "removes all keys" do
          store.write("a", "a" * 5_000_000)
          expect(store.read("a").nil?).to eq(false)
          store.delete("a")
          expect(store.read("a").nil?).to eq(true)
        end
      end

      describe "when a custom serializer is specified" do
        let(:store) { LargeObjectStore.wrap(cache, serializer: YAML) }

        it "uses the custom serializer" do
          value = {"foo" => "bar"}
          json = YAML.dump(value)
          expect(YAML).to receive(:dump).with(value).and_call_original
          store.fetch("a") { value }
          expect(YAML).to receive(:load).with(json).and_call_original
          expect(store.read("a")).to eq(value)
        end
      end
    end
  end
end
