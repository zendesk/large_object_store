require "large_object_store/version"
require "zlib"
require "securerandom"

module LargeObjectStore
  UUID_BYTES = 16
  UUID_SIZE = UUID_BYTES * 2
  CACHE_VERSION = 2
  MAX_OBJECT_SIZE = 1024**2
  ITEM_HEADER_SIZE = 100
  DEFAULT_COMPRESS_LIMIT = 16*1024
  COMPRESSED = 'z'
  NORMAL = '0'

  def self.wrap(store)
    RailsWrapper.new(store)
  end

  class RailsWrapper
    attr_reader :store

    def initialize(store)
      @store = store
    end

    def write(key, value, options = {})
      value = Marshal.dump(value)

      options = options.dup
      compressed = false
      if options.delete(:compress)
        # Don't pass compression on to Rails, we're doing it ourselves.
        compress_limit = options.delete(:compress_limit) || DEFAULT_COMPRESS_LIMIT
        if value.bytesize > compress_limit
          value = Zlib::Deflate.deflate(value)
          compressed = true
        end
      end
      value.prepend(compressed ? COMPRESSED : NORMAL)

      # calculate slice size; note that key length is a factor because
      # the key is stored on the same slab page as the value
      slice_size = MAX_OBJECT_SIZE - ITEM_HEADER_SIZE - UUID_SIZE - key.bytesize

      # store number of pages
      pages = (value.size / slice_size.to_f).ceil

      if pages == 1
        @store.write(key(key, 0), value, options)
      else
        # store meta
        uuid = SecureRandom.hex(UUID_BYTES)
        return false unless @store.write(key(key, 0), [pages, uuid], options) # invalidates the old cache

        # store object
        page = 1
        loop do
          slice = value.slice!(0, slice_size)
          break if slice.size == 0

          return false unless @store.write(key(key, page), slice.prepend(uuid), options.merge(raw: true))
          page += 1
        end
        true
      end
    end

    def read(key)
      # read pages
      pages, uuid = @store.read(key(key, 0))
      return if pages.nil?

      data = if pages.is_a?(Fixnum)
        # read sliced data
        keys = Array.new(pages).each_with_index.map{|_,i| key(key, i+1) }
        slices = @store.read_multi(*keys).values
        return nil if slices.compact.size != pages
        slices.map! { |s| [s.slice!(0, UUID_SIZE), s] }
        return nil unless slices.map(&:first).uniq == [uuid]
        slices.map!(&:last).join("")
      else
        pages
      end

      if data.slice!(0, 1) == COMPRESSED
        data = Zlib::Inflate.inflate(data)
      end

      Marshal.load(data)
    end

    def fetch(key, options={})
      value = read(key)
      return value unless value.nil?
      value = yield
      write(key, value, options)
      value
    end

    def delete(key)
      @store.delete(key(key, 0))
    end

    private

    def key(key, i)
      "#{key}_#{CACHE_VERSION}_#{i}"
    end
  end
end
