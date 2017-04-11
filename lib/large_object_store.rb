require "large_object_store/version"
require "zlib"
require "securerandom"

module LargeObjectStore
  UUID_BYTES = 16
  UUID_SIZE = UUID_BYTES * 2
  CACHE_VERSION = 3
  MAX_OBJECT_SIZE = 1024**2
  ITEM_HEADER_SIZE = 100
  DEFAULT_COMPRESS_LIMIT = 16*1024
  NORMAL = 0
  COMPRESSED = 1
  RAW = 2
  RADIX = 32 # we can store 32 different states

  def self.wrap(store)
    RailsWrapper.new(store)
  end

  class RailsWrapper
    attr_reader :store

    def initialize(store)
      @store = store
    end

    def write(key, value, options = {})
      options = options.dup
      value = serialize(value, options)

      # calculate slice size; note that key length is a factor because
      # the key is stored on the same slab page as the value
      slice_size = MAX_OBJECT_SIZE - ITEM_HEADER_SIZE - UUID_SIZE - key.bytesize

      # store number of pages
      pages = (value.size / slice_size.to_f).ceil

      if pages == 1
        !!@store.write(key(key, 0), value, options)
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
        keys = (1..pages).map { |i| key(key, i) }
        # use values_at to enforce key order because read_multi doesn't guarantee a return order
        slices = @store.read_multi(*keys).values_at(*keys)
        return nil if slices.compact.size != pages
        slices = slices.map { |s| [s[0...UUID_SIZE], s[UUID_SIZE..-1]] }
        return nil unless slices.map(&:first).uniq == [uuid]
        slices.map(&:last).join("")
      else
        pages
      end

      deserialize(data)
    end

    def fetch(key, options={})
      value = read(key)
      return value unless value.nil?
      value = yield
      write(key, value, options)
      value
    end

    def exist?(key)
      @store.exist?(key(key, 0))
    end

    def delete(key)
      @store.delete(key(key, 0))
    end

    private

    # convert a object to a string
    # modifies options
    def serialize(value, options)
      flag = NORMAL

      if options.delete(:raw)
        flag |= RAW
        value = value.to_s
      else
        value = Marshal.dump(value)
      end

      if compress?(value, options)
        flag |= COMPRESSED
        value = Zlib::Deflate.deflate(value)
      end

      value.prepend(flag.to_s(RADIX))
    end

    # opposite operations and order of serialize
    def deserialize(data)
      flag, data = data[0].to_i(RADIX), data[1..-1]
      data = Zlib::Inflate.inflate(data) if flag & COMPRESSED == COMPRESSED
      data = Marshal.load(data) if flag & RAW != RAW
      data
    end

    # Don't pass compression on to Rails, we're doing it ourselves.
    def compress?(value, options)
      return unless options.delete(:compress)
      compress_limit = options.delete(:compress_limit) || DEFAULT_COMPRESS_LIMIT
      value.bytesize > compress_limit
    end

    def key(key, i)
      "#{key}_#{CACHE_VERSION}_#{i}"
    end
  end
end
