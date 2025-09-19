# frozen_string_literal: true

require "large_object_store/version"
require "zlib"
require "zstd-ruby"
require "securerandom"

module LargeObjectStore
  UUID_BYTES = 16
  UUID_SIZE = UUID_BYTES * 2
  CACHE_VERSION = 4
  MAX_OBJECT_SIZE = 1024**2
  ITEM_HEADER_SIZE = 100
  DEFAULT_COMPRESS_LIMIT = 16 * 1024
  NORMAL = 0
  COMPRESSED = 1
  RAW = 2
  FLAG_RADIX = 32 # we can store 32 different states
  ZSTD_MAGIC = (+"\x28\xB5\x2F\xFD").force_encoding("ASCII-8BIT")
  ZSTD_COMPRESS_LEVEL = 3 # Default level recommended by zstd authors

  def self.wrap(*args)
    RailsWrapper.new(*args)
  end
  class << self
    ruby2_keywords :wrap if respond_to?(:ruby2_keywords, true)
  end

  class RailsWrapper
    attr_reader :store

    def initialize(store, serializer: Marshal, max_slice_size: MAX_OBJECT_SIZE)
      @store = store
      @serializer = serializer
      @max_slice_size = [max_slice_size, MAX_OBJECT_SIZE].min
      @namespace = (store.respond_to?(:options) && store.options[:namespace]) || ""
    end

    def write(key, value, **options)
      options = options.dup
      value = serialize(value, options)

      slice_size = safe_slice_size(key)
      # store number of pages
      pages = (value.size / slice_size.to_f).ceil

      if pages == 1
        !!@store.write(key(key, 0), value, **options)
      else
        # store meta
        uuid = SecureRandom.hex(UUID_BYTES)
        return false unless @store.write(key(key, 0), [pages, uuid], **options) # invalidates the old cache

        # store object
        page = 1
        loop do
          slice = value.slice!(0, slice_size)
          break if slice.size == 0

          return false unless @store.write(key(key, page), slice.prepend(uuid), raw: true, **options)
          page += 1
        end
        true
      end
    end

    def read(key)
      # read pages
      pages, uuid = @store.read(key(key, 0))
      return if pages.nil?

      data = if pages.is_a?(Integer)
        # read sliced data
        keys = (1..pages).map { |i| key(key, i) }
        # use values_at to enforce key order because read_multi doesn't guarantee a return order
        slices = @store.read_multi(*keys, raw: true).values_at(*keys)
        return nil if slices.compact.size != pages

        slices = slices.map do |slice|
          s = slice.dup
          [s.slice!(0, UUID_SIZE), s]
        end

        return nil unless slices.map(&:first).uniq == [uuid]

        slices.map!(&:last).join("")
      else
        pages
      end

      deserialize(data)
    end

    def fetch(key, **options)
      value = read(key)
      return value unless value.nil?
      value = yield
      write(key, value, **options)
      value
    end

    def exist?(key)
      @store.exist?(key(key, 0))
    end

    def delete(key)
      @store.delete(key(key, 0))
    end

    private

    # calculate slice size; note that key length is a factor because
    # the key is stored on the same slab page as the value
    def safe_slice_size(key)
      namespace_length = @namespace.empty? ? 0 : @namespace.size + 1
      overhead = ITEM_HEADER_SIZE + UUID_SIZE + key.bytesize + namespace_length
      slice_size = @max_slice_size - overhead
      if slice_size <= 0
        MAX_OBJECT_SIZE - overhead
      else
        slice_size
      end
    end

    # convert a object to a string
    # modifies options
    def serialize(value, options)
      flag = NORMAL

      if options.delete(:raw)
        flag |= RAW
        value = value.to_s
      else
        value = @serializer.dump(value)
      end

      if compress?(value, options)
        flag |= COMPRESSED
        value = compress(value, options)
      end

      "#{flag.to_s(FLAG_RADIX)}#{value}"
    end

    def compress(value, options)
      if options[:zstd]
        Zstd.compress(value, level: ZSTD_COMPRESS_LEVEL)
      else
        Zlib::Deflate.deflate(value)
      end
    end

    def decompress(data)
      if data.start_with?(ZSTD_MAGIC)
        Zstd.decompress(data)
      else
        Zlib::Inflate.inflate(data)
      end
    end

    # opposite operations and order of serialize
    def deserialize(raw_data)
      data = raw_data.dup
      flag = data.slice!(0, 1).to_i(FLAG_RADIX)
      data = decompress(data) if flag & COMPRESSED == COMPRESSED
      data = @serializer.load(data) if flag & RAW != RAW
      data
    end

    # Don't pass compression on to Rails, we're doing it ourselves.
    def compress?(value, options)
      return unless options[:compress]

      compress_limit = options[:compress_limit] || DEFAULT_COMPRESS_LIMIT
      should_compress = value.bytesize > compress_limit

      if should_compress
        # Pass compress: false to Rails in case the default is true
        options[:compress] = false
        options.delete(:compress_limit)
      end

      should_compress
    end

    def key(key, i)
      "#{key}_#{CACHE_VERSION}_#{i}"
    end
  end
end
