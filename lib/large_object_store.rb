require "large_object_store/version"

module LargeObjectStore

  def self.wrap(store)
    RailsWrapper.new(store)
  end

  class RailsWrapper
    attr_reader :store

    LIMIT = 1024**2 - 100

    def initialize(store)
      @store = store
    end

    def write(key, value, options = {})
      value = Marshal.dump(value)
      
      # store number of pages
      pages = (value.size / LIMIT.to_f).ceil
      @store.write("#{key}_0", pages, options)

      # store object
      page = 1
      loop do
        slice = value.slice!(0, LIMIT)
        break if slice.size == 0

        @store.write("#{key}_#{page}", slice, options)
        page += 1
      end

      true
    end

    def read(key)
      # read pages
      pages = @store.read("#{key}_0")
      return if pages.nil?

      # read sliced data
      keys = Array.new(pages).each_with_index.map{|_,i| "#{key}_#{i+1}" }
      slices = @store.read_multi(*keys).values
      return nil if slices.compact.size < pages
      Marshal.load(slices.join(""))
    end

    def fetch(key, options={})
      value = read(key)
      return value unless value.nil?
      value = yield
      write(key, value, options)
      value
    end
  end
end
