class Octopus::ScopeProxy
  attr_accessor :shard, :klass

  def initialize(shard, klass)
    @shard = shard
    @klass = klass
  end

  def using(shard)
    raise "Nonexistent Shard Name: #{shard}" if @klass.connection.instance_variable_get(:@shards)[shard].nil?
    @shard = shard
    return self
  end

  # Transaction Method send all queries to a specified shard.
  def transaction(options = {}, &block)
    @klass.connection.run_queries_on_shard(@shard) do
      @klass = @klass.connection().transaction(options, &block)
    end
  end

  def connection
    @klass.connection().current_shard = @shard
    @klass.connection()
  end

  def method_missing(method, *args, &block)
    result = nil
    @klass.connection.run_queries_on_shard(@shard) do
      result = @klass.send(method, *args, &block)
      @klass = result if result.respond_to?(:connection)
    end

    return result if result.is_a?(ActiveRecord::Base) or result.is_a?(Array) or result.is_a?(Fixnum) or result.nil?
    return self
  end

  def ==(other)
    @shard == other.shard
    @klass == other.klass
  end
end
