require 'resque-lock-timeout'

# A simple job class that processes a given index.
#
class ThinkingSphinx::Deltas::ResqueDelta::DeltaJob

  extend Resque::Plugins::LockTimeout
  @queue = :ts_delta
  @lock_timeout = 240
  # @redis = Redis::Namespace.new(:incident_deltas, redis: Redis.new(host: (server_address if Rails.env.production?)))
  @redis = Redis::Namespace.new(:incident_deltas, redis: Redis.new(host: self.server_address))

  REDIS_SET_NAME = "incident_deltas"
  NUMBER_OF_INCIDENT_DELTAS = 5

  # Runs Sphinx's indexer tool to process the index. Currently assumes Sphinx
  # is running.
  #
  # @param [String] index the name of the Sphinx index
  #
  def self.perform(index)
    if skip?(index) || delta_is_running(index)
      puts "Indexer is allready running for #{index} index. skipping this run"
      return
    end

    config = ThinkingSphinx::Configuration.instance
    master_index = index.sub("_delta","_core")
    full_index_indication = "/tmp/#{master_index}"

    run_full = File.exist?(full_index_indication) || !File.exist?("#{config.indices_location}/#{master_index}.spl")

    if run_full
      model_class = index.gsub("_delta","")
      model_table = if model_class == "contracts_contract"
        Contracts::Contract
      elsif model_class.start_with?("incident") # For supporting multiple indices, that look like "incident_index_1"
        Incident
      else
        model_class.classify.constantize
      end

      max_delta_update = model_table.select("max(updated_at) as max_updated_at").where(["delta=?",1]).first.max_updated_at
    end

    # Delta Index
    config.controller.index index, :verbose => !config.settings['quiet_deltas']

    if run_full
      File.delete(full_index_indication) if File.exist?(full_index_indication)
      output = `#{config.controller.bin_path}#{config.controller.indexer_binary_name} --config #{config.configuration_file} #{master_index} --rotate`
      puts output

      if model_table == Incident
        update_incident_deltas(index, max_delta_update)
      else
        model_table.where(['delta=? AND updated_at<?', 1, max_delta_update]).update_all("delta=0")
      end
    end

    #@atlantis: far as I can tell, ThinkingSphinx handles this automatically https://groups.google.com/forum/?fromgroups=#!topic/thinking-sphinx/_fGTfqJXog0

    # Flag As Deleted
    #return unless ThinkingSphinx.sphinx_running?

    #index = ThinkingSphinx::Deltas::ResqueDelta::IndexUtils.delta_to_core(index)

    # Get the document ids we've saved
    # flag_as_deleted_ids = ThinkingSphinx::Deltas::ResqueDelta::FlagAsDeletedSet.processing_members(index)

    # unless flag_as_deleted_ids.empty?
    #   # Filter out the ids that aren't present in sphinx
    #   flag_as_deleted_ids = filter_flag_as_deleted_ids(flag_as_deleted_ids, index)

    #   unless flag_as_deleted_ids.empty?
    #     # Each hash element should be of the form { id => [1] }
    #     flag_hash = Hash[*flag_as_deleted_ids.collect {|id| [id, [1]] }.flatten(1)]

    #     config.client.update(index, ['sphinx_deleted'], flag_hash)
    #   end
    # end
  end

  def self.delta_is_running(index)
    output = `ps -ef | grep #{index} | grep rotate`
    output.include? "#{index} --rotate"
  end

  # Try again later if lock is in use.
  def self.lock_failed(*args)
    Resque.enqueue(self, *args)
  end

  # Run only one DeltaJob at a time regardless of index.
  #def self.identifier(*args)
    #nil
  #end

  # This allows us to have a concurrency safe version of ts-delayed-delta's
  # duplicates_exist:
  #
  # http://github.com/freelancing-god/ts-delayed-delta/blob/master/lib/thinkin
  # g_sphinx/deltas/delayed_delta/job.rb#L47
  #
  # The name of this method ensures that it runs within around_perform_lock.
  #
  # We've leveraged resque-lock-timeout to ensure that only one DeltaJob is
  # running at a time. Now, this around filter essentially ensures that only
  # one DeltaJob of each index type can sit at the queue at once. If the queue
  # has more than one, lrem will clear the rest off.
  #
  def self.around_perform_lock1(*args)
    # Remove all other instances of this job (with the same args) from the
    # queue. Uses LREM (http://code.google.com/p/redis/wiki/LremCommand) which
    # takes the form: "LREM key count value" and if count == 0 removes all
    # instances of value from the list.
    redis_job_value = Resque.encode(:class => self.to_s, :args => args)
    Resque.redis.lrem("queue:#{@queue}", 0, redis_job_value)

    # Grab the subset of flag as deleted document ids to work on
    # core_index = ThinkingSphinx::Deltas::ResqueDelta::IndexUtils.delta_to_core(*args)
    #ThinkingSphinx::Deltas::ResqueDelta::FlagAsDeletedSet.get_subset_for_processing(core_index)

    yield

    # Clear processing set
    #ThinkingSphinx::Deltas::ResqueDelta::FlagAsDeletedSet.clear_processing(core_index)
  end

  protected

  def self.skip?(index)
    ThinkingSphinx::Deltas::ResqueDelta.locked?(index)
  end

  def self.server_address
    byebug
    resque_config = YAML.load_file("#{Rails.root.to_s}/config/resque.yml")
    resque_config[Rails.env]['redis_server']
  end

  def self.update_incident_deltas(index, update_time)
    mutex = Redis::Semaphore.new(:incident_deltas_mutex, stale_client_timeout: 600)
    mutex.lock do
      puts "workers completed: #{@redis.keys('incident_index*')}"

      unless @redis.exists(index)
        @redis.setex(index, 21600, update_time) # delete after 6 hours
        puts "added completed worker: #{index}"
      end

      unless (keys = @redis.keys("incident_index*")).size < NUMBER_OF_INCIDENT_DELTAS
        puts "> all incident deltas ran - clearing all workers and setting delta = 0"
        Incident.where(['delta=? AND updated_at<?', 1, minimum_updated_timestamp(keys)]).update_all("delta=0")
        @redis.flushall
      end
    end
  end

  def self.minimum_updated_timestamp(keys)
    keys.inject do |min, x|
      ts = @redis.get(x)
      min = ts if min > ts
      min
    end
  end

  # def self.filter_flag_as_deleted_ids(ids, index)
  #   search_results = []
  #   partition_ids(ids, 4096) do |subset|
  #     search_results += ThinkingSphinx.search_for_ids(
  #       :with => {:@id => subset}, :index => index
  #     ).results[:matches].collect { |match| match[:doc] }
  #   end

  #   search_results
  # end

  # def self.partition_ids(ids, n)
  #   if n > 0 && n < ids.size
  #     result = []
  #     max_subarray_size = n - 1
  #     i = j = 0
  #     while i < ids.size && j < ids.size
  #       j = i + max_subarray_size
  #       result << ids.slice(i..j)
  #       i += n
  #     end
  #   else
  #     result = ids
  #   end

  #   if block_given?
  #     result.each do |ary|
  #       yield ary
  #     end
  #   end

  #   result
  # end
end