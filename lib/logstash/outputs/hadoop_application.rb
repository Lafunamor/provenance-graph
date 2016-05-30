require_relative 'hadoop_state_change'
require_relative 'hadoop_event'
require 'concurrent'
require_relative 'hadoop_base'

class HadoopApplication < HadoopBase

  # @created
  # @ID
  #
  # @queue
  # @username
  # @unregisteredAt #unregistered at resourcemanager
  # @acceptedAt #accepted at scheduler
  # @stoppingAt
  # @endTime
  # @appState
  # @appName
  # @trackingURL
  # @port
  # @appMasterHost
  # @startTime
  # @finishTime
  # @finalState
  # @masterContainer


  # # HashMap of state changes
  # @AppStates
  # # HashMap of Events
  # @Events
  # # Hash of AppAttempts
  # @app_attempts

  def initialize(id)
    @id = id
    @last_edited = Time.now
    @app_states = []
    @events = []
    @blocks = ThreadSafe::Hash.new
    # @app_attempts = Hash.new
    @app_attempts = []
    @data = ThreadSafe::Hash.new

  end


  def add_attempt(attempt)

    unless @app_attempts.include? attempt
      @app_attempts+= [attempt]

    end
  end

  def parse_data (data)
    if data.has_key? 'AppPreviousState'
      # @app_states[data['timestamp']] = HadoopStateChange.new(data['timestamp'], data['AppPreviousState'], data['AppState'])
      @app_states += [data['timestamp'], data['AppPreviousState'], data['AppState']]
      ######################

    elsif data.has_key? 'Event'
      # @events[data['timestamp']]= HadoopEvent.new data['timestamp'], data['Event']
      @events += [data['timestamp'], data['Event']]
      ##################

    elsif data['message'].include?('Accepted application')
      @data['accepted_at'] = data['timestamp']


      @username = data['UserName']


    elsif data['message'].include?('unregistered successfully.')
      @data['unregistered_at'] = data['timestamp']
    elsif data['message'].include?('Stopping application')
      @data['stopping_at'] = data['timestamp']
    elsif data['message'].include?('Application just finished ')
      @data['end_time'] = data['timestamp']
    elsif data['message'].include?('ApplicationSummary')
      get_summary data
    elsif data.has_key?('CleanLocalDisk')
      @data['removed_at'] = data['timestamp']
      @data['clean_up_local_disks'] = data['CleanLocalDisk']

    else
      return false

    end
    @last_edited = Time.now
    return true
  end

  def get_summary(data)

    if data.has_key? 'State'
      @data['app_state'] = data['State']
    end
    if data.has_key? 'UserName'
      @username= data['UserName']
    end
    if data.has_key? 'Name'
      @data['app_name'] = data['Name']
    end
    if data.has_key? 'queue'
      @queue = data['queue']
    end
    if data.has_key? 'finishTime'
      @data['finish_time'] = data['finishTime']
    end
    if data.has_key? 'TrackingURL'
      @data['tracking_url'] = data['TrackingURL']
    end
    if data.has_key? 'port'
      @data['port'] = data['port']
    end
    if data.has_key? 'AppMasterHost'
      @app_master_host = data['AppMasterHost']

    end
    if data.has_key? 'StartTime'
      @data['start_time'] = data['StartTime']
    end
    if data.has_key? 'FinalStatus'
      @data['final_state'] = data['FinalStatus']
    end

  end

  def add_hdfs_trace(data, block)
    @blocks[data['timestamp']]= [data['namespace'], data['Block_ID'], data['operation']]
    node.create_rel(data['operation'], block.node, timestamp: data['timestamp'])
    return true
  end


  def last_edited
    return @last_edited
  end

  def node
    if @node.nil?
      @node = get_create_application(@id)
    end
    return @node
  end

  def to_db

    node

    @app_attempts.each { |attempt|
      rel = node.rels(dir: :outgoing, between: attempt.node)
      if rel.length == 0
        @node.create_rel(:has, attempt.node)
      end
    }

    node.update_props(@data)
    node[:states] += @app_states
    node[:events] += @events

    unless @username.nil?
      user = get_create_username(@username)
      rel = node.rels(dir: :outgoing, between: user)
      if rel.length == 0
        @node.create_rel(:belongs_to, user)
      end
    end

    unless @queue.nil?
      q = get_create_queue(@queue)
      rel = node.rels(dir: :outgoing, between: q)
      if rel.length == 0
        @node.create_rel(:used_queue, q)
      end
    end

    unless @app_master_host.nil?
      h = get_create_host(@app_master_host)
      rel = node.rels(dir: :outgoing, between: h)
      if rel.length == 0
        @node.create_rel(:app_master, h)
      end
    end

  end

end