require_relative 'hadoop_state_change'
require_relative 'hadoop_event'
require 'concurrent'

class HadoopApplication

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
    @app_states = ThreadSafe::Hash.new
    @events = ThreadSafe::Hash.new
    @blocks = ThreadSafe::Hash.new
    # @app_attempts = Hash.new
    @app_attempts = []
  end


  # def add_attempt(attempt_id, attempt)
  #   @app_attempts[attempt_id]= attempt
  # end
  def add_attempt(attempt_id)
    unless @app_attempts.include? attempt_id
      @app_attempts+= [attempt_id]
    end
  end

  def parse_data (data)
    if data.has_key? 'AppPreviousState'
      @app_states[data['timestamp']] = HadoopStateChange.new(data['timestamp'], data['AppPreviousState'], data['AppState'])
    elsif data.has_key? 'Event'
      @events[data['timestamp']]= HadoopEvent.new data['timestamp'], data['Event']
    elsif data['message'].include?('Accepted application')
      @accepted_at = data['timestamp']
      @username = data['UserName']
    elsif data['message'].include?('unregistered successfully.')
      @unregistered_at = data['timestamp']
    elsif data['message'].include?('Stopping application')
      @stopping_at = data['timestamp']
    elsif data['message'].include?('Application just finished ')
      @end_time = data['timestamp']
    elsif data['message'].include?('ApplicationSummary')
      get_summary data
    elsif data.has_key?('CleanLocalDisk')
      @removed_at = data['timestamp']
      @clenup_local_disks = data['CleanLocalDisk']

    else
      return false

    end
    @last_edited = Time.now
    return true
  end

  def get_summary(data)

    if data.has_key? 'State'
      @app_state = data['State']
    end
    if data.has_key? 'UserName'
      @username= data['UserName']
    end
    if data.has_key? 'Name'
      @app_name = data['Name']
    end
    if data.has_key? 'queue'
      @queue = data['queue']
    end
    if data.has_key? 'finishTime'
      @finish_time = data['finishTime']
    end
    if data.has_key? 'TrackingURL'
      @tracking_url = data['TrackingURL']
    end
    if data.has_key? 'port'
      @port = data['port']
    end
    if data.has_key? 'AppMasterHost'
      @app_master_host = data['AppMasterHost']
    end
    if data.has_key? 'StartTime'
      @start_time = data['StartTime']
    end
    if data.has_key? 'FinalStatus'
      @final_state = data['FinalStatus']
    end

  end

  def add_hdfs_trace(data)
    @blocks[data['timestamp']]= [data['namespace'], data['Block_ID'], data['operation']]
    return true
  end

  def last_edited
    return @last_edited
  end

end