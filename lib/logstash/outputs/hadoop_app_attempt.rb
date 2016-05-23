require_relative 'hadoop_state_change'
require_relative 'hadoop_event'
require 'concurrent'

class HadoopAppAttempt

  # @created
  # @ID
  #
  # @queue
  # @username
  # @unregisteredAt #unregistered at resourcemanager
  # @acceptedAt #accepted at scheduler
  # @stoppingAt
  # @endTime
  # @finalState
  #
  # # Hash of containers of this application
  # @Containers
  # # HashMap of state changes
  # @AppStates
  # # HashMap of Events
  # @Events

  def initialize(id)
    @id = id
    @last_edited = Time.now
    # @containers = Hash.new
    @app_states = ThreadSafe::Hash.new
    @events = ThreadSafe::Hash.new
    @containers = []
  end

  # def add_container(container_id, container)
  #   @containers[container_id] = container
  # end
  def add_container(container_id)
    unless @containers.include? container_id
      @containers+=[container_id]
    end
  end

  def parse_data (data)
    if data.has_key? 'PreviousState'
      @app_states[data['timestamp']] = HadoopStateChange.new(data['timestamp'], data['PreviousState'], data['State'])
    elsif data.has_key? 'Event'
      @events[data['timestamp']]= HadoopEvent.new data['timestamp'], data['Event']

    elsif data['message'].include?('is done.')
      get_summary data
    elsif data['message'].include?('Storing attempt')
      get_summary data

    else
      return false

    end
    @last_edited = Time.now
    return true
  end

  def get_summary(data)
    if data.has_key? 'FinalState'
      @final_state = data['FinalState']
      @end_time = data['timestamp']
    end
    if data.has_key? 'MasterContainerID'
      @master_container = data['MasterContainerID']
    end
    if data.has_key? 'Node'
      @node = data['Node']
    end
    if data.has_key? 'NodeHTTPAddress'
      @node_http_adr = data['NodeHTTPAddress']
    end
    if data.has_key? 'resource'
      @resource = data['resource']
    end
    if data.has_key? 'Priority'
      @priority = data['Priority']
    end
    if data.has_key? 'Token'
      @token = data['Token']
    end
  end

  def last_edited
    return @last_edited
  end


end