require_relative 'hadoopstatechange'
require_relative 'hadoopevent'

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
    @created = Time.now
    @containers = Hash.new
    @app_states = Hash.new
    @events = Hash.new
  end

  def add_container(container_id, container)
    @containers[container_id] = container
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


end