require_relative 'hadoop_state_change'
require_relative 'hadoop_event'
require 'concurrent'
require_relative 'hadoop_base'

class HadoopAppAttempt < HadoopBase

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
    @app_states = []
    @events = []
    @containers = []
    @data = ThreadSafe::Hash.new
  end

  def add_container(container)
    unless @containers.include? container
      @containers+=[container]
    end
  end

  def parse_data (data)
    if data.has_key? 'PreviousState'
      # @app_states[data['timestamp']] = HadoopStateChange.new(data['timestamp'], data['PreviousState'], data['State'])
      @app_states += [data['timestamp'], data['PreviousState'], data['State']]
    elsif data.has_key? 'Event'
      # @events[data['timestamp']]= HadoopEvent.new data['timestamp'], data['Event']
      @events += [data['timestamp'], data['Event']]
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
      @data['final_state'] = data['FinalState']
      @data['end_time'] = data['timestamp']
    end
    if data.has_key? 'MasterContainerID'
      ids = data['MasterContainerID'].split('_')
      @master_container = ids[1] +'_'+ ids[2] +'_'+ ids[3] +'_'+ ids[4]

    end
    if data.has_key? 'Node'
      @host = data['Node'].split(':')[0]

    end
    if data.has_key? 'NodeHTTPAddress'
      @data['host_http_adr'] = data['NodeHTTPAddress']
    end
    if data.has_key? 'resource'
      @data['resource'] = data['resource']
    end
    if data.has_key? 'Priority'
      @data['priority'] = data['Priority']
    end
    if data.has_key? 'Token'
      @data['token'] = data['Token']
    end
  end

  def last_edited
    return @last_edited
  end

  def node
    if @node.nil?
      @node = get_create_attempt(@id)
    end
    return @node
  end

  def to_db

    node

    @containers.each { |container|
      rel = node.rels(dir: :outgoing, between: container.node)
      if rel.length == 0
        @node.create_rel(:has, container.node)
      end
    }

    node.update_props(@data)
    node[:states] +=@app_states
    node[:events] += @events

    unless @master_container.nil?
      master_host = get_create_container(@master_container)
      rel = node.rels(dir: :outgoing, between: master_host)
      if rel.length == 0
        @node.create_rel(:master_container, master_host)
      end
    end

    unless @host.nil?
      h = get_create_host(@host)
      rel = node.rels(dir: :outgoing, between: h)
      if rel.length == 0
        @node.create_rel(:hosted_on, h)
      end
    end


  end

end