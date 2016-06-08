require_relative 'hadoop_container_resource_usage'
require_relative 'hadoop_base'
require 'concurrent'

class HadoopContainer < HadoopBase


  def initialize(id)
    @id = id
    @last_edited = Time.now
    @container_states = [] #ThreadSafe::Hash.new
    @states = [] #ThreadSafe::Hash.new
    @events = [] #ThreadSafe::Hash.new
    @resource_usage = [] #ThreadSafe::Hash.new
    @data = ThreadSafe::Hash.new
    @data[:states] = []
    @data[:events] = []
    @data[:state_changes] = []
    @data[:resource_usage] = []

  end

  def parse_data (data)
    if data.has_key? 'PreviousState'
      # @container_states[data['timestamp']] = HadoopStateChange.new(data['timestamp'], data['PreviousState'], data['State'])
      @container_states += [data['timestamp'], data['PreviousState'], data['State']]
      ###########
    elsif data.has_key? 'Event'
      # @events[data['timestamp']]= HadoopEvent.new data['timestamp'], data['Event']
      ##########
      @events += [data['timestamp'], data['Event']]
    elsif data['message'].include?('Start request')
      @data['start_request_time'] = data['timestamp']
      if @username.nil?
        @username = data['UserName']
      end
    elsif data.has_key? 'StateChange'
      # @states[data['timestamp']] = data['StateChange']
      @states += [data['timestamp'], data['StateChange']]
      ##########
    elsif data['message'].include?('Created localizer')
      @data['localizer_created_at'] = data['timestamp']
    elsif data.has_key? 'ProcessTreeID'
      # @resource_usage[data['timestamp']] = HadoopContainerResourceUsage.new(data['timestamp'], data['ProcessTreeID'], data['UsedPysicalMemory'], data['AvailablePhysicalMemory'], data['UsedVirtualMemory'], data['AvailableVirtualMemory'])
      ##########
      @resource_usage += [data['timestamp'], data['ProcessTreeID'], data['UsedPysicalMemory'], data['AvailablePhysicalMemory'], data['UsedVirtualMemory'], data['AvailableVirtualMemory']]
      ##########
    elsif data['message'].include?('succeeded')
      @data['succeeded_at'] = data['timestamp']
    elsif data['message'].include?('Cleaning up container')
      @data['clean_up_time'] = data['timestamp']
    elsif data.has_key? 'ReleaseResource'
      @host = data['Host'].split(':')[0]
      @data['capacity'] = data['capacity']
      # elsif data["message"].include?("Start request")
      #   getSummary data
    elsif data['message'].include?('Adding container')
      @data['added_to_app_at'] = data['timestamp']
    elsif data['message'].include?('Removing container')
      @data['removed_from_app'] = data['timestamp']
    elsif data['message'].include?('Stopping container with container Id')
      @data['stopped_at'] = data['timestamp']
    elsif data.has_key? 'Arguments'
      @data['started_at'] = data['timestamp']
      @data['arguments'] = data['Arguments']
    else
      return false

    end
    @last_edited = Time.now
    return true
  end

  def last_edited
    return @last_edited
  end

  def node
    if @node.nil?
      @node = get_create_container(@id)
    end
    return @node
  end

  def to_db

    node

    node.update_props(@data)

    node[:states] +=@container_states
    node[:events] += @events
    node[:state_changes] += @states
    node[:resource_usage] += @resource_usage

    query = " merge (e#{@id}:container {id: '#{@id}'}) "
    unless @host.nil?
      # h = get_create_host(@host)
      # rel = node.rels(dir: :outgoing, between: h)
      # if rel.length == 0
      #   @node.create_rel(:hosted_on, h)
      # end
      # Neo4j::Session.current.query("merge (a:container {id: '#{@id}'}) merge (b:host {name: '#{@host}'}) create unique (a)-[:hosted_on]->(b)")
      query += "merge (f#{s(@host+@id)}:host {name: '#{@host}'}) create unique (e#{@id})-[:hosted_on]->(f#{s(@host+@id)}) "
    end

    unless @username.nil?
      # user = get_create_username(@username)
      # rel = node.rels(dir: :outgoing, between: user)
      # if rel.length == 0
      #   @node.create_rel(:belongs_to, user)
      # end
      # Neo4j::Session.current.query("merge (a:container {id: '#{@id}'}) merge (b#{@username}:user {name: '#{@username}'}) create unique (a#{@id})-[:belongs_to]->(b#{@username})")
      query += " merge (f#{@username+@id}:user {name: '#{@username}'}) create unique (e#{@id})-[:belongs_to]->(f#{@username+@id}) "
    end
    query
  end

  def to_csv

    @id +','+ @data['start_request_time'] +','+ @data['localizer_created_at'] +','+ @data['succeeded_at'] +','+ @data['clean_up_time'] +
        ','+ @data['capacity'] +','+ @data['added_to_app_at'] +','+ @data['removed_from_app'] +','+ @data['stopped_at'] +
        ','+ @data['started_at'] +','+ @data['arguments'] +
        ','+@host +','+ @username +','+ @queue
  end


  def csv_header
    'id,start_request_time,localizer_created_at,succeeded_at,clean_up_time,capacity,added_to_app_at,removed_from_app,stopped_at,started_at,arguments,host,username,queue'
  end

end