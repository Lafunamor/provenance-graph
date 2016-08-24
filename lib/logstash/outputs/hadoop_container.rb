require_relative 'hadoop_container_resource_usage'
require_relative 'hadoop_base'
require 'concurrent'

class HadoopContainer < HadoopBase


  def initialize(id)
    @id = id
    @last_edited = Time.now
    @container_states = ThreadSafe::Hash.new
    @states = ThreadSafe::Hash.new
    @events = ThreadSafe::Hash.new
    @resource_usage = ThreadSafe::Hash.new
    @data = ThreadSafe::Hash.new
    @data[:state_changes] = []
    @data[:resource_usage] = []
    # @data['start_request_time'] = @data['localizer_created_at'] = @data['succeeded_at'] = @data['clean_up_time'] = @data['capacity'] =
    #     @data['added_to_app_at'] = @data['removed_from_app'] = @data['stopped_at'] = @data['started_at']= @data['arguments'] =
    #         @host = @username = @queue = ''
  end

  def parse_data (data)
    if data.has_key? 'PreviousState'
      # @container_states[data['timestamp']] = HadoopStateChange.new(data['timestamp'], data['PreviousState'], data['State'])
      @states[data['timestamp']] = [data['PreviousState'], data['State']]
      ###########
    elsif data.has_key? 'Event'
      # @events[data['timestamp']]= HadoopEvent.new data['timestamp'], data['Event']
      ##########
      @events[data['timestamp']] = data['Event']
    elsif data['message'].include?('Start request')
      @data['start_request_time'] = data['timestamp']
      if @username.nil?
        @username = data['UserName']
      end
    elsif data.has_key? 'StateChange'
      # @states[data['timestamp']] = data['StateChange']
      @container_states[data['timestamp']] = data['StateChange']
      ##########
    elsif data['message'].include?('Created localizer')
      @data['localizer_created_at'] = data['timestamp']
    elsif data.has_key? 'ProcessTreeID'
      # @resource_usage[data['timestamp']] = HadoopContainerResourceUsage.new(data['timestamp'], data['ProcessTreeID'], data['UsedPysicalMemory'], data['AvailablePhysicalMemory'], data['UsedVirtualMemory'], data['AvailableVirtualMemory'])
      ##########
      @resource_usage[data['timestamp']] = [data['ProcessTreeID'], data['UsedPysicalMemory'], data['AvailablePhysicalMemory'], data['UsedVirtualMemory'], data['AvailableVirtualMemory']]
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

  def to_db
q = ["MERGE (container:container {id: '#{@id}'})"]

    if @data.has_key? 'localizer_created_at'
      query = "MERGE (container:container {id: '#{@id}'}) set "
      query += "container.localizer_created_at = '#{@data['localizer_created_at']}';"
      q += [query]
    end
    if @data.has_key? 'succeeded_at'
      query = "MERGE (container:container {id: '#{@id}'}) set "
      query += "container.succeeded_at = '#{@data['succeeded_at']}';"
      q += [query]
    end
    if @data.has_key? 'clean_up_time'
      query = "MERGE (container:container {id: '#{@id}'}) set "
      query += "container.clean_up_time = '#{@data['clean_up_time']}';"
      q += [query]
    end
    if @data.has_key? 'added_to_app_at'
      query = "MERGE (container:container {id: '#{@id}'}) set "
      query += "container.added_to_app_at = '#{@data['added_to_app_at']}';"
      q += [query]
    end
    if @data.has_key? 'removed_from_app'
      query = "MERGE (container:container {id: '#{@id}'}) set "
      query += "container.removed_from_app = '#{@data['removed_from_app']}';"
      q += [query]
    end
    if @data.has_key? 'stopped_at'
      query = "MERGE (container:container {id: '#{@id}'}) set "
      query += "container.stopped_at = '#{ @data['stopped_at']}';"
      q += [query]
    end
    if @data.has_key? 'started_at'
      query = "MERGE (container:container {id: '#{@id}'}) set "
      query += "container.started_at = '#{@data['started_at']}', container.arguments = '#{@data['arguments']}';"
      q += [query]
    end
    if (@data.has_key?('capacity') && @host != nil)
      query = "MERGE (container:container {id: '#{@id}'}) set "
      query += "container.capacity = #{@data['capacity']};"
      q += [query]
    end

    unless @host == '' || @path.nil?
      # h = get_create_host(@host)
      # rel = node.rels(dir: :outgoing, between: h)
      # if rel.length == 0
      #   @node.create_rel(:hosted_on, h)
      # end
      # Neo4j::Session.current.query("merge (a:container {id: ''#{@id}''}) merge (b:host {name: '#{@host}'}) create unique (a)-[:hosted_on]->(b)")
      query = " merge (e:container {id: ''#{@id}''}) "
      query += "merge (f:host {name: '#{@host}'}) merge (e)-[:hosted_on]->(f); "
      q += [query]
    end

    unless @username == '' || @username.nil?
      # user = get_create_username(@username)
      # rel = node.rels(dir: :outgoing, between: user)
      # if rel.length == 0
      #   @node.create_rel(:belongs_to, user)
      # end
      # Neo4j::Session.current.query("merge (a:container {id: ''#{@id}''}) merge (b#{@username}:user {name: '#{@username}'}) create unique (a'#{@id}')-[:belongs_to]->(b#{@username})")
      query = merge_query
      query += " merge (f:user {name: '#{@username}'}) merge (e)-[:belongs_to]->(f); "
      q += [query]
    end

    unless @states.empty?
      q += states_to_query
    end

    unless @events.empty?
      q += events_to_query
    end

    @container_states.each {|timestamp, state|
      query = "MERGE (a:container {id: '#{@id}'})"
      query += " merge (state:state {name: '#{state}'}) merge (a)-[:transitioned_to {timestamp: '#{timestamp}'}]->(state);"
      q += [query]
    }

    @resource_usage.each {|timestamp, usage|
    query = "MERGE (a:container {id: '#{@id}'})
    CREATE (res:resource_usage {ProcessTreeID: '#{usage[1]}',
        UsedPysicalMemory: '#{usage[2]}',
        AvailablePhysicalMemory: '#{usage[3]}',
        UsedVirtualMemory: '#{usage[4]}',
        AvailableVirtualMemory: '#{usage[5]}'
    })

    CREATE (a)-[:used {timestamp: '#{timestamp}'}]->(res);"
    q += [query]
    }

    q
  end

  def merge_query
    " merge (a:container {id: '#{@id}'}) "
  end

  def to_csv(path)
    unless @username.nil?
      File.open(path + 'container_user.csv', 'a') { |f|
        f.puts @id +','+ @username +','+ @data['start_request_time']
      }
    end
    if @data.has_key? 'localizer_created_at'
      File.open(path + 'container_localizer_created.csv', 'a') { |f|
        f.puts @id +','+ @data['localizer_created_at']
      }
    end
    if @data.has_key? 'succeeded_at'
      File.open(path + 'container_succeeded_at.csv', 'a') { |f|
        f.puts @id +','+ @data['succeeded_at']
      }
    end
    if @data.has_key? 'clean_up_time'
      File.open(path + 'container_clean_up_time.csv', 'a') { |f|
        f.puts @id +','+ @data['clean_up_time']
      }
    end
    if @data.has_key? 'added_to_app_at'
      File.open(path + 'container_added_to_app_at.csv', 'a') { |f|
        f.puts @id +','+ @data['added_to_app_at']
      }
    end
    if @data.has_key? 'removed_from_app'
      File.open(path + 'container_removed_from_app.csv', 'a') { |f|
        f.puts @id +','+ @data['removed_from_app']
      }
    end
    if @data.has_key? 'stopped_at'
      File.open(path + 'container_stopped_at.csv', 'a') { |f|
        f.puts @id +','+ @data['stopped_at']
      }
    end
    if @data.has_key? 'started_at'
      File.open(path + 'container_started_at.csv', 'a') { |f|
        f.puts @id +','+ @data['started_at']+','+ @data['arguments']
      }
    end
    if (@data.has_key?('capacity') && @host != nil)
      File.open(path + 'container_host.csv', 'a') { |f|
        f.puts @id +',"'+ @data['capacity'] +'",'+@host
      }
    end

    unless @states.empty?
      File.open(path + 'container_states.csv', 'a') { |i|
        i.puts states_to_csv
      }
    end
    unless @events.empty?
      File.open(path + 'container_events.csv', 'a') { |j|
        j.puts events_to_csv
      }
    end
    unless @resource_usage.empty?
      File.open(path + 'container_resource_usage.csv', 'a') { |h|
        h.puts resource_usage_to_csv
      }
    end
    unless @container_states.empty?
      File.open(path + 'container_state_transitions.csv', 'a') { |g|
        g.puts container_states_to_csv
      }
    end

  end


  def container_states_to_csv
    string = ''
    unless @container_states.empty?
      @container_states.each { |k, v|
        string += "#{@id},#{k},#{v}\n"
      }
    end
    string
  end

  def resource_usage_to_csv
    string = ''
    unless @resource_usage.empty?
      @resource_usage.each { |k, v|
        string += "#{@id},#{k},#{v[0]},#{v[1]},#{v[2]},#{v[3]},#{v[4]}\n"
      }
    end
    string
  end

end