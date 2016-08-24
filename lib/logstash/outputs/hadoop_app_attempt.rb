require_relative 'hadoop_state_change'
require_relative 'hadoop_event'
require 'concurrent'
require_relative 'hadoop_base'

class HadoopAppAttempt < HadoopBase

  def initialize(id)
    @id = id
    @last_edited = Time.now
    # @containers = Hash.new
    @states = ThreadSafe::Hash.new
    # @events = ThreadSafe::Hash.new
    @containers = []
    @data = ThreadSafe::Hash.new
    # @data['final_state'] = @data['end_time'] = @data['host_http_adr'] = @data['resource'] = @data['priority'] = @data['token'] =
    #     @host = @username = @master_container = ''
  end

  def add_container(container_id)
    unless @containers.include? container_id
      @containers+=[container_id]
    end
  end

  def parse_data (data)
    if data.has_key? 'PreviousState'
      # @app_states[data['timestamp']] = HadoopStateChange.new(data['timestamp'], data['PreviousState'], data['State'])
      @states[data['timestamp']] = [data['PreviousState'], data['State']]
    # elsif data.has_key? 'Event'
    #   # @events[data['timestamp']]= HadoopEvent.new data['timestamp'], data['Event']
    #   @events[data['timestamp']] = data['Event']
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

  def to_db
    q = [" MERGE (attempt:attempt {id: '#{@id}'}) "]


    if (@master_container !=nil )
      query = " MERGE (attempt:attempt {id: '#{@id}'}) "
      query += "SET
		attempt.host_http_adr = '#{@data['host_http_adr']}',
		attempt.resource = '#{@data['resource']}',
		attempt.priority = '#{@data['priority']}',
		attempt.token = '#{@data['token']}';"
      q += [query]
    end
    if (@data.has_key?('final_state'))
      query = " MERGE (attempt:attempt {id: '#{@id}'}) "
      query += "SET attempt.final_state = '#{@data['final_state']}',
		attempt.end_time = TOINT('#{@data['end_time']}');"
      q += [query]
    end



    @containers.each { |container_id|
      # rel = node.rels(dir: :outgoing, between: container.node)
      # if rel.length == 0
      #   @node.create_rel(:has, container.node)
      # end
      # Neo4j::Session.current.query("merge (a:attempt {id: '#{@id}'}) merge (b:container {id: '#{container_id}'}) create unique (a)-[:has]->(b)")
      query = " merge (c:attempt {id: '#{@id}'}) "
      query += "merge (d:container {id: '#{container_id}'}) merge (c)-[:has]->(d); "
      q += [query]
    }
    unless @states.empty?
      q += states_to_query
    end


    unless @master_container == '' || @master_container.nil?
      query = " merge (c:attempt {id: '#{@id}'}) "
      query += "merge (d:container {name: '#{@master_container}'}) merge (c)-[:master_container]->(d); "
      q += [query]
    end

    unless @host == '' || @host.nil?
      # h = get_create_host(@host)
      # rel = node.rels(dir: :outgoing, between: h)
      # if rel.length == 0
      #   @node.create_rel(:hosted_on, h)
      # end
      # Neo4j::Session.current.query("merge (a:attempt {id: '#{@id}'}) merge (b:host {name: '#{@host}'}) create unique (a)-[:hosted_on]->(b)")
      query = " merge (c:attempt {id: '#{@id}'}) "
      query += "merge (d:host {name: '#{@host}'}) merge (c)-[:hosted_on]->(d)}); "
      q += [query]
    end
   q

  end

  def to_csv(path)
    if (@master_container !=nil )
    File.open(path + 'app_attempt_summary.csv', 'a') { |f|
      f.puts "#{@id},#{@data['host_http_adr']},#{@data['resource']},#{@data['priority']},#{@data['token']},#{@host},#{@master_container}"
    }
    end
    if (@data.has_key?('final_state'))
      File.open(path + 'app_attempt_summary2.csv', 'a') { |f|
        f.puts "#{@id},#{@data['final_state']},#{@data['end_time']}"
      }
    end
    unless @containers.empty?
      File.open(path + 'attempts_containers.csv', 'a') { |g|
        g.puts to_csv2
      }
    end
    unless @states.empty?
      File.open(path + 'app_attempt_states.csv', 'a') { |i|
        i.puts states_to_csv
      }
    end
    # unless @events.empty?
    #   File.open(path + 'app_attempt_events.csv', 'a') { |j|
    #     j.puts events_to_csv
    #   }
    # end
  end

  def to_csv2
    string = ''
    @containers.each { |container_id|
      string +=  @id +','+ container_id + "\n"
    }
    string

  end

  def csv_header
    'id,final_state,end_time,host_http_adr,resource,priority,token,host,master_container'
  end

  def get_containers
    @containers
  end

  def match_query
    " merge (a:attempt {id: '#{@id}'}) "
  end

end