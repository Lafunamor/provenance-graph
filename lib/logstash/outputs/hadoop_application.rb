require_relative 'hadoop_state_change'
require_relative 'hadoop_event'
require 'concurrent'
require_relative 'hadoop_base'

class HadoopApplication < HadoopBase

  def initialize(id)
    @id = id
    @last_edited = Time.now
    @states = ThreadSafe::Hash.new
    @events = ThreadSafe::Hash.new
    @blocks = ThreadSafe::Hash.new
    @block_ids = []
    # @app_attempts = Hash.new
    @app_attempts = []
    @data = ThreadSafe::Hash.new
    # @data['accepted_at'] = @data['unregistered_at'] = @data['stopping_at'] = @data['end_time'] = @data['removed_at'] =
    # @data['clean_up_local_disks'] = @data['app_state'] = @data['app_name'] = @data['finish_time'] =
    # @data['tracking_url'] = @data['port'] = @data['start_time'] = @data['final_state'] = @username =
    # @queue = @app_master_host = ''
  end


  def add_attempt(attempt_id)

    unless @app_attempts.include? attempt_id
      @app_attempts+= [attempt_id]

    end
  end

  def parse_data (data)
    if data.has_key? 'AppPreviousState'
      # @app_states[data['timestamp']] = HadoopStateChange.new(data['timestamp'], data['AppPreviousState'], data['AppState'])
      @states[data['timestamp']] = [data['AppPreviousState'], data['AppState']]
      ######################

    elsif data.has_key? 'Event'
      # @events[data['timestamp']]= HadoopEvent.new data['timestamp'], data['Event']
      @events[data['timestamp']] = data['Event']
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

    # if data.has_key? 'State'
    @data['app_state'] = data['State']
    # end
    # if data.has_key? 'UserName'
    @username= data['UserName']
    # end
    # if data.has_key? 'Name'
    @data['app_name'] = data['Name']
    # end
    # if data.has_key? 'queue'
    @queue = data['queue']
    # end
    # if data.has_key? 'finishTime'
    @data['finish_time'] = data['FinishTime']
    # end
    # if data.has_key? 'TrackingURL'
    @data['tracking_url'] = data['TrackingURL']
    # end
    # if data.has_key? 'port'
    #   @data['port'] = data['port']
    # end
    # if data.has_key? 'AppMasterHost'
    @app_master_host = data['AppMasterHost']

    # end
    # if data.has_key? 'StartTime'
    @data['start_time'] = data['StartTime']
    # end
    # if data.has_key? 'FinalStatus'
    @data['final_state'] = data['FinalStatus']
    # end

  end

  def add_hdfs_trace(data, import_mode)
    @blocks[data['timestamp']]= [data['namespace'], data['Block_ID'], data['operation']]
    # unless @block_ids.include? data['Block_ID']
    #   @block_ids+= [data['Block_ID']]
    # end
    return true
  end


  def last_edited
    return @last_edited
  end


  def to_db

    q = [" merge (app:application {id: '#{@id}'}) "]


    if (@data.has_key?('app_state') && @data.has_key?('app_name') && @data.has_key?('finish_time'))
      query = " merge (app:application {id: '#{@id}'}) "
      query += " SET
		    app.app_state = '#{@data['app_state']}',
		    app.app_name = '#{@data['app_name']}',
		    app.finish_time = TOINT(#{@data['finish_time']}),
		    app.tracking_url = '#{@data['tracking_url']}',
		    app.start_time = TOINT(#{@data['start_time']}),
		    app.final_state = '#{@data['final_state']}';"
      q += [query]
    end


    @app_attempts.each { |attempt_id|
      # rel = node.rels(dir: :outgoing, between: attempt.node)
      # if rel.length == 0
      #   @node.create_rel(:has, attempt.node)
      # end
      # Neo4j::Session.current.query("merge (a:application {id: '#{@id}'}) merge (b:attempt {id: '#{attempt_id}'}) create unique (a)-[:has]->(b)")
      query = " merge (a:application {id: '#{@id}'}) "
      query += "merge (b:attempt {id: '#{attempt_id}'}) merge (a)-[:has]->(b); "
      q += [query]
    }

    unless @states.empty?
      q += states_to_query
    end

    unless @events.empty?
      q += events_to_query
    end

    unless @blocks.empty?
      @blocks.each{ |timestamp, block|
        query = " MERGE (a:application {id: '#{@id}'}) MERGE (block:block {id: '#{block[1]}'}) MERGE  (a)-[:#{block[2]} {timestamp: '#{timestamp}'}]->(block); "
      }
      q += [query]
    end

    unless @username == '' || @username.nil?
      # Neo4j::Session.current.query("merge (a:application {id: '#{@id}'}) merge (b:user {name: '#{@username}'}) create unique (a)-[:belongs_to]->(b)")
      query = " merge (a:application {id: '#{@id}'}) "
      query += "merge (b:user {name: '#{@username}'}) merge (a)-[:belongs_to]->(b); "
      q += [query]
    end

    unless @queue == '' || @queue.nil?
      query = " merge (a:application {id: '#{@id}'}) "
      query += "merge (b:queue {name: '#{@queue}'}) merge (a)-[:used_queue]->(b); "
      q += [query]
    end

    unless @app_master_host == '' || @app_master_host.nil?
      # h = get_create_host(@app_master_host)
      # rel = node.rels(dir: :outgoing, between: h)
      # if rel.length == 0
      #   @node.create_rel(:app_master, h)
      # end
      # Neo4j::Session.current.query("merge (a:application {id: '#{@id}'}) merge (b:host {name: '#{@app_master_host}'}) (a)-[:app_master]->(b)")
      query = " merge (a:application {id: '#{@id}'}) "
      query += "merge (b:host {name: '#{@app_master_host}'}) merge (a)-[:app_master]->(b); "
      q += [query]
    end
    q

  end


  def to_csv(path)
    if (@data.has_key?('app_state') && @data.has_key?('app_name') && @data.has_key?('finish_time'))
      File.open(path + 'app_summary.csv', 'a') { |f|
        f.puts "#{@id},#{@data['app_state']},#{@data['app_name']},#{@data['finish_time']},#{@data['tracking_url']},#{@data['start_time']},#{@data['final_state']},#{@username},#{@queue},#{@app_master_host}"
      }
    end
    # @id +','+ @data['accepted_at'] +','+ @data['unregistered_at'] +','+ @data['stopping_at'] +','+ @data['end_time'] +
    #     ','+ @data['removed_at'] +','+ @data['clean_up_local_disks']

    unless @states.empty?
      File.open(path + 'apps_states.csv', 'a') { |i|
        i.puts states_to_csv
      }
    end
    unless @events.empty?
      File.open(path + 'apps_events.csv', 'a') { |j|
        j.puts events_to_csv
      }
    end
    unless @app_attempts.empty?
      File.open(path + 'apps_attempts.csv', 'a') { |g|
        g.puts to_csv2
      }
    end
    unless @blocks.empty?
      File.open(path + 'apps_blocks.csv', 'a') { |h|
        h.puts to_csv3
      }
    end
  end

  def to_csv2
    string = ''
    @app_attempts.each { |attempt_id|
      string += "#{@id},#{attempt_id}\n"
    }
    string
  end

  def to_csv3
    string = ''
    @blocks.each { |k, v|
      string += "#{@id},#{k},#{v[1]},#{v[2]}\n"
    }
    string
  end

  # def states_to_csv
  #   string = ''
  #   @states.each{|k,v|
  #     string += "#{@id},#{k},#{v[0]},#{v[1]}\n"
  #   }
  # end
  #
  # def events_to_csv
  #   string = ''
  #   @events.each{|k,v|
  #     string += "#{@id},#{k},#{v}\n"
  #   }
  # end

  def csv_header
    'id,accepted_at,unregistered_at,stopping_at,end_time,removed_at,clean_up_local_disks,app_state,app_name,finish_time,tracking_url,port,start_time,final_state,username,queue,app_master_host'
  end

  def get_attempts
    @app_attempts
  end

  def match_query
    " merge (a:application {id: '#{@id}'}) "
  end

end