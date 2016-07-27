require_relative 'hadoop_base'
require 'concurrent'

class HadoopHDFSBlock < HadoopBase

  def initialize(id)
    @id = id
    @last_edited = Time.now
    @states = ThreadSafe::Hash.new
    @replica = ThreadSafe::Hash.new
    @data = ThreadSafe::Hash.new
    @source_host = ThreadSafe::Hash.new
    @destination_host = ThreadSafe::Hash.new
    @data['namespace'] = @username = @queue = ''
    # @path = ''
  end

  def parse_data(data)
    if data['message'].include?('InvalidateBlocks: add')
      @states[data['timestamp']]=["invalidate block"]
      # @states += [data['timestamp'], 'invalidate', data['hdfs_host']]

    elsif data['message'].include?('allocateBlock')
      @data['namespace'] = data['namespace']
      @path = data['HDFSpath']
      @replica[data['timestamp']]=[data['BlockUCState'], data['primaryNodeIndex'], data['replicas']]
      # @states += [data['timestamp'], data['BlockUCState'], data['primaryNodeIndex'], data['replicas']]

    elsif data['message'].include?('addStoredBlock')
      @replica[data['timestamp']]=[data['BlockUCState'], data['primaryNodeIndex'], data['replicas'], data['size']]
      # @states += [data['timestamp'], data['BlockUCState'], data['primaryNodeIndex'], data['replicas'], data['size']]

    elsif data['message'].include?('addToInvalidates')
      @states[data['timestamp']]=['addToInvalidates']
      # @states += [data['timestamp'], 'addToInvalidates']

    elsif data['message'].include?('BlockManager') && data['message'].include?('to delete')
      @states[data['timestamp']]=["askToDelete"]
      # @states += [data['timestamp'], 'askToDelete', data['hdfs_host']]

    elsif data['message'].include?('FsDatasetAsyncDiskService: Deleted')
      @data['namespace'] = data['namespace']
      @path = data['HDFSpath']
      @states[data['timestamp']]=["Deleted"]
      # @states += [data['timestamp'].to_s, 'Deleted', data['hdfs_host'].to_s]

    elsif data['message'].include?('DataNode: Receiving')
      @data['namespace'] = data['namespace']
      source = data['source_host'].split(':')
      @source_host[data['timestamp']] = source[0]
      destination = data['dest_host'].split(':')
      @destination_host[data['timestamp']] = destination[0]

      @states[data['timestamp']]=['Receiving block']
      # @states += [data['timestamp'], 'Receiving block']
    elsif data['message'].include?('DataNode: Received')
      @data['namespace'] = data['namespace']
      source = data['source_host'].split(':')
      @source_host[data['timestamp']] = source[0]
      destination = data['dest_host'].split(':')
      @destination_host[data['timestamp']] = destination[0]

      @states[data['timestamp']]=['Received block']
      # @states += [data['timestamp'], 'Received block']
      @data['size'] = data['size']


      # elsif data.has_key?('srvID')

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
      @node = get_create_block(@id)
    end
    return @node
  end

  # def filename
  #   file = get_create_file(@path)
  #   rel = node.rels(dir: :outgoing, between: file)
  #   if rel.length == 0
  #     @node.create_rel(:belongs_to, file)
  #   end
  #
  # end


  def to_db

    node
    node.update_props(@data)

    query = " merge (aa#{@id}:block {id: '#{@id}'})  "

    unless @path == '' || @path.nil?
      # Neo4j::Session.current.query("merge (b:file {name: '#{@path}'}) create unique (a#{@id})-[:belongs_to]->(b)")
      query += "merge (path_#{s(@path)}_#{@id}:file {name: '#{@path}'}) create unique (aa#{@id})-[:belongs_to]->(path_#{s(@path)}_#{@id}) "
    end


    @source_host.each { |timestamp, host|
      # source_host = get_create_host(host)
      # rel = node.rels(dir: :outgoing, between: source_host)
      # if rel.length == 0
      #   @node.create_rel(:source_host, source_host)
      # end
      # Neo4j::Session.current.query(" merge (b:host {name: '#{host}'}) create unique (a#{@id})-[:source_host]->(b)")
      query += " merge (bb#{s(host+@id)}:host {name: '#{host}'}) create unique (aa#{@id})-[:source_host {timestamp: '#{timestamp}'}]->(bb#{s(host+@id)}) "
    }
    @destination_host.each { |timestamp, host|
      # dest_host = get_create_host(host)
      # rel = node.rels(dir: :outgoing, between: dest_host)
      # if rel.length == 0
      #   @node.create_rel(:destination_host, dest_host)
      # end
      # Neo4j::Session.current.query(" merge (b:host {name: '#{host}'}) create unique (a#{@id})-[:destination_host]->(b)")
      query += " merge (bb#{s(host+@id)}:host {name: '#{host}'}) create unique (aa#{@id})-[:destination_host {timestamp: '#{timestamp}'}]->(bb#{s(host+@id)}) "
    }
    query

  end

  def to_csv(path)
    if @data.has_key? 'namespace'
    File.open(path + 'blocks.csv', 'a') { |f|
      f.puts @id +','+ @data['namespace']
    }
    end
    unless @path.nil?
      File.open(path + 'block_path.csv', 'a') { |f|
        f.puts @id +','+  @path
      }
    end
    if @data.has_key? 'size'
      File.open(path + 'block_size.csv', 'a') { |f|
        f.puts @id +','+ @data['size']
      }
    end
    unless @states.empty?
      File.open(path + 'block_states.csv', 'a') { |i|
        i.puts states_to_csv
      }
    end
    unless @source_host.empty?
      File.open(path + 'block_source_hosts.csv', 'a') { |g|
        g.puts source_hosts_to_csv
      }
    end
    unless @destination_host.empty?
      File.open(path + 'block_destination_hosts.csv', 'a') { |h|
        h.puts dest_hosts_to_csv
      }
    end
    unless @replica.empty?
      File.open(path + 'block_replica_states.csv', 'a') { |e|
        e.puts replica_states_to_csv
      }
    end
  end

  def source_hosts_to_csv
    string = ''
    unless @source_host.empty?
      @source_host.each { |timestamp, host|
        string += "#{@id},#{host},#{timestamp}\n"
      }
    end
    string
  end

  def dest_hosts_to_csv
    string = ''
    unless @destination_host.empty?
      @destination_host.each { |timestamp, host|
        string += "#{@id},#{host},#{timestamp}\n"
      }
    end
    string
  end

  def replica_states_to_csv
    string = ''
    unless @replica.empty?
      @replica.each { |k, v|
        string += "#{@id},#{k},#{v[0]},#{v[1]},#{v[2]}\n"
      }
    end
    string
  end

  def csv_header
    'id,namespace,path,username,queue'
  end

  def enough_data?
    @data['namespace'] != '' && @path != ''
  end
end