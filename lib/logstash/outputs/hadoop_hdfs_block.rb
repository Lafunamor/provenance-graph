require_relative 'hadoop_base'
require 'concurrent'

class HadoopHDFSBlock < HadoopBase

  def initialize(id)
    @id = id
    @last_edited = Time.now
    @states = []
    @data = ThreadSafe::Hash.new
    @source_host = []
    @destination_host = []
    @data[:states] = []
  end

  def parse_data(data)
    if data['message'].include?('InvalidateBlocks: add')
      # @states[data['timestamp']]=['invalidate', data['hdfs_host']]
      @states += [data['timestamp'], 'invalidate', data['hdfs_host']]

    elsif data['message'].include?('allocateBlock')
      @data['namespace'] = data['namespace']
      @path = data['HDFSpath']
      # @states[data['timestamp']]=[data['BlockUCState'], data['primaryNodeIndex'], data['replicas']]
      @states += [data['timestamp'], data['BlockUCState'], data['primaryNodeIndex'], data['replicas']]

    elsif data['message'].include?('addStoredBlock')
      # @states[data['timestamp']]=[data['BlockUCState'], data['primaryNodeIndex'], data['replicas'], data['size']]
      @states += [data['timestamp'], data['BlockUCState'], data['primaryNodeIndex'], data['replicas'], data['size']]

    elsif data['message'].include?('addToInvalidates')
      # @states[data['timestamp']]=['addToInvalidates']
      @states += [data['timestamp'], 'addToInvalidates']

    elsif data['message'].include?('BlockManager') && data['message'].include?('to delete')
      # @states[data['timestamp']]=['askToDelete', data['hdfs_host']]
      @states += [data['timestamp'], 'askToDelete', data['hdfs_host']]

    elsif data['message'].include?('FsDatasetAsyncDiskService: Deleted')
      @data['namespace'] = data['namespace']
      @path = data['HDFSpath']
      # @states[data['timestamp']]=['Deleted', data['hdfs_host']]
      @states += [data['timestamp'].to_s, 'Deleted', data['hdfs_host'].to_s]

    elsif data['message'].include?('DataNode: Receiving')
      @data['namespace'] = data['namespace']
      @source_host += [data['source_host']]
      @destination_host += [data['dest_host']]

      # @states[data['timestamp']]=['Receiving block']
      @states += [data['timestamp'], 'Receiving block']
    elsif data['message'].include?('DataNode: Received')
      @data['namespace'] = data['namespace']
      @source_host += [data['source_host']]
      @destination_host += [data['dest_host']]

      # @states[data['timestamp']]=['Received block']
      @states += [data['timestamp'], 'Received block']
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
    node[:states] +=@states

    query = " merge (aa#{@id}:block {id: '#{@id}'})  "

    unless @path.nil?
      # Neo4j::Session.current.query("merge (b:file {name: '#{@path}'}) create unique (a#{@id})-[:belongs_to]->(b)")
      query += "merge (bb#{s(@path)}:file {name: '#{@path}'}) create unique (aa#{@id})-[:belongs_to]->(bb#{s(@path)}) "
    end


    @source_host.each { |host|
      # source_host = get_create_host(host)
      # rel = node.rels(dir: :outgoing, between: source_host)
      # if rel.length == 0
      #   @node.create_rel(:source_host, source_host)
      # end
      # Neo4j::Session.current.query(" merge (b:host {name: '#{host}'}) create unique (a#{@id})-[:source_host]->(b)")
      query += " merge (bb#{s(host+@id)}:host {name: '#{host}'}) create unique (aa#{@id})-[:source_host]->(bb#{s(host+@id)}) "
    }
    @destination_host.each { |host|
      # dest_host = get_create_host(host)
      # rel = node.rels(dir: :outgoing, between: dest_host)
      # if rel.length == 0
      #   @node.create_rel(:destination_host, dest_host)
      # end
      # Neo4j::Session.current.query(" merge (b:host {name: '#{host}'}) create unique (a#{@id})-[:destination_host]->(b)")
      query += " merge (bb#{s(host+@id)}:host {name: '#{host}'}) create unique (aa#{@id})-[:destination_host]->(bb#{s(host+@id)}) "
    }
    query

  end

  def to_csv

    @id +','+ @data['namespace'] +','+ @source_host +','+ @destination_host +
        ','+ @path +','+ @username +','+ @queue
  end


  def csv_header
    'id,namespace,source_host,destination_host,path,username,queue'
  end

end