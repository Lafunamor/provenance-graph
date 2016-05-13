class HadoopHDFSBlock

  def initialize(id)
    @id = id
    @created = Time.now
    @states = Hash.new
  end

  def parse_data(data)
    if data['message'].include?('InvalidateBlocks: add')
      @states[data['timestamp']]=['invalidate', data['hdfs_host']]

    elsif data['message'].include?('allocateBlock')
      @namespace = data['namespace']
      @path = data['HDFSpath']
      @states[data['timestamp']]=[data['BlockUCState'], data['primaryNodeIndex'], data['replicas']]
    elsif data['message'].include?('addStoredBlock')
      @states[data['timestamp']]=[data['BlockUCState'], data['primaryNodeIndex'], data['replicas'], data['size']]

    elsif data['message'].include?('addToInvalidates')
      @states[data['timestamp']]=['addToInvalidates']
    elsif data['message'].include?('BlockManager') && data['message'].include?('to delete')
      @states[data['timestamp']]=['askToDelete', data['hdfs_host']]
    elsif data['message'].include?('FsDatasetAsyncDiskService: Deleted')
      @namespace = data['namespace']
      @path = data['HDFSpath']
      @states[data['timestamp']]=['Deleted', data['hdfs_host']]
    elsif data['message'].include?('DataNode: Receiving')
      @namespace = data['namespace']
      @source_host = data['source_host']
      @destination_host = data['dest_host']
      @states[data['timestamp']]=['Receiving block']
    elsif data['message'].include?('DataNode: Received')
      @namespace = data['namespace']
      @source_host = data['source_host']
      @destination_host = data['dest_host']
      @states[data['timestamp']]=['Received block']
      @size = data['size']
    else
      return false
    end
    return true
  end

    # def to_s
    #   'Block: '+@id+', namepscae='+@namespace+', path='+@path+', '+@states
    # end


end