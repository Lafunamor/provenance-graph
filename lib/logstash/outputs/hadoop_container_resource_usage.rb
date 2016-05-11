class HadoopContainerResourceUsage

  # @timestamp
  # @process_tree_id
  # @UsedPhysicalMemory
  # @AvailablePhysicalMemory
  # @UsedVirtualMemory
  # @AvailableVirtualMemory


  def initialize(time, process_tree_id, used_physical_mem, avail_phy_mem, used_virtual_mem, avail_virtual_mem)
    @timestamp = time
    @process_tree_id = process_tree_id
    @used_physical_memory = used_physical_mem
    @available_physical_memory = avail_phy_mem
    @used_virtual_memory = used_virtual_mem
    @available_virtual_memory = avail_virtual_mem
  end


end