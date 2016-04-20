class HadoopContainerResourceUsage

  @timestamp
  @processTreeID
  @UsedPhysicalMemory
  @AvailablePhysicalMemory
  @UsedVirtualMemory
  @AvailableVirtualMemory


  def initialize(time, processTreeID, usedPhysicalMem, availablePhysicalMem, usedVirtualMem, availableVirtualMem)
    @timestamp = time
    @processTreeID = processTreeID
    @UsedPhysicalMemory = usedPhysicalMem
    @AvailablePhysicalMemory = availablePhysicalMem
    @UsedVirtualMemory = usedVirtualMem
    @AvailableVirtualMemory = availableVirtualMem
  end


end