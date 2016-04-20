class HadoopJob

  @created
  @ID
  @submitTime
  @launchTime
  @firstMapTaskLaunchTime
  @firstReduceTaskLaunchTime
  @finishTime
  @resourcesPerMap
  @resourcesPerReduce
  @numMaps
  @numReduces
  @username
  @queue
  @jobStatus
  @mapSlotSeconds
  @reduceSlotSeconds
  @jobName

  # Hash of applications of this job
  @Applications


  def initialize(id)
    @ID = id
    @created = Time.now
    @Applications = Hash.new
  end

  def parseData (data)

    if data.has_key? "submitTime" && @submitTime.nil?
      @submitTime = data["submitTime"]
    end
    if data.has_key? "launchTime" && @launchTime.nil?
      @launchTime = data["launchTime"]
    end
    if data.has_key? "firstMapTaskLaunchTime" && @firstMapTaskLaunchTime.nil?
      @firstMapTaskLaunchTime = data["firstMapTaskLaunchTime"]
    end
    if data.has_key? "firstReduceTaskLaunchTime" && @firstReduceTaskLaunchTime.nil?
      @firstReduceTaskLaunchTime = data["firstReduceTaskLaunchTime"]
    end
    if data.has_key? "finishTime" && @finishTime.nil?
      @finishTime = data["finishTime"]
    end
    if data.has_key? "resourcesPerMap" && @resourcesPerMap.nil?
      @resourcesPerMap = data["resourcesPerMap"]
    end
    if data.has_key? "resourcesPerReduce" && @resourcesPerReduce.nil?
      @resourcesPerReduce = data["resourcesPerReduce"]
    end
    if data.has_key? "numMaps" && @numMaps.nil?
      @numMaps = data["numMaps"]
    end
    if data.has_key? "numReduces" && @numReduces.nil?
      @numReduces = data["numReduces"]
    end
    if data.has_key? "username" && @username.nil?
      @username = data["username"]
    end
    if data.has_key? "queue" && @queue.nil?
      @queue = data["queue"]
    end
    if data.has_key? "jobStatus" && @jobStatus.nil?
      @jobStatus = data["jobStatus"]
    end
    if data.has_key? "mapSlotSeconds" && @mapSlotSeconds.nil?
      @mapSlotSeconds = data["mapSlotSeconds"]
    end
    if data.has_key? "reduceSlotSeconds" && @reduceSlotSeconds.nil?
      @reduceSlotSeconds = data["reduceSlotSeconds"]
    end
    if data.has_key? "jobName" && @jobName.nil?
      @jobName = data["jobName"]
    end

  end

end