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

  def addApp appID, app
    @Applications[appID] = app
  end

  def parseData (data)

    if data.has_key? "submitTime"
      @submitTime = data["submitTime"]
    end
    if data.has_key? "launchTime"
      @launchTime = data["launchTime"]
    end
    if data.has_key? "firstMapTaskLaunchTime"
      @firstMapTaskLaunchTime = data["firstMapTaskLaunchTime"]
    end
    if data.has_key? "firstReduceTaskLaunchTime"
      @firstReduceTaskLaunchTime = data["firstReduceTaskLaunchTime"]
    end
    if data.has_key? "finishTime"
      @finishTime = data["finishTime"]
    end
    if data.has_key? "resourcesPerMap"
      @resourcesPerMap = data["resourcesPerMap"]
    end
    if data.has_key? "resourcesPerReduce"
      @resourcesPerReduce = data["resourcesPerReduce"]
    end
    if data.has_key? "numMaps"
      @numMaps = data["numMaps"]
    end
    if data.has_key? "numReduces"
      @numReduces = data["numReduces"]
    end
    if data.has_key? "username"
      @username = data["username"]
    end
    if data.has_key? "queue"
      @queue = data["queue"]
    end
    if data.has_key? "jobStatus"
      @jobStatus = data["jobStatus"]
    end
    if data.has_key? "mapSlotSeconds"
      @mapSlotSeconds = data["mapSlotSeconds"]
    end
    if data.has_key? "reduceSlotSeconds"
      @reduceSlotSeconds = data["reduceSlotSeconds"]
    end
    if data.has_key? "jobName"
      @jobName = data["jobName"]
    end
    if data["message"].include?("JobSummary")
      open('/home/cloudera/share/provenance-graph/output/jobs2.txt', 'a') { |f|
        f.puts "Summary found"
        f.puts data
      }
    end
    return true
  end

end