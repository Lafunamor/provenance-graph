class HadoopJob

  # @created
  # @ID
  # @submitTime
  # @launchTime
  # @firstMapTaskLaunchTime
  # @firstReduceTaskLaunchTime
  # @finishTime
  # @resourcesPerMap
  # @resourcesPerReduce
  # @numMaps
  # @numReduces
  # @username
  # @queue
  # @jobStatus
  # @mapSlotSeconds
  # @reduceSlotSeconds
  # @jobName

  # Hash of applications of this job
  # @applications


  def initialize(id)
    @id = id
    @last_edited = Time.now
    # @applications = Hash.new
    @applications = []
    @job_summary = false
  end

  # def add_app(app_id, app)
  # @applications[app_id] = app
  # end

  def add_app(app_id)
    unless @applications.include? app_id
      @applications += [app_id]
    end
  end

  def parse_data (data)

    if data.has_key? 'submitTime'
      @submit_time = data['submitTime']
    end
    if data.has_key? 'launchTime'
      @launch_time = data['launchTime']
    end
    if data.has_key? 'firstMapTaskLaunchTime'
      @first_map_task_launch_time = data['firstMapTaskLaunchTime']
    end
    if data.has_key? 'firstReduceTaskLaunchTime'
      @first_reduce_task_launch_time = data['firstReduceTaskLaunchTime']
    end
    if data.has_key? 'finishTime'
      @finish_time = data['finishTime']
    end
    if data.has_key? 'resourcesPerMap'
      @resources_per_map = data['resourcesPerMap']
    end
    if data.has_key? 'resourcesPerReduce'
      @resources_per_reduce = data['resourcesPerReduce']
    end
    if data.has_key? 'numMaps'
      @num_maps = data['numMaps']
    end
    if data.has_key? 'numReduces'
      @num_reduces = data['numReduces']
    end
    if data.has_key? 'username'
      @username = data['username']
    end
    if data.has_key? 'queue'
      @queue = data['queue']
    end
    if data.has_key? 'JobStatus'
      @job_status = data['JobStatus']
    end
    if data.has_key? 'mapSlotSeconds'
      @map_slot_seconds = data['mapSlotSeconds']
    end
    if data.has_key? 'reduceSlotSeconds'
      @reduce_slot_seconds = data['reduceSlotSeconds']
    end
    if data.has_key? 'jobName'
      @job_name = data['jobName']
    end
    if data['message'].include?('JobSummary')
      @job_summary = true
    end
    @last_edited = Time.now
    return true
  end

  def has_job_summary?
    @job_summary
  end

  def last_edited
    return @last_edited
  end

end