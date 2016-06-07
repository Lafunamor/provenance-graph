require_relative 'hadoop_base'

class HadoopJob < HadoopBase

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
    @data = ThreadSafe::Hash.new
  end


  def add_app(app_id)
    unless @applications.include? app_id
      @applications += [app_id]
      # @node.create_rel(:has, app.node)
    end
    # unless @applications.include? app
    #   @applications += [app]
    # end
  end

  def parse_data (data)

    if data.has_key? 'submitTime'
      @data['submit_time'] = data['submitTime']
    end
    if data.has_key? 'launchTime'
      @data['launch_time'] = data['launchTime']
    end
    if data.has_key? 'firstMapTaskLaunchTime'
      @data['first_map_task_launch_time'] = data['firstMapTaskLaunchTime']
    end
    if data.has_key? 'firstReduceTaskLaunchTime'
      @data['first_reduce_task_launch_time'] = data['firstReduceTaskLaunchTime']
    end
    if data.has_key? 'finishTime'
      @data['finish_time'] = data['finishTime']
    end
    if data.has_key? 'resourcesPerMap'
      @data['resources_per_map'] = data['resourcesPerMap']
    end
    if data.has_key? 'resourcesPerReduce'
      @data['resources_per_reduce'] = data['resourcesPerReduce']
    end
    if data.has_key? 'numMaps'
      @data['num_maps'] = data['numMaps']
    end
    if data.has_key? 'numReduces'
      @data['num_reduces'] = data['numReduces']
    end


    if data.has_key? 'username'
      @username = data['username']

    end
    if data.has_key? 'queue'
      @queue = data['queue']

    end


    if data.has_key? 'JobStatus'
      @data['job_status'] = data['JobStatus']
    end
    if data.has_key? 'mapSlotSeconds'
      @data['map_slot_seconds'] = data['mapSlotSeconds']
    end
    if data.has_key? 'reduceSlotSeconds'
      @data['reduce_slot_seconds'] = data['reduceSlotSeconds']
    end
    if data.has_key? 'jobName'
      @data['job_name'] = data['jobName']
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

  def node
    if @node.nil?
      @node = get_create_job(@id)
    end
    return @node
  end

  def to_db

    node
    node.update_props(@data)

    # add application relations
    query = " merge (j#{@id}:job {id: '#{@id}'}) "
    @applications.each { |app_id|
      # rel = node.rels(dir: :outgoing, between: app.node)
      # if rel.length == 0
      #   @node.create_rel(:has, app.node)
      # end
      query += "merge (ju#{app_id}:application {id: '#{app_id}'}) create unique (j#{@id})-[:has]->(ju#{app_id}) "
    }

    # Neo4j::Session.current.query("merge (j:job {id: '#{@id}'}) merge (u:application {id: '#{app_id}'}) create unique (j)-[:has]->(u)")

    unless @username.nil?
      # user = get_create_username(@username)
      # rel = node.rels(dir: :outgoing, between: user)
      # if rel.length == 0
      #   @node.create_rel(:belongs_to, user)
      # end
      # Neo4j::Session.current.query("merge (j:job {id: '#{@id}'}) merge (u:user {name: '#{@username}'}) create unique (j)-[:belongs_to]->(u#{@username})")
      query += "merge (ju#{@username+@id}:user {name: '#{@username}'}) create unique (j#{@id})-[:belongs_to]->(ju#{@username+@id}) "
    end


    unless @queue.nil?
      # q = get_create_queue(@queue)
      # rel = node.rels(dir: :outgoing, between: q)
      # if rel.length == 0
      #   @node.create_rel(:used_queue, q)
      # end
      # Neo4j::Session.current.query("merge (j:job {id: '#{@id}'}) merge (u:queue {name: '#{@queue}'}) create unique (j)-[:used_queue]->(u)")
      query += "merge (ju#{@queue+@id}:queue {name: '#{@queue}'}) create unique (j#{@id})-[:used_queue]->(ju#{@queue+@id}) "
    end

    query

  end

  def to_csv

  end


end