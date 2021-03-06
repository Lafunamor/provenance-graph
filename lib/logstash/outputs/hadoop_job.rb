require_relative 'hadoop_base'

class HadoopJob < HadoopBase

  def initialize(id)
    @id = id
    @last_edited = Time.now
    # @applications = Hash.new
    @applications = []
    @job_summary = false
    @data = ThreadSafe::Hash.new
    # @data['submit_time'] = @data['launch_time'] = @data['first_map_task_launch_time'] = @data['first_reduce_task_launch_time'] =
    #     @data['finish_time'] = @data['resources_per_map'] = @data['resources_per_reduce'] = @data['num_maps'] =
    #         @data['num_reduces'] = @data['job_status'] = @data['map_slot_seconds'] = @data['reduce_slot_seconds'] =
    #             @data['job_name'] = @username = @queue = ''
    @log_processed_at = (Time.now.to_f * 1000).to_i
    @initial_to_db = true
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
      @data['job_name'] = s(data['jobName'])
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


  def to_db
    if @initial_to_db
      q = ["MERGE (job:job {id: '#{@id}' } ) set job.log_processed_at = coalesce(TOINT(job.log_processed_at),TOINT('#{@log_processed_at}')), job.db_timestamp = coalesce(TOINT(job.db_timestamp), timestamp()) "]
      @initial_to_db = false
    else
      q = ["MERGE (job:job {id: '#{@id}' } ) "]
    end


    if has_job_summary?
      query = "MERGE (job:job {id: '#{@id}' } ) "
      query += " SET
    job.submit_time = TOINT(#{@data['submit_time']}),
        job.launch_time = TOINT(#{@data['launch_time']}),
        job.first_map_task_launch_time = TOINT(#{@data['first_map_task_launch_time']}),
        job.first_reduce_task_launch_time = TOINT(#{@data['first_reduce_task_launch_time']}),
        job.finish_time = TOINT(#{@data['finish_time']}),
        job.resources_per_map = TOINT(#{@data['resources_per_map']}),
        job.resources_per_reduce = TOINT(#{@data['resources_per_reduce']}),
        job.num_maps = TOINT(#{@data['num_maps']}),
        job.num_reduces = TOINT(#{@data['num_reduces']}),
        job.job_status = '#{@data['job_status']}',
        job.map_slot_seconds = TOINT(#{@data['map_slot_seconds']}),
        job.reduce_slot_seconds = TOINT(#{@data['reduce_slot_seconds']}),
        job.job_name = '#{@data['job_name']}';"
      q += [query]
    end

    # add application relations
    @applications.each { |app_id|
      # rel = node.rels(dir: :outgoing, between: app.node)
      # if rel.length == 0
      #   @node.create_rel(:has, app.node)
      # end
      query = " merge (j:job {id: '#{@id}'}) "
      query += "merge (ju:application {id: '#{app_id}'}) merge (j)-[:has]->(ju); "
      q += [query]
    }

    # Neo4j::Session.current.query("merge (j:job {id: '#{@id}'}) merge (u:application {id: '#{app_id}'}) create unique (j)-[:has]->(u)")

    unless @username == '' || @username.nil?
      # user = get_create_username(@username)
      # rel = node.rels(dir: :outgoing, between: user)
      # if rel.length == 0
      #   @node.create_rel(:belongs_to, user)
      # end
      # Neo4j::Session.current.query("merge (j:job {id: '#{@id}'}) merge (u:user {name: '#{@username}'}) create unique (j)-[:belongs_to]->(u#{@username})")
      query = " merge (j:job {id: '#{@id}'}) "
      query += "merge (ju:user {name: '#{@username}'}) merge (j)-[:belongs_to]->(ju); "
      q += [query]
    end


    unless @queue == '' || @queue.nil?
      # q = get_create_queue(@queue)
      # rel = node.rels(dir: :outgoing, between: q)
      # if rel.length == 0
      #   @node.create_rel(:used_queue, q)
      # end
      # Neo4j::Session.current.query("merge (j:job {id: '#{@id}'}) merge (u:queue {name: '#{@queue}'}) create unique (j)-[:used_queue]->(u)")
      query = " merge (j:job {id: '#{@id}'}) "
      query += "merge (ju:queue {name: '#{@queue}'}) merge (j)-[:used_queue]->(ju); "
      q += [query]
    end

    q

  end

  def to_csv(path)
    if @job_summary
      File.open(path + 'jobs.csv', 'a') { |f|

        f.puts @id +','+ @data['submit_time'] +','+ @data['launch_time'] +','+ @data['first_map_task_launch_time'] +','+ @data['first_reduce_task_launch_time'] +
                   ','+ @data['finish_time'] +','+ @data['resources_per_map'] +','+ @data['resources_per_reduce'] +','+ @data['num_maps'] +
                   ','+ @data['num_reduces'] +','+ @data['job_status'] +','+ @data['map_slot_seconds'] +','+ @data['reduce_slot_seconds'] +
                   ','+ @data['job_name'] +','+ @username +','+ @queue
      }
    end
    unless @applications.empty?
      File.open(path + 'jobs_apps.csv', 'a') { |g|
        g.puts to_csv2
      }
    end
  end

  def to_csv2
    string = ''
    @applications.each { |app_id|
      string += @id +','+ app_id + "\n"
    }
    string

  end

  def csv_header
    'id,submit_time,launch_time,first_map_task_launch_time,first_reduce_task_launch_time,finish_time,resources_per_map,resources_per_reduce,num_maps,num_reduces,job_status,map_slot_seconds,reduce_slot_seconds,job_name,username,queue'
  end

  def get_apps
    @applications
  end

end