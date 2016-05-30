# encoding: utf-8
require 'logstash/outputs/base'
require 'logstash/namespace'
require_relative 'hadoop_job'
require_relative 'hadoop_application'
require_relative 'hadoop_container'
require_relative 'hadoop_app_attempt'
require_relative 'hadoop_hdfs_block'
require 'rubygems'
require 'concurrent'
require 'thread_safe'
require 'thread'
require 'neo4j-core'
require 'neo4j-embedded'
require 'neo4j-embedded/embedded_ha_session'


# An example output that does nothing.
class LogStash::Outputs::ProvenanceGraph < LogStash::Outputs::Base
  config_name 'provenancegraph'

  # @Jobs
  # config :fields, :validate => :array, :required => true
  #:fields
  config :path, :validate => :string, :required => true
  config :flush, :validate => :number, :required => false

  # configuration for the neo4j graph db
  config :neo4j_server, :validate => :string, :required => true
  config :neo4j_username, :validate => :string, :required => true
  config :neo4j_password, :validate => :string, :required => true

  # defaults
  default :flush, '300'
  default :codec, 'json_lines'


  public
  def register
    @logger.debug('starting', :plugin => self)
    @count = 0
    @logger.debug('loading data', :plugin => self)
    # deserialize
    if @jobs.nil? || @jobs == false
      @jobs = ThreadSafe::Hash.new
    end
    if @applications.nil? || @applications == false
      @applications = ThreadSafe::Hash.new
    end
    if @app_attempts.nil? || @app_attempts == false
      @app_attempts = ThreadSafe::Hash.new
    end
    if @containers.nil? || @containers == false
      @containers = ThreadSafe::Hash.new
    end
    if @blocks.nil? || @blocks == false
      @blocks = ThreadSafe::Hash.new
    end
    @write_thread = nil
    @last_flush = Time.now
    @eps = 0
    @last_count = 0
    @logger.debug('finished initialisation', :plugin => self)

    # Using Neo4j Server Cypher Database
    begin
      @session = Neo4j::Session.open(:server_db, @neo4j_server, basic_auth: {username: @neo4j_username, password: @neo4j_password})

      # embeded
      # # /var/lib/neo4j/data/
      # @session = Neo4j::Session.open(:ha_db, '/var/lib/neo4j/data/', auto_commit: true)
      # @session.start
      Neo4j::Label.create(:job).create_index(:id)
      Neo4j::Label.create(:application).create_index(:id)
      Neo4j::Label.create(:attempt).create_index(:id)
      Neo4j::Label.create(:container).create_index(:id)
      Neo4j::Label.create(:file).create_index(:name)
      Neo4j::Label.create(:queue).create_index(:name)
      Neo4j::Label.create(:host).create_index(:name)
      Neo4j::Label.create(:user).create_index(:username)

    rescue Error => error
      @logger.error('Could not connect to neo4j DB' + error, :plugin => self)
    end


  end


  public
  def receive(event)
    @count +=1
    data = event.to_hash
    type = nil

    if data.has_key? ('ContainerID')
      ids = data['ContainerID'].split('_')
      job_id = ids[1]+'_'+ ids[2]
      app_id = ids[1]+'_'+ ids[2]
      app_attempt_id = ids[1] +'_'+ ids[2] +'_0000'+ ids[3] #containerIDs do not use the same length for the app attempt id
      container_id = ids[1] +'_'+ ids[2] +'_'+ ids[3] +'_'+ ids[4]
      type = 'container'
    elsif data.has_key? ('AppAttemptID')
      ids = data['AppAttemptID'].split('_')
      job_id = ids[1]+'_'+ ids[2]
      app_id = ids[1]+'_'+ ids[2]
      app_attempt_id = ids[1] +'_'+ ids[2] +'_'+ ids[3]
      type = 'attempt'
    elsif data.has_key? ('DFSClientID')
      ids = data['DFSClientID'].split('_')
      job_id = ids[2]+'_'+ ids[3]
      app_id = ids[2]+'_'+ ids[3]
      block_id = data['Block_ID']
      if ids[1]== 'attempt'
        type = 'hdfs_trace'
      end
    elsif data.has_key?('ApplicationID')
      ids = data['ApplicationID'].split('_')
      job_id = ids[1]+'_'+ ids[2]
      app_id = ids[1]+'_'+ ids[2]
      type = 'app'
    elsif data.has_key? ('JobID')
      ids = data['JobID'].split('_')
      job_id = job_id = ids[1]+'_'+ ids[2]
      type = 'job'
    elsif data.has_key?('Block_ID')
      block_id = data["Block_ID"]
      type = 'hdfs'
    end
    if type.nil?
      return
    end


    case type
      when 'job'
        # noinspection RubyScope
        job = get_create_job job_id
        unless job.parse_data data
          unhandled data
        end
      when 'app'
        # noinspection RubyScope
        app = get_create_app app_id, job_id
        unless app.parse_data data
          unhandled data
        end
      when 'hdfs_trace'
        app = get_create_app app_id, job_id
        unless app.add_hdfs_trace data, get_create_block(block_id)
          unhandled data
        end
      when 'attempt'
        # noinspection RubyScope
        app_attempt = get_create_attempt app_attempt_id, app_id, job_id
        unless app_attempt.parse_data data
          unhandled data
        end
      when 'container'
        # noinspection RubyScope
        container = get_create_container container_id, app_attempt_id, app_id, job_id
        unless container.parse_data data
          unhandled data
        end
      when 'hdfs'
        unless data.has_key?('srvID')
          block = get_create_block block_id
          unless block.parse_data data
            unhandled data
          end
        end
      else
        unhandled data
        return
    end

    time_difference = Time.now - @last_flush
    @logger.debug('time difference: ' + time_difference.to_s, :plugin => self)
    if time_difference >= 30

      @eps = (@eps + (@count-@last_count)/time_difference)/2
      @last_count = @count
      counter = @count

      if @write_thread.nil? || @write_thread.stop?
        @last_flush = Time.now
        @write_thread = Thread.new(counter, @eps, @jobs.clone, @applications.clone, @app_attempts.clone, @containers.clone, @blocks.clone) {
            |count, eps, jobs, apps, app_attempts, containers, blocks|
          open(path + 'count.txt', 'w') { |f|
            f.puts count
            f.puts eps
          }
          # serialize(jobs, apps, app_attempts, containers, blocks)
          start = Time.now
          flush_to_db(jobs, apps, app_attempts, containers, blocks)
          end_time = Time.now
          open(path + 'to_db.txt', 'w') { |f|
            f.puts 'written ' + end_time.to_s
            f.puts 'elapsed time: ' + (end_time - start).to_s
            f.puts count
          }
        }
        remove_old_data
      end

      # unhandled(data)
    end

  end

  def deserialize
    if File.exists?(path + '.save_job.yaml')
      @jobs = YAML.load(File.read(path + '.save_job.yaml'))
    end
    if File.exists?(path + '.save_apps.yaml')
      @applications = YAML.load(File.read(path + '.save_apps.yaml'))
    end
    if File.exists?(path + '.save_appattempt.yaml')
      @app_attempts = YAML.load(File.read(path + '.save_appattempt.yaml'))
    end
    if File.exists?(path + '.save_containers.yaml')
      @containers = YAML.load(File.read(path + '.save_containers.yaml'))
    end
    if File.exists?(path + '.save_blocks.yaml')
      @blocks = YAML.load(File.read(path + '.save_blocks.yaml'))
    end
  end

  def serialize(jobs, apps, app_attempts, containers, blocks)
    File.open(path + '.save_job.yaml', 'w') { |f|
      f.write(YAML.dump(jobs))
    }
    File.open(path + '.save_apps.yaml', 'w') { |f|
      f.write(YAML.dump(apps))
    }
    File.open(path + '.save_appattempt.yaml', 'w') { |f|
      f.write(YAML.dump(app_attempts))
    }
    File.open(path + '.save_containers.yaml', 'w') { |f|
      f.write(YAML.dump(containers))
    }
    File.open(path + '.save_blocks.yaml', 'w') { |f|
      f.write(YAML.dump(blocks))
    }
  end

  def flush_to_db(jobs, apps, app_attempts, containers, blocks)
    blocks.each { |k, v| v.to_db }
    containers.each { |k, v| v.to_db }
    app_attempts.each { |k, v| v.to_db }
    apps.each { |k, v| v.to_db }
    jobs.each { |k, v| v.to_db }
  end

  def remove_old_data
    @jobs = ThreadSafe::Hash.new
    @applications = ThreadSafe::Hash.new
    @app_attempts = ThreadSafe::Hash.new
    @containers = ThreadSafe::Hash.new
    @blocks = ThreadSafe::Hash.new
    # @jobs.delete_if { |key, value| value.has_job_summary? }
    # @jobs.delete_if { |key, value| @last_flush - value.last_edited >= 120 }
    # @applications.delete_if { |key, value| @last_flush - value.last_edited >= 120 }
    # @app_attempts.delete_if { |key, value| @last_flush - value.last_edited >= 120 }
    # @containers.delete_if { |key, value| @last_flush - value.last_edited >= 120 }
    # @blocks.delete_if { |key, value| @last_flush - value.last_edited >= 120 }
  end


  def unhandled(data)
    open(path + 'unhandled.txt', 'a') { |f|
      f.puts data
    }
  end

  def get_create_job(job_id)
    if @jobs.has_key? job_id
      job = @jobs[job_id]
    else
      job = HadoopJob.new (job_id)
      @jobs[job_id] = job
    end
    return job
  end

  def get_create_app(app_id, job_id)
    if @applications.has_key? app_id
      app = @applications[app_id]
    else
      app = HadoopApplication.new (app_id)
      @applications[app_id] = app
      get_create_job(job_id).add_app app
    end
    return app
  end

  def get_create_attempt(app_attempt_id, app_id, job_id)
    if @app_attempts.has_key? app_attempt_id
      app_attempt = @app_attempts[app_attempt_id]
    else
      app_attempt = HadoopAppAttempt.new (app_attempt_id)
      @app_attempts[app_attempt_id] = app_attempt
      get_create_app(app_id, job_id).add_attempt app_attempt
    end
    return app_attempt
  end

  def get_create_container(container_id, app_attempt_id, app_id, job_id)
    if @containers.has_key? container_id
      container = @containers[container_id]
    else
      container = HadoopContainer.new(container_id)
      @containers[container_id] = container
      get_create_attempt(app_attempt_id, app_id, job_id).add_container container
    end
    return container
  end

  def get_create_block(block_id)
    if @blocks.has_key? block_id
      block = @blocks[block_id]
    else
      block = HadoopHDFSBlock.new(block_id)
      @blocks[block_id]=block
    end
    return block
  end

  # handles shutdown of pipeline
  def close
    @logger.info('clean shutdown, flushing data', :plugin => self)
    flush_to_db(@jobs, @applications, @app_attempts, @containers, @blocks)
    @logger.info('writing to disk completed, shutting down', :plugin => self)

    session.close
  end

end # class LogStash::Outputs::ProvenanceGraph

