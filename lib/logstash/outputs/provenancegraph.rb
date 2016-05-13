# encoding: utf-8
require 'logstash/outputs/base'
require 'logstash/namespace'
require_relative 'hadoop_job'
require_relative 'hadoop_application'
require_relative 'hadoop_container'
require_relative 'hadoop_app_attempt'
require_relative 'hadoop_hdfs_block'
require 'rubygems'
require 'json'

# An example output that does nothing.
class LogStash::Outputs::ProvenanceGraph < LogStash::Outputs::Base
  config_name 'provenancegraph'

  # @Jobs
  # config :fields, :validate => :array, :required => true
  #:fields
  config :path, :validate => :string, :required => true
  default :codec, 'json_lines'

  public
  def register
    @jobs = Hash.new
    @applications = Hash.new
    @app_attempts = Hash.new
    @containers = Hash.new
    @blocks = Hash.new
    deserialize
  end

  # def register

  public
  def receive(event)

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
        block = get_create_block block_id
        unless block.parse_data data
          unhandled data
        end
      else
        unhandled data
        return
    end

    # open(path + 'file.txt', 'a') { |f|
    #   f.puts data
    # }

    # if data["message"].include?("JobSummary")
    open(path + 'jobs.txt', 'w') { |f|
      # @jobs.each {|j|
      #   f.puts j}
      # f.puts JSON.pretty_generate(@jobs, :object_nl => '\n')
      # f.puts '**************************************************'
      f.puts @jobs
    }
    open(path + 'hdfs.txt', 'w') { |f|
      # @blocks.each {|j|
      #   f.puts j}
      # f.puts JSON.pretty_generate(@blocks, :object_nl => '\n')
      # f.puts '**************************************************'
      f.puts @blocks
    }
    #end
    # File.write(path + 'jobs.txt', @jobs)
    # File.write(path + 'hdfs.txt', @blocks)

    #File.write('/home/cloudera/file.txt', event.to_hash)
    serialize
  end

  def deserialize
    if File.file?(path + '.save_job.yaml')
      @jobs = YAML.load(File.read(path + '.save_job.yaml'))
    end
    if File.file?(path + '.save_apps.yaml')
      @applications = YAML.load(File.read(path + '.save_apps.yaml'))
    end
    if File.file?(path + '.save_appattempt.yaml')
      @app_attempts = YAML.load(File.read(path + '.save_appattempt.yaml'))
    end
    if File.file?(path + '.save_joby.yaml')
      @containers = YAML.load(File.read(path + '.save_containers.yaml'))
    end
    if File.file?(path + '.save_containers.yaml')
      @blocks =YAML.load(File.read(path + '.save_blocks.yaml'))
    end
  end

  def serialize
    File.open(path + '.save_job.yaml', 'w') { |f|
      f.write(YAML.dump(@jobs))
    }
    File.open(path + '.save_apps.yaml', 'w') { |f|
      f.write(YAML.dump(@applications))
    }
    File.open(path + '.save_appattempt.yaml', 'w') { |f|
      f.write(YAML.dump(@app_attempts))
    }
    File.open(path + '.save_containers.yaml', 'w') { |f|
      f.write(YAML.dump(@containers))
    }


    File.open(path + '.save_blocks.yaml', 'w') { |f|
      f.write(YAML.dump(@blocks))
    }
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
      get_create_job(job_id).add_app app_id
    end
    return app
  end

  def get_create_attempt(app_attempt_id, app_id, job_id)
    if @app_attempts.has_key? app_attempt_id
      app_attempt = @app_attempts[app_attempt_id]
    else
      app_attempt = HadoopAppAttempt.new (app_attempt_id)
      @app_attempts[app_attempt_id] = app_attempt
      get_create_app(app_id, job_id).add_attempt app_attempt_id
    end
    return app_attempt
  end

  def get_create_container(container_id, app_attempt_id, app_id, job_id)
    if @containers.has_key? container_id
      container = @containers[container_id]
    else
      container = HadoopContainer.new(container_id)
      @containers[container_id] = container
      get_create_attempt(app_attempt_id, app_id, job_id).add_container container_id
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


end # class LogStash::Outputs::Example

