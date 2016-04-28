# encoding: utf-8
require "logstash/outputs/base"
require "logstash/namespace"
require_relative 'hadoopjob'
require_relative 'hadoopapplication'
require_relative 'hadoopcontainer'
require_relative 'hadoopappattempt'

# An example output that does nothing.
class LogStash::Outputs::ProvenanceGraph < LogStash::Outputs::Base
  config_name "provenancegraph"

  @Jobs
  # config :fields, :validate => :array, :required => true
  #:fields
  config :path, :validate => :string, :required => true
  default :codec, "json_lines"

  public
  def register
    @Jobs = Hash.new
    @Applications = Hash.new
    @AppAttempts = Hash.new
    @Containers = Hash.new
  end

  # def register

  public
  def receive(event)

    data = event.to_hash
    type = nil

    if data.has_key? ("ContainerID")
      ids = data["ContainerID"].split('_')
      jobID = ids[1]
      appID = ids[1] +'_'+ ids[2]
      appAttemptID = ids[1] +'_'+ ids[2] +'_'+ ids[3]
      containerID = ids[1] +'_'+ ids[2] +'_'+ ids[3] +'_'+ ids[4]
      type = "container"
    elsif data.has_key? ("AppAttemptID")
      ids = data["AppAttemptID"].split('_')
      jobID = ids[1]
      appID = ids[1] +'_'+ ids[2]
      appAttemptID = ids[1] +'_'+ ids[2] +'_'+ ids[3]
      type = "attempt"
    elsif data.has_key?("ApplicationID")
      ids = data["ApplicationID"].split('_')
      jobID = ids[1]
      appID = ids[1] +'_'+ ids[2]
      type = "app"
    elsif data.has_key? ("JobID")
      jobID = data["JobID"].split('_')[1]
      type = "job"
    end
    if type.nil?
      return
    end


    case type
      when "job"
        job = getCreateJob jobID
        unless job.parseData data
          unhandled data
        end
      when "app"
        app = getCreateApp appID, jobID
        unless app.parseData data
          unhandled data
        end
      when "attempt"
        appAttempt = getCreateAttempt appAttemptID, appID, jobID
        unless appAttempt.parseData data
          unhandled data
        end
      when "container"
        container = getCreateContainer containerID, appAttemptID, appID, jobID
        unless container.parseData data
          unhandled data
        end
    end

    open(path + 'file.txt', 'a') { |f|
      f.puts data
    }

    #if data["message"].include?("JobSummary")
      open(path + 'jobs.txt', 'a') { |f|
        f.puts @Jobs
      }
    #end

    #File.write('/home/cloudera/file.txt', event.to_hash)

  end

  def unhandled data
    open(path + 'unhandled.txt', 'a') { |f|
      f.puts data
    }
  end

  def getCreateJob jobID
    if @Jobs.has_key? jobID
      job = @Jobs[jobID]
    else
      job = HadoopJob.new (jobID)
      @Jobs[jobID] = job
    end
    return job
  end

  def getCreateApp appID, jobID
    if @Applications.has_key? appID
      app = @Applications[appID]
    else
      app = HadoopApplication.new (appID)
      @Applications[appID] = app
      getCreateJob(jobID).addApp appID, app
    end
    return app
  end

  def getCreateAttempt appAttemptID, appID, jobID
    if @AppAttempts.has_key? appAttemptID
      appAttempt = @AppAttempts[appAttemptID]
    else
      appAttempt = HadoopAppAttempt.new (appAttemptID)
      @AppAttempts[appAttemptID] = appAttempt
      getCreateApp(appID,jobID).addAttempt appAttemptID, appAttempt
    end
    return appAttempt
  end

  def getCreateContainer containerID, appAttemptID, appID, jobID
    if @Containers.has_key? containerID
      container = @Containers[containerID]
    else
      container = HadoopContainer.new(containerID)
      @Containers[containerID] = container
     getCreateAttempt(appAttemptID,appID,jobID).addContainer containerID, container
    end
    return container
  end


end # class LogStash::Outputs::Example

