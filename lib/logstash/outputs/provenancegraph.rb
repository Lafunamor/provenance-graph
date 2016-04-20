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
  default :codec, "json_lines"

  public
  def register
    @Jobs = Hash.new
    @Applications = Hash.new
    @AppAttempts = Hash.new
    @Containers = Hash.new
  end # def register

  public
  def receive(event)

    data = event.to_hash

    # handle job related messages
    if data.has_key? "JobID"
      jobID = data["JobID"]
      if @Jobs.has_key? jobID
        job = @Jobs[jobID]
        job.parseData data
      else
        job = HadoopJob.new (jobID)
        @Jobs[jobID] = job
        job.parseData data
      end
      open('/home/cloudera/share/provenance-graph/output/JobID2.txt', 'a') { |f|
        f.puts @Jobs
      }

      # handle application related messages
    elsif data.has_key? "ApplicationID"
      appID = data["ApplicationID"]
      if @Applications.has_key? appID
        app = @Applications[appID]
      else
        app = HadoopApplication.new (appID)
        @Applications[appID] = app
      end
      if data.has_key? "ContainerID"
        containerID = data["ContainerID"]
        if @Containers.has_key? containerID
          container = @Containers[containerID]
        else
          container = HadoopContainer.new(containerID)
          @Containers[containerID] = container
        end
        app.addContainer containerID, container
      else
        app.parseData data
      end
      open('/home/cloudera/share/provenance-graph/output/ApplicationID2.txt', 'a') { |f|
        f.puts @Applications
      }



    elsif data.has_key? "AppAttemptID"
      appAttemptID = data["AppAttemptID"]
      if @AppAttempts.has_key? appAttemptID
        appAttempt = @AppAttempts[appAttemptID]
      else
        appAttempt = HadoopAppAttempt.new (appAttemptID)
        @AppAttempts[appAttemptID] = appAttempt
      end
      if data.has_key? "ContainerID"
        containerID = data["ContainerID"]
        if @Containers.has_key? containerID
          container = @Containers[containerID]
        else
          container = HadoopContainer.new(containerID)
          @Containers[containerID] = container
        end
        appAttempt.addContainer containerID, container
      else
        appAttempt.parseData data
      end
      open('/home/cloudera/share/provenance-graph/output/AppAttemptID2.txt', 'a') { |f|
        f.puts @AppAttempts
      }



    elsif data.has_key? "ContainerID"
      containerID = data["ContainerID"]
      if @Containers.has_key? containerID
        container = @Containers[containerID]
      else
        container = HadoopContainer.new(containerID)
        @Containers[containerID] = container
      end
      container.parseData data
      open('/home/cloudera/share/provenance-graph/output/ContainerID2.txt', 'a') { |f|
        f.puts @Containers
      }
    end

    open('/home/cloudera/share/provenance-graph/output/file.txt', 'a') { |f|
      f.puts data
    }

    #File.write('/home/cloudera/file.txt', event.to_hash)

  end # def event






  private
  def get_value(name, event)
    val = event[name]
    val.is_a?(Hash) ? LogStash::Json.dump(val) : (val)
  end



end # class LogStash::Outputs::Example

