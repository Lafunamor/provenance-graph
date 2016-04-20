
require_relative 'hadoopcontainerresourceusage'

class HadoopContainer

  @created
  @ID
  @startRequestTime # Time at which the container manager receives a start request
  @username
  @localizerCreatedAt # Time at which the resource localizer was created
  @succeededAt # Time at which the container succeeded
  @cleanUpTime
  @host


  # State changes if the container
  @ContainerStates
  # Sates of the container
  @States
  # List of Events
  @Events
  # Resource usage
  @ResourceUsage


  def initialize(id)
    @ID = id;
    @created = Time.now
    @ContainerStates = Hash.new
    @States = Hash.new
    @Events = Hash.new
    @ResourceUsage = Hash.new
  end

  def parseData (data)
    if data.has_key? "PreviousState"
      @ContainerStates[data["@timestamp"]] = HadoopStateChange.new(data["@timestamp"], data["PreviousState"], data["State"])

    elsif data.has_key? "Event"
      @Events[data["@timestamp"]]= HadoopEvent.new data["@timestamp"], data["Event"]
      # elsif data["message"].include?("Accepted application")
      #   @acceptedAt = data["@timestamp"]
      #   @username = data["UserName"]
      # elsif data["message"].include?("unregistered successfully.")
      #   @unregisteredAt = data["@timestamp"]
      # elsif data["message"].include?("Stopping application")
      #   @stoppingAt = data["@timestamp"]
      # elsif data["message"].include?("Application just finished ")
      #   @endTime = data["@timestamp"]
    elsif data["message"].include?("Start request")
      @startRequestTime = data["@timestamp"]
      if @username.nil?
        @username = data["UserName"]
      end
    elsif data.has_key? "StateChange"
      @States[data["@timestamp"]] = data["StateChange"]
    elsif data["message"].include?("Created localizer")
      @localizerCreatedAt = data["@timestamp"]
    elsif data.has_key? "ProcessTreeID"
      @ResourceUsage[data["@timestamp"]] = HadoopContainerResourceUsage.new(data["@timestamp"],data["ProcessTreeID"],data["UsedPysicalMemory"],data["AvailablePhysicalMemory"],data["UsedVirtualMemory"],data["AvailableVirtualMemory"])
    elsif data["message"].include?("succeeded")
      @succeededAt = data["@timestamp"]
    elsif data["message"].include?("Cleaning up container")
      @cleanUpTime = data["@timestamp"]
    elsif data.has_key? "ReleaseResource"
      @host = data["Host"]
      @capacity = data["capacity"]
    # elsif data["message"].include?("Start request")
    #   getSummary data


    else
      open('/home/cloudera/share/provenance-graph/output/ContainerID.txt', 'a') { |f|
        f.puts data
      }

    end
  end

end