require_relative 'hadoopstatechange'
require_relative 'hadoopevent'

class HadoopApplication

  @created
  @ID

  @queue
  @username
  @unregisteredAt #unregistered at resourcemanager
  @acceptedAt #accepted at scheduler
  @stoppingAt
  @endTime
  @appState
  @appName
  @trackingURL
  @port
  @appMasterHost
  @startTime
  @finishTime
  @finalState

  # Hash of containers of this application
  @Containers
  # HashMap of state changes
  @AppStates
  # HashMap of Events
  @Events

  def initialize(id)
    @ID = id;
    @created = Time.now
    @Containers = Hash.new
    @AppStates = Hash.new
    @Events = Hash.new
  end

  def addContainer(containerID, container)
    @Containers[containerID] = container
  end

  def parseData (data)
    if data.has_key? "AppPreviousState"
      @AppStates[data["@timestamp"]] = HadoopStateChange.new(data["@timestamp"], data["AppPreviousState"], data["AppState"])
    elsif data.has_key? "Event"
      @Events[data["@timestamp"]]= HadoopEvent.new data["@timestamp"], data["Event"]
    elsif data["message"].include?("Accepted application")
      @acceptedAt = data["@timestamp"]
      @username = data["UserName"]
    elsif data["message"].include?("unregistered successfully.")
      @unregisteredAt = data["@timestamp"]
    elsif data["message"].include?("Stopping application")
      @stoppingAt = data["@timestamp"]
    elsif data["message"].include?("Application just finished ")
      @endTime = data["@timestamp"]
    elsif data["message"].include?("ApplicationSummary")
      getSummary data


    else
      open('/home/cloudera/share/provenance-graph/output/ApplicationID.txt', 'a') { |f|
        f.puts data
      }

    end
  end

  def getSummary data

    if data.has_key? "State" && @appState.nil?
      @appState = data["State"]
    end
    if data.has_key? "UserName" && @username.nil?
      @username= data["UserName"]
    end
    if data.has_key? "Name" && @appName.nil?
      @appName = data["Name"]
    end
    if data.has_key? "queue" && @queue.nil?
      @queue = data["queue"]
    end
    if data.has_key? "finishTime" && @finishTime.nil?
      @finishTime = data["finishTime"]
    end
    if data.has_key? "TrackingURL" && @trackingURL.nil?
      @trackingURL = data["TrackingURL"]
    end
    if data.has_key? "port" && @port.nil?
      @port = data["port"]
    end
    if data.has_key? "AppMasterHost" && @appMasterHost.nil?
      @appMasterHost = data["AppMasterHost"]
    end
    if data.has_key? "StartTime" && @startTime.nil?
      @startTime = data["StartTime"]
    end
    if data.has_key? "FinalStatus" && @finalState.nil?
      @finalState = data["FinalStatus"]
    end
  end


end