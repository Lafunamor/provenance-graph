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
  @masterContainer


  # HashMap of state changes
  @AppStates
  # HashMap of Events
  @Events
  # Hash of AppAttempts
  @AppAttempts

  def initialize(id)
    @ID = id;
    @created = Time.now
    @AppStates = Hash.new
    @Events = Hash.new
    @AppAttempts = Hash.new
  end


  def addAttempt(attemptID, attempt)
    @AppAttempts[attemptID]= attempt
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
      return false

    end
    return true
  end

  def getSummary data

    if data.has_key? "State"
      @appState = data["State"]
    end
    if data.has_key? "UserName"
      @username= data["UserName"]
    end
    if data.has_key? "Name"
      @appName = data["Name"]
    end
    if data.has_key? "queue"
      @queue = data["queue"]
    end
    if data.has_key? "finishTime"
      @finishTime = data["finishTime"]
    end
    if data.has_key? "TrackingURL"
      @trackingURL = data["TrackingURL"]
    end
    if data.has_key? "port"
      @port = data["port"]
    end
    if data.has_key? "AppMasterHost"
      @appMasterHost = data["AppMasterHost"]
    end
    if data.has_key? "StartTime"
      @startTime = data["StartTime"]
    end
    if data.has_key? "FinalStatus"
      @finalState = data["FinalStatus"]
    end

  end


end