require_relative 'hadoopstatechange'
require_relative 'hadoopevent'

class HadoopAppAttempt

  @created
  @ID

  @queue
  @username
  @unregisteredAt #unregistered at resourcemanager
  @acceptedAt #accepted at scheduler
  @stoppingAt
  @endTime
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
    if data.has_key? "PreviousState"
      @AppStates[data["@timestamp"]] = HadoopStateChange.new(data["@timestamp"], data["PreviousState"], data["State"])
    elsif data.has_key? "Event"
      @Events[data["@timestamp"]]= HadoopEvent.new data["@timestamp"], data["Event"]

    elsif data["message"].include?("is done.")
      getSummary data
    elsif data["message"].include?("Storing attempt")
      getSummary data

    else
      return false

    end
    return true
  end

  def getSummary data
    if data.has_key? "FinalState"
      @finalState = data["FinalState"]
      @endTime = data["@timestamp"]
    end
    if data.has_key? "MasterContainerID"
      @masterContainer = data["MasterContainerID"]
    end
    if data.has_key? "Node"
      @node = data["Node"]
    end
    if data.has_key? "NodeHTTPAddress"
      @nodeHTTPAdr = data["NodeHTTPAddress"]
    end
    if data.has_key? "resource"
      @resource = data["resource"]
    end
    if data.has_key? "Priority"
      @priority = data["Priority"]
    end
    if data.has_key? "Token"
      @token = data["Token"]
    end
  end


end