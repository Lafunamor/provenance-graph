
require_relative 'hadoopcontainerresourceusage'

class HadoopContainer

  # @created
  # @ID
  # @startRequestTime # Time at which the container manager receives a start request
  # @username
  # @localizerCreatedAt # Time at which the resource localizer was created
  # @succeededAt # Time at which the container succeeded
  # @cleanUpTime
  # @host
  # @addedToAppAt
  # @removedFromApp
  # @stoppedAt
  # @startedAt
  # @arguments
  #
  #
  # # State changes if the container
  # @ContainerStates
  # # Sates of the container
  # @States
  # # List of Events
  # @Events
  # # Resource usage
  # @ResourceUsage


  def initialize(id)
    @id = id
    @created = Time.now
    @container_states = Hash.new
    @states = Hash.new
    @events = Hash.new
    @resource_usage = Hash.new
  end

  def parse_data (data)
    if data.has_key? 'PreviousState'
      @container_states[data['@timestamp']] = HadoopStateChange.new(data['@timestamp'], data['PreviousState'], data['State'])

    elsif data.has_key? 'Event'
      @events[data['@timestamp']]= HadoopEvent.new data['@timestamp'], data['Event']

    elsif data['message'].include?('Start request')
      @start_request_time = data['@timestamp']
      if @username.nil?
        @username = data['UserName']
      end
    elsif data.has_key? 'StateChange'
      @states[data['@timestamp']] = data['StateChange']
    elsif data['message'].include?('Created localizer')
      @localizer_created_at = data['@timestamp']
    elsif data.has_key? 'ProcessTreeID'
      @resource_usage[data['@timestamp']] = HadoopContainerResourceUsage.new(data['@timestamp'], data['ProcessTreeID'], data['UsedPysicalMemory'], data['AvailablePhysicalMemory'], data['UsedVirtualMemory'], data['AvailableVirtualMemory'])
    elsif data['message'].include?('succeeded')
      @succeeded_at = data['@timestamp']
    elsif data['message'].include?('Cleaning up container')
      @clean_up_time = data['@timestamp']
    elsif data.has_key? 'ReleaseResource'
      @host = data['Host']
      @capacity = data['capacity']
    # elsif data["message"].include?("Start request")
    #   getSummary data
    elsif data['message'].include?('Adding container')
      @added_to_app_at = data['@timestamp']
    elsif data['message'].include?('Removing container')
      @removed_from_app = data['@timestamp']
    elsif data['message'].include?('Stopping container with container Id')
      @stopped_at = data['@timestamp']
    elsif data.has_key? 'Arguments'
      @started_at = data['@timestamp']
      @arguments = data['Arguments']
    else
      return false

    end
    return true
  end

end