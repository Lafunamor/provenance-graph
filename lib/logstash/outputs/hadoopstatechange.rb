
class HadoopStateChange

  # @timestamp
  # @previousState
  # @newState


  def initialize(time, prev_state, new_state)
    @timestamp = time
    @previous_state = prev_state
    @new_state = new_state
  end


end