
class HadoopStateChange

  @timestamp
  @previousState
  @newState


  def initialize(time, prevState, newState)
    @timestamp = time
    @previousState = prevState
    @newState = newState
  end


end