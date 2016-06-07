class SynchronizedCounter

  attr_reader :count

  def initialize(count)
    @count = count
    @mutex = Mutex.new
  end

  def increment!
    @mutex.synchronize { @count += 1 }
  end

  def decrement!
    @mutex.synchronize { @count -= 1 }
  end
end