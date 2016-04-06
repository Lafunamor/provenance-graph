# encoding: utf-8
require "logstash/outputs/base"
require "logstash/namespace"

# An example output that does nothing.
class LogStash::Outputs::ProvenanceGraph < LogStash::Outputs::Base
  config_name "provenance-graph"


  public
  def initialize

  end

  public
  def register
  end # def register

  public
  def receive(event)
    File.write('./file', event)
    return "Event received"
  end # def event
end # class LogStash::Outputs::Example
