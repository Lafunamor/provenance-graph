class ResUsageCollector
  require 'rubygems'
  require 'neo4j-core'


  puts "start"
  @session = Neo4j::Session.open(:server_db, "http://127.0.0.1:7474", basic_auth: {username: "neo4j", password: "neo4jpassword"})
  puts "Session created"
  hosts = @session.query('MATCH (host:host) return host.name limit 5;')
  hosts.each { |h|
    puts h['host.name']
    resource_usage = @session.query("MATCH (h:host {name: '#{h['host.name']}'})<-[:hosted_on]-(:container)-[u:used]->(ru:resource_usage) return u.timestamp,ru limit 5;")

    if resource_usage.size == 0
      puts 'no entries found for host ' + h['host.name']
    end

    resource_usage.each { |ru|
      # puts ru['timestamp']
      puts ru[0]
      puts ru[1].get_property('ProcessTreeID')
      puts ru[1].get_property('UsedPysicalMemory') +' / '+ ru[1].get_property('AvailablePhysicalMemory')+' physical memory'
      puts ru[1].get_property('UsedVirtualMemory') +' / '+ ru[1].get_property('AvailableVirtualMemory')+' virtual memory'
    }
    puts 'next host'
  }
  puts "done"

  @session.close

end