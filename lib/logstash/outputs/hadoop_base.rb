class HadoopBase


  def get_create_username(username)
    # n = Neo4j::Session.current.query.match("(n:user)").where("n.username = \"#{username}\"").pluck(:n)
    # # n = Neo4j::Node.find_nodes(:user, username:username, Neo4j::Session.current.current)
    # if n.length < 1
    #   user = Neo4j::Node.create({username: username}, :user)
    # else
    #   user = n.first
    # end
    # return user
    Neo4j::Session.current.query("MERGE (j:job {id: '#{username}'}) return j").first.j
  end

  def get_create_host(host)
    # n = Neo4j::Session.current.query.match("(n:host)").where("n.name = \"#{host}\"").pluck(:n)
    # if n.length < 1
    #   h = Neo4j::Node.create({name: host}, :host)
    # else
    #   h = n.first
    # end
    # return h
    Neo4j::Session.current.query("MERGE (j:host {id: '#{host}'}) return j").first.j
  end

  def get_create_queue(queue)
    # n = Neo4j::Session.current.query.match("(n:queue)").where("n.name = \"#{queue}\"").pluck(:n)
    # if n.length < 1
    #   q = Neo4j::Node.create({name: queue}, :queue)
    # else
    #   q = n.first
    # end
    Neo4j::Session.current.query("MERGE (j:queue {id: '#{queue}'}) return j").first.j
  end

  def get_create_job(id)
    # # create node if it doesn't already exists
    # n = Neo4j::Session.current.query.match("(n:job)").where("n.id = \"#{id}\"").pluck(:n)
    # if n.length < 1
    #   Neo4j::Node.create({id: id}, :job)
    # else
    #   n.first
    # end
    Neo4j::Session.current.query("MERGE (j:job {id: '#{id}'}) return j").first.j
  end

  def get_create_application(id)
    # n = Neo4j::Session.current.query.match("(n:application)").where("n.id = \"#{id}\"").pluck(:n)
    # if n.length < 1
    #   Neo4j::Node.create({id: id, events: [], states: []}, :application)
    # else
    #   n.first
    # end
    Neo4j::Session.current.query("MERGE (j:application {id: '#{id}'}) return j").first.j
  end

  def get_create_attempt(id)
    # n = Neo4j::Session.current.query.match("(n:attempt)").where("n.id = \"#{id}\"").pluck(:n)
    # if n.length < 1
    #   Neo4j::Node.create({id: id, events: [], states: []}, :attempt)
    # else
    #   n.first
    # end
    Neo4j::Session.current.query("MERGE (j:attempt {id: '#{id}'}) return j").first.j
  end

  def get_create_container(id)
    # n = Neo4j::Session.current.query.match("(n:container)").where("n.id = \"#{container_id}\"").pluck(:n)
    # if n.length < 1
    #   c = Neo4j::Node.create({id: container_id, states: [], events: [], state_changes: [], resource_usage: []}, :container)
    # else
    #   c = n.first
    # end
    Neo4j::Session.current.query("MERGE (j:container {id: '#{id}'}) return j").first.j
  end

  def get_create_block(id)
    # n = Neo4j::Session.current.query.match("(n:block)").where("n.id = \"#{id}\"").pluck(:n)
    # if n.length < 1
    #   return Neo4j::Node.create({id: id, states: []}, :block)
    # else
    #   return n.first
    # end
    Neo4j::Session.current.query("MERGE (j:block {id: '#{id}'}) return j").first.j
  end


  def get_create_file(file_name)
    # n = Neo4j::Session.current.query.match("(n:file)").where("n.name = \"#{file_name}\"").pluck(:n)
    # if n.length < 1
    #   f = Neo4j::Node.create({name: file_name}, :file)
    # else
    #   f = n.first
    # end
    Neo4j::Session.current.query("MERGE (j:file {id: '#{file_name}'}) return j").first.j
  end

  # removes special characters from input string
  def s(string)
    string.gsub!(/[^0-9A-Za-z]/, '')
  end


  def states_to_csv
    string = ''
    unless @states.empty?
      @states.each { |k, v|
        string += "#{@id},#{k},#{v[0]},#{v[1]}\n"
      }
    end
    string
  end

  def events_to_csv
    string = ''
    unless @events.empty?
      @events.each { |k, v|
        string += "#{@id},#{k},#{v}\n"
      }
    end
    string
  end

  def states_to_query
    q =[]
    @states.each { |timestamp, state|
      string = match_query
      string += " MERGE (prev_state:state {name: '#{state[0]}'}) MERGE (a)-[:from {timestamp: '#{timestamp}'}]->(prev_state);"
      q += [string]

      string = match_query
      string +="MERGE (new_state:state {name: '#{state[1]}'}) MERGE (a)-[:to {timestamp: '#{timestamp}'}]->(new_state);"
      q += [string]
    }
    q
  end

  def events_to_query
    q =[]
    @events.each { |timestamp, event|
      string = match_query
      string += " MERGE (event:event {name: '#{event}'})
      MERGE (a)-[:has {timestamp: '#{timestamp}'}]->(event);"
      q += [string]
    }
    q
  end

  def match_query
    return ''
  end

end