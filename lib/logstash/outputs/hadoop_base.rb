class HadoopBase


  def get_create_username(username)
    n = Neo4j::Session.current.query.match("(n:user)").where("n.username = \"#{username}\"").pluck(:n)
    # n = Neo4j::Node.find_nodes(:user, username:username, Neo4j::Session.current.current)
    if n.length < 1
      user = Neo4j::Node.create({username: username}, :user)
    else
      user = n.first
    end
    return user
  end

  def get_create_host(host)
    n = Neo4j::Session.current.query.match("(n:host)").where("n.name = \"#{host}\"").pluck(:n)
    if n.length < 1
      h = Neo4j::Node.create({name: host}, :host)
    else
      h = n.first
    end
    return h
  end

  def get_create_queue(queue)
    n = Neo4j::Session.current.query.match("(n:queue)").where("n.name = \"#{queue}\"").pluck(:n)
    if n.length < 1
      q = Neo4j::Node.create({name: queue}, :queue)
    else
      q = n.first
    end
  end

  def get_create_job(id)
    # create node if it doesn't already exists
    n = Neo4j::Session.current.query.match("(n:job)").where("n.id = \"#{id}\"").pluck(:n)
    if n.length < 1
      Neo4j::Node.create({id: id}, :job)
    else
      n.first
    end
  end

  def get_create_application(id)
    n = Neo4j::Session.current.query.match("(n:application)").where("n.id = \"#{id}\"").pluck(:n)
    if n.length < 1
      Neo4j::Node.create({id: id, events: [], states: []}, :application)
    else
      n.first
    end
  end

  def get_create_attempt(id)
    n = Neo4j::Session.current.query.match("(n:attempt)").where("n.id = \"#{id}\"").pluck(:n)
    if n.length < 1
      Neo4j::Node.create({id: id, events: [], states: []}, :attempt)
    else
      n.first
    end

  end

  def get_create_container(container_id)
    n = Neo4j::Session.current.query.match("(n:container)").where("n.id = \"#{container_id}\"").pluck(:n)
    if n.length < 1
      c = Neo4j::Node.create({id: container_id, states: [], events: [], state_changes: [], resource_usage: []}, :container)
    else
      c = n.first
    end
  end

  def get_create_block(id)
    n = Neo4j::Session.current.query.match("(n:block)").where("n.id = \"#{id}\"").pluck(:n)
    if n.length < 1
      return Neo4j::Node.create({id: id, states: []}, :block)
    else
      return n.first
    end
  end


  def get_create_file(file_name)
    n = Neo4j::Session.current.query.match("(n:file)").where("n.name = \"#{file_name}\"").pluck(:n)
    if n.length < 1
      f = Neo4j::Node.create({name: file_name}, :file)
    else
      f = n.first
    end
  end



end