# encoding: utf-8
require 'logstash/outputs/base'
require 'logstash/namespace'
require_relative 'hadoop_job'
require_relative 'hadoop_application'
require_relative 'hadoop_container'
require_relative 'hadoop_app_attempt'
require_relative 'hadoop_hdfs_block'
require 'rubygems'
require 'concurrent'
require 'thread_safe'
require 'thread'
require 'neo4j-core'
require_relative 'synchronized_counter'


# An example output that does nothing.
class LogStash::Outputs::ProvenanceGraph < LogStash::Outputs::Base

  config_name 'provenancegraph'

  # @Jobs
  # config :fields, :validate => :array, :required => true
  #:fields
  config :path, :validate => :string, :required => true
  config :flush_interval, :validate => :number, :required => false
  config :import_mode, :required => false

  # configuration for the neo4j graph db
  config :neo4j_server, :validate => :string, :required => true
  config :neo4j_username, :validate => :string, :required => true
  config :neo4j_password, :validate => :string, :required => true

  # defaults
  default :flush_interval, '300'
  default :codec, 'json_lines'
  default :import_mode, false

  # declare_threadsafe!

  public
  def register
    @logger.debug('starting', :plugin => self)
    @count = 0
    @logger.debug('loading data', :plugin => self)
    if @import_mode
      @available_threads = SynchronizedCounter.new 1
    else
      @available_threads = SynchronizedCounter.new 100
    end

    # deserialize
    # if @jobs.nil? || @jobs == false
    @jobs = ThreadSafe::Hash.new
    # end
    # if @applications.nil? || @applications == false
    @applications = ThreadSafe::Hash.new
    # end
    # if @app_attempts.nil? || @app_attempts == false
    @app_attempts = ThreadSafe::Hash.new
    # end
    # if @containers.nil? || @containers == false
    @containers = ThreadSafe::Hash.new
    # end
    # if @blocks.nil? || @blocks == false
    @blocks = ThreadSafe::Hash.new
    # end
    @write_thread = nil
    @last_flush = Time.now
    @eps = 0
    @last_count = 0

    unless @import_mode
      # Using Neo4j Server Cypher Database
      begin
        @session = Neo4j::Session.open(:server_db, @neo4j_server, basic_auth: {username: @neo4j_username, password: @neo4j_password})

           # @session = Neo4j::Session.open(:embedded_db, '/var/lib/neo4j/data', auto_commit: true)
           @session.start
      rescue
        @logger.error('Error: Could not connect to neo4j DB', :plugin => self)
        exit
      end
      begin
        # embeded
        # # /var/lib/neo4j/data/
        # @session = Neo4j::Session.open(:ha_db, '/var/lib/neo4j/data/', auto_commit: true)
        # @session.start
        @session.query('Create CONSTRAINT ON (n:job) ASSERT n.id IS UNIQUE')
        @session.query('Create CONSTRAINT ON (n:application) ASSERT n.id IS UNIQUE')
        @session.query('Create CONSTRAINT ON (n:attempt) ASSERT n.id IS UNIQUE')
        @session.query('Create CONSTRAINT ON (n:container) ASSERT n.id IS UNIQUE')
        @session.query('Create CONSTRAINT ON (n:block) ASSERT n.id IS UNIQUE')
        @session.query('Create CONSTRAINT ON (n:file) ASSERT n.name IS UNIQUE')
        @session.query('Create CONSTRAINT ON (n:queue) ASSERT n.name IS UNIQUE')
        @session.query('Create CONSTRAINT ON (n:host) ASSERT n.name IS UNIQUE')
        @session.query('Create CONSTRAINT ON (n:user) ASSERT n.username IS UNIQUE')
        @session.query('Create CONSTRAINT ON (n:state) ASSERT n.name IS UNIQUE')
        @session.query('Create CONSTRAINT ON (n:event) ASSERT n.name IS UNIQUE')

      rescue
        @logger.error('Error: DB is locked', :plugin => self)
        exit
      end
    end
    @logger.debug('finished initialisation', :plugin => self)
  end


  public
  def receive(event)
    @count +=1
    data = event.to_hash
    type = nil

    # open(path + 'events.txt', 'a') { |f|
    #   f.puts data
    # }
    data['timestamp'] = data['timestamp'].gsub(',', '.')


    if data.has_key? ('ContainerID')
      ids = data['ContainerID'].split('_')
      job_id = ids[1]+'_'+ ids[2]
      app_id = ids[1]+'_'+ ids[2]
      app_attempt_id = ids[1] +'_'+ ids[2] +'_0000'+ ids[3] #containerIDs do not use the same length for the app attempt id
      container_id = ids[1] +'_'+ ids[2] +'_'+ ids[3] +'_'+ ids[4]
      type = 'container'
    elsif data.has_key? ('AppAttemptID')
      ids = data['AppAttemptID'].split('_')
      job_id = ids[1]+'_'+ ids[2]
      app_id = ids[1]+'_'+ ids[2]
      app_attempt_id = ids[1] +'_'+ ids[2] +'_'+ ids[3]
      type = 'attempt'
    elsif data.has_key? ('DFSClientID')
      ids = data['DFSClientID'].split('_')
      job_id = ids[2]+'_'+ ids[3]
      app_id = ids[2]+'_'+ ids[3]
      block_id = data['Block_ID']
      if ids[1]== 'attempt'
        type = 'hdfs_trace'
      end
    elsif data.has_key?('ApplicationID')
      ids = data['ApplicationID'].split('_')
      job_id = ids[1]+'_'+ ids[2]
      app_id = ids[1]+'_'+ ids[2]
      type = 'app'
    elsif data.has_key? ('JobID')
      ids = data['JobID'].split('_')
      job_id = job_id = ids[1]+'_'+ ids[2]
      type = 'job'
    elsif data.has_key?('Block_ID')
      block_id = data["Block_ID"]
      type = 'hdfs'
    end
    if type.nil?
      return
    end


    case type
      when 'job'
        # noinspection RubyScope
        job = get_create_job job_id
        unless job.parse_data data
          unhandled data
        end
      when 'app'
        # noinspection RubyScope
        app = get_create_app app_id, job_id
        unless app.parse_data data
          unhandled data
        end
      when 'hdfs_trace'
        app = get_create_app app_id, job_id
        unless app.add_hdfs_trace data, @import_mode
          unhandled data
        end
      when 'attempt'
        # noinspection RubyScope
        app_attempt = get_create_attempt app_attempt_id, app_id, job_id
        unless app_attempt.parse_data data
          unhandled data
        end
      when 'container'
        # noinspection RubyScope
        container = get_create_container container_id, app_attempt_id, app_id, job_id
        unless container.parse_data data
          unhandled data
        end
      when 'hdfs'
        unless data.has_key?('srvID')
          block = get_create_block block_id
          unless block.parse_data data
            unhandled data
          end
        end
      else
        unhandled data
        return
    end

    time_difference = Time.now - @last_flush
    @logger.debug('time difference: ' + time_difference.to_s, :plugin => self)
    # if time_difference >= 30
    if @count % 100 == 0 #|| time_difference >= @flush_interval
      if @import_mode
        @eps = (@eps + (@count-@last_count)/(Time.now - @last_flush))/2
        @last_count = @count
        @last_flush = Time.now
        open(path + 'count.txt', 'w') { |f|
          f.puts @count
          f.puts @eps
        }
        start = Time.now
        to_csv(@jobs, @applications, @app_attempts, @containers, @blocks)
        remove_old_data
        end_time = Time.now
        open(path + 'count_return.txt', 'w') { |f|
          f.puts 'written ' + end_time.to_s
          f.puts 'elapsed time: ' + (end_time - start).to_s
          f.puts @count
        }
      else
        # real-time mode
        if @available_threads.count > 0
          @available_threads.decrement!
          # if @write_thread.nil? || @write_thread.stop?
          @eps = (@eps + (@count-@last_count)/(Time.now - @last_flush))/2
          @last_count = @count
          @last_flush = Time.now
          count = @count
          @write_thread = Thread.new(count, @eps, @jobs.clone, @applications.clone, @app_attempts.clone, @containers.clone, @blocks.clone,
                                     @available_threads, @import_mode) {
              |count, eps, jobs, apps, app_attempts, containers, blocks, available_threads, import_mode|
            open(path + 'count.txt', 'w') { |f|
              f.puts count
              f.puts eps
              f.puts available_threads.count
            }
            # serialize(jobs, apps, app_attempts, containers, blocks)
            start = Time.now
            flush_to_db(jobs, apps, app_attempts, containers, blocks)
            # remove_old_data(jobs, blocks)
            end_time = Time.now
            open(path + 'count_return.txt', 'w') { |f|
              f.puts 'written ' + end_time.to_s
              f.puts 'elapsed time: ' + (end_time - start).to_s
              f.puts count
              f.puts available_threads.count
            }
            available_threads.increment!
          }
          remove_old_data
        end
      end
      # unhandled(data)
    end

  end

  def deserialize
    if File.exists?(path + '.save_job.yaml')
      @jobs = YAML.load(File.read(path + '.save_job.yaml'))
    end
    if File.exists?(path + '.save_apps.yaml')
      @applications = YAML.load(File.read(path + '.save_apps.yaml'))
    end
    if File.exists?(path + '.save_appattempt.yaml')
      @app_attempts = YAML.load(File.read(path + '.save_appattempt.yaml'))
    end
    if File.exists?(path + '.save_containers.yaml')
      @containers = YAML.load(File.read(path + '.save_containers.yaml'))
    end
    if File.exists?(path + '.save_blocks.yaml')
      @blocks = YAML.load(File.read(path + '.save_blocks.yaml'))
    end
  end

  def serialize(jobs, apps, app_attempts, containers, blocks)
    File.open(path + 'save_job.yaml', 'w') { |f|
      f.write(YAML.dump(jobs))
    }
    File.open(path + 'save_apps.yaml', 'w') { |f|
      f.write(YAML.dump(apps))
    }
    File.open(path + 'save_appattempt.yaml', 'w') { |f|
      f.write(YAML.dump(app_attempts))
    }
    File.open(path + 'save_containers.yaml', 'w') { |f|
      f.write(YAML.dump(containers))
    }
    File.open(path + 'save_blocks.yaml', 'w') { |f|
      f.write(YAML.dump(blocks))
    }
  end

  def flush_to_db(jobs, apps, app_attempts, containers, blocks)

    blocks.each { |k, v|
      v.to_db.each { |query|
        Neo4j::Session.current.query(query)
      }
    }
    # Neo4j::Session.current.query(query)
    # query = ""
    containers.each { |k, v|
      v.to_db.each { |query|
        Neo4j::Session.current.query(query)
      }
    }
    # Neo4j::Session.current.query(query)
    # query = ""
    app_attempts.each { |k, v|
      v.to_db.each { |query|
        Neo4j::Session.current.query(query)
      }
    }
    # Neo4j::Session.current.query(query)
    # query = ""
    apps.each { |k, v|
      v.to_db.each { |query|
        Neo4j::Session.current.query(query)
      }
    }
    # Neo4j::Session.current.query(query)
    # query = ""
    jobs.each { |k, v|
      v.to_db.each { |query|
        Neo4j::Session.current.query(query)
      }
    }
    # Neo4j::Session.current.query(query)
  end

  def to_csv(jobs, apps, app_attempts, containers, blocks)
    jobs.each { |k, v|
      v.to_csv(path)
    }
    apps.each { |k, v|
      v.to_csv(path)
    }
    app_attempts.each { |k, v|
      v.to_csv(path)
    }
    containers.each { |k, v|
      v.to_csv(path)
    }
    blocks.each { |k, v|
      v.to_csv(path)
    }

    # jobs = apps = app_attempts = containers = blocks = []
    # jobs_copy.each { |key, value|
    #   if value.has_job_summary?
    #     jobs += value
    #   end
    # }
    # jobs.each { |job|
    #   apps += job.get_apps
    # }
    # apps.each { |app|
    #   app_attempts += app.get_attempts
    # }
    # app_attempts.each { |attempt|
    #   containers += attempt.get_containers
    # }
    # blocks_copy.each { |k, v|
    #   if v.enough_data?
    #     blocks += [v]
    #   end
    # }
    # File.open(path + 'blocks.csv', 'a') { |f|
    #   File.open(path + 'block_states.csv', 'a') { |i|
    #     File.open(path + 'block_source_hosts.csv', 'a') { |g|
    #       File.open(path + 'block_destination_hosts.csv', 'a') { |h|
    #         File.open(path + 'block_replica_states.csv', 'a') { |e|
    #           blocks.each { |k, v|
    #             unless_empty? f, v.to_csv
    #             unless_empty? g, v.source_hosts_to_csv
    #             unless_empty? h, v.dest_hosts_to_csv
    #             unless_empty? i, v.states_to_csv
    #             unless_empty? e, v.replica_states_to_csv
    #           }
    #         }
    #       }
    #     }
    #   }
    # }
    # File.open(path + 'containers.csv', 'a') { |f|
    # File.open(path + 'container_states.csv', 'a') { |i|
    #   File.open(path + 'container_resource_usage.csv', 'a') { |h|
    # File.open(path + 'container_state_transitions.csv', 'a') { |g|
    # File.open(path + 'container_events.csv', 'a') { |j|
    #           containers.each { |v|
    #             unless_empty? f, v.to_csv
    #             unless_empty? g, v.container_states_to_csv
    #             unless_empty? h, v.resource_usage_to_csv
    #             unless_empty? i, v.states_to_csv
    #             unless_empty? j, v.events_to_csv
    #           }
    #         }
    #       }
    #     }
    #   }
    # }
    # File.open(path + 'app_attempts.csv', 'a') { |f|
    #   File.open(path + 'attempts_containers.csv', 'a') { |g|
    #     File.open(path + 'app_attempt_states.csv', 'a') { |i|
    #       File.open(path + 'app_attempt_events.csv', 'a') { |j|
    #         app_attempts.each { |v|
    #           unless_empty? f, v.to_csv
    #           unless_empty? g, v.to_csv2
    #           unless_empty? i, v.states_to_csv
    #           unless_empty? j, v.events_to_csv
    #         }
    #       }
    #     }
    #   }
    # }
    # File.open(path + 'apps.csv', 'a') { |f|
    #   File.open(path + 'apps_attempts.csv', 'a') { |g|
    #     File.open(path + 'apps_blocks.csv', 'a') { |h|
    #       File.open(path + 'apps_states.csv', 'a') { |i|
    #         File.open(path + 'apps_events.csv', 'a') { |j|
    #           apps.each { |v|
    #             unless_empty? f, v.to_csv
    #             unless_empty? g, v.to_csv2
    #             unless_empty? h, v.to_csv3
    #             unless_empty? i, v.states_to_csv
    #             unless_empty? j, v.events_to_csv
    #           }
    #         }
    #       }
    #     }
    #   }
    # }
    # File.open(path + 'jobs.csv', 'a') { |f|
    #   File.open(path + 'jobs_apps.csv', 'a') { |g|
    #     jobs.each { |v|
    #       f.puts v.to_csv
    #       g.puts v.to_csv2
    #     }
    #   }
    # }
  end

  def unless_empty?(file, string)
    unless string == ''
      file.puts string
    end
  end

  def to_csv_headers(jobs, apps, app_attempts, containers, blocks)
    File.open(path + 'blocks.csv', 'a') { |f|
      f.puts blocks.first[1].csv_header
    }
    File.open(path + 'containers.csv', 'a') { |f|
      f.puts containers.first[1].csv_header
    }
    File.open(path + 'app_attempts.csv', 'a') { |f|
      File.open(path + 'attempts_containers.csv', 'a') { |g|
        f.puts app_attempts.first[1].csv_header
        g.puts 'attempt_id,container_id'
      }
    }
    File.open(path + 'apps.csv', 'a') { |f|
      File.open(path + 'apps_attempts.csv', 'a') { |g|
        File.open(path + 'apps_blocks.csv', 'a') { |h|
          f.puts apps.first[1].csv_header
          g.puts 'app_id,attempt_id'
          h.puts 'app_id,block_id'
        }
      }
    }
    File.open(path + 'jobs.csv', 'a') { |f|
      File.open(path + 'jobs_apps.csv', 'a') { |g|
        f.puts jobs.first[1].csv_header
        g.puts 'job_id,app_id'
      }
    }

  end


  def remove_old_data #(jobs_copy, blocks_copy)
    # if @import_mode
    #   jobs = apps = app_attempts = containers = blocks = []
    #   jobs_copy.each { |key, value|
    #     if value.has_job_summary?
    #       jobs += value
    #     end
    #   }
    #   jobs.each { |job|
    #     apps += job.get_apps
    #   }
    #   apps.each { |app|
    #     app_attempts += app.get_attempts
    #   }
    #   app_attempts.each { |attempt|
    #     containers += attempt.get_containers
    #   }
    #   blocks_copy.each { |k, v|
    #     if v.enough_data?
    #       blocks += [v]
    #     end
    #   }
    #   @jobs.delete_if { |k, v| jobs.include? v }
    #   @applications.delete_if { |k, v| apps.include? v }
    #   @app_attempts.delete_if { |k, v| app_attempts.include? v }
    #   @containers.delete_if { |k, v| containers.include? v }
    #
    #   # @jobs.delete_if { |key, value| value.has_job_summary? }
    #   @blocks.delete_if { |k, v| blocks.include? v }
    # else
    @jobs = ThreadSafe::Hash.new
    @applications = ThreadSafe::Hash.new
    @app_attempts = ThreadSafe::Hash.new
    @containers = ThreadSafe::Hash.new
    @blocks = ThreadSafe::Hash.new
    # end

    # @jobs.delete_if { |key, value| @last_flush - value.last_edited >= 120 }
    # @applications.delete_if { |key, value| @last_flush - value.last_edited >= 120 }
    # @app_attempts.delete_if { |key, value| @last_flush - value.last_edited >= 120 }
    # @containers.delete_if { |key, value| @last_flush - value.last_edited >= 120 }

  end


  def unhandled(data)
    open(path + 'unhandled.txt', 'a') { |f|
      f.puts data
    }
  end

  def get_create_job(job_id)
    if @jobs.has_key? job_id
      job = @jobs[job_id]
    else
      job = HadoopJob.new (job_id)
      @jobs[job_id] = job
    end
    return job
  end

  def get_create_app(app_id, job_id)
    if @applications.has_key? app_id
      app = @applications[app_id]
    else
      app = HadoopApplication.new (app_id)
      @applications[app_id] = app
      get_create_job(job_id).add_app app_id
    end
    return app
  end

  def get_create_attempt(app_attempt_id, app_id, job_id)
    if @app_attempts.has_key? app_attempt_id
      app_attempt = @app_attempts[app_attempt_id]
    else
      app_attempt = HadoopAppAttempt.new (app_attempt_id)
      @app_attempts[app_attempt_id] = app_attempt
      get_create_app(app_id, job_id).add_attempt app_attempt_id
    end
    return app_attempt
  end

  def get_create_container(container_id, app_attempt_id, app_id, job_id)
    if @containers.has_key? container_id
      container = @containers[container_id]
    else
      container = HadoopContainer.new(container_id)
      @containers[container_id] = container
      get_create_attempt(app_attempt_id, app_id, job_id).add_container container_id
    end
    return container
  end

  def get_create_block(block_id)
    if @blocks.has_key? block_id
      block = @blocks[block_id]
    else
      block = HadoopHDFSBlock.new(block_id)
      @blocks[block_id]=block
    end
    return block
  end

  # handles shutdown of pipeline
  def close
    @logger.info('clean shutdown, flushing data', :plugin => self)
    if @import_mode
      to_csv(@jobs, @applications, @app_attempts, @containers, @blocks)
      # to_csv(@jobs, @blocks)
      serialize(@jobs, @applications, @app_attempts, @containers, @blocks)
    else
      flush_to_db(@jobs, @applications, @app_attempts, @containers, @blocks)
    end
    @logger.info('writing to disk completed, shutting down', :plugin => self)
  end

end # class LogStash::Outputs::ProvenanceGraph

