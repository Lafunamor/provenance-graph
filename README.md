# Provenance Graph - a Logstash Plugin

This is a plugin for [Logstash](https://github.com/elastic/logstash).

It is fully free and fully open source. The license is Apache 2.0, meaning you are pretty much free to use it however you want in whatever way.

This plugin is designed to process Apache Haddops log files. Logs should be shipped with Filebeat and parsed using the Grok plugin.
The appropriate matching patterns for Grok are in 'GrokLines.txt'.

To import huge amounts of data, the integrated import mode can be used. In import mode, Provenance Graph will generate a set of CSV files in the output path.
The queries to import the dataset from the CSV files into Neo4j are located in the subfolder './neo_import'.

## Developing

### 1. Plugin Developement and Testing

#### Code
- To get started, you'll need JRuby with the Bundler gem installed.

- Create a new plugin or clone and existing from the GitHub [logstash-plugins](https://github.com/logstash-plugins) organization. We also provide [example plugins](https://github.com/logstash-plugins?query=example).

- Install dependencies
```sh
bundle install
```

#### Test

- Update your dependencies

```sh
bundle install
```

- Run tests

```sh
bundle exec rspec
```

### 2. Running your unpublished Plugin in Logstash

#### 2.1 Run in a local Logstash clone

- Edit Logstash `Gemfile` and add the local plugin path, for example:
```ruby
gem "logstash-output-provenance-graph", :path => "/your/local/path/to/provenance-graph"
```
- Install plugin
```sh
bin/plugin install --no-verify
```
- Run Logstash with your plugin

At this point any modifications to the plugin code will be applied to this local Logstash setup. After modifying the plugin, simply rerun Logstash.

#### 2.2 Run in an installed Logstash

You can use the same **2.1** method to run your plugin in an installed Logstash by editing its `Gemfile` and pointing the `:path` to your local plugin development directory or you can build the gem and install it using:

- Build your plugin gem
```sh
gem build logstash-output-provenance-graph.gemspec
```
- Install the plugin from the Logstash home
```sh
bin/plugin install /your/local/plugin/logstash-output-provenance-graph.gem
```
- Start Logstash and proceed to test the plugin

## Configuration
To use this output plugin you need to change the configuration of logstash.
Here is an example configuration:
```
output{
   provenancegraph {
       path => "/path/to/provenance-graph/output/"
       import_mode => true # or false
       # neo4j configuration not needed in import mode
       neo4j_server => "http://127.0.0.1:7474"
       neo4j_username => "neo4j"
       neo4j_password => "password"
   }
}
```



## Contributing

All contributions are welcome: ideas, patches, documentation, bug reports, complaints, and even something you drew up on a napkin.

For more information about contributing, see the [CONTRIBUTING](https://github.com/elastic/logstash/blob/master/CONTRIBUTING.md) file.