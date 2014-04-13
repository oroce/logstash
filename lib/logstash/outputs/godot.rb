# encoding: utf-8
require "logstash/outputs/base"
require "logstash/namespace"
require "json"
require "socket"
class LogStash::Outputs::Godot < LogStash::Outputs::Base
  config_name "godot"
  milestone 1

  # The address of the Riemann server.
  config :host, :validate => :string, :default => "localhost"

  # The port to connect to on your Riemann server.
  config :port, :validate => :number, :default => 1337

  # The protocol to use
  # UDP is non-blocking
  # TCP is blocking
  #
  # Logstash's default output behaviour
  # is to never lose events
  # As such, we use tcp as default here
  config :protocol, :validate => ["tcp", "udp"], :default => "tcp"

  # The name of the sender.
  # This sets the `host` value
  # in the Riemann event
  config :sender, :validate => :string, :default => "%{host}"

  # A Hash to set Riemann event fields
  # (<http://riemann.io/concepts.html>).
  #
  # The following event fields are supported:
  # `description`, `state`, `metric`, `ttl`, `service`
  #
  # Tags found on the Logstash event will automatically be added to the
  # Riemann event.
  #
  # Any other field set here will be passed to Riemann as an event attribute.
  #
  # Example:
  #
  #     riemann {
  #         riemann_event => {
  #             "metric"  => "%{metric}"
  #             "service" => "%{service}"
  #         }
  #     }
  #
  # `metric` and `ttl` values will be coerced to a floating point value.
  # Values which cannot be coerced will zero (0.0).
  #
  # `description`, by default, will be set to the event message
  # but can be overridden here.
  config :godot_event, :validate => :hash

  # If set to true automatically map all logstash defined fields to riemann event fields.
  # All nested logstash fields will be mapped to riemann fields containing all parent keys
  # separated by dots and the deepest value.
  #
  # As an example, the logstash event:
  #    {
  #      "@timestamp":"2013-12-10T14:36:26.151+0000",
  #      "@version": 1,
  #      "message":"log message",
  #      "host": "host.domain.com",
  #      "nested_field": {
  #                        "key": "value"
  #                      }
  #    }
  # Is mapped to this riemann event:
  #   {
  #     :time 1386686186,
  #     :host host.domain.com,
  #     :message log message,
  #     :nested_field.key value
  #   }
  #
  # It can be used in conjunction with or independent of the riemann_event option.
  # When used with the riemann_event any duplicate keys receive their value from
  # riemann_event instead of the logstash event itself.
  config :map_fields, :validate => :boolean, :default => false

  #
  # Enable debugging output?
  config :debug, :validate => :boolean, :default => false

  # Interval between reconnect attempts to godot (when protocol is tcp).
  config :reconnect_interval, :validate => :number, :default => 2

  # Should metrics be resent on failure?
  config :resend_on_failure, :validate => :boolean, :default => false

  public
  def register
    connect
    @logger.warn( "socket is initialized with", :protocol => @protocol )
  end # def register

  public
  def connect
    if @protocol == "udp"
      connect_udp
    else
      connect_tcp
    end
  end # def connect

  public
  def connect_udp
    @socket = UDPSocket.new
  end #def connect_udp

  public
  def connect_tcp
    begin
      @socket = TCPSocket.new @host, @port
    rescue Errno::ECONNREFUSED => e
      @logger.warn("Connection refused to godot server, sleeping...",
                   :host => @host, :port => @port)
      sleep(@reconnect_interval)
      retry
    end
  end #def connect_tcp

  public
  def map_fields(parent, fields)
    fields.each {|key, val|
      if !key.start_with?("@")
        field = parent.nil? ? key : parent + '.' + key
        contents = val                            
        if contents.is_a?(Hash)                                     
          map_fields(field, contents)                                       
        else                                                                                  
          @my_event[field.to_sym] = contents                                                          
        end
      end
    }                 
  end

  public
  def receive(event)
    @logger.warn( "got the event", :event => event )
    return unless output?(event)

    # Let's build us an event, shall we?
    payload = Hash.new
    if event["@source_host"]
      payload[:host] = event["@source_host"]
    else
      payload[:host] = event.sprintf(@sender)
    end
    payload[:service] = event["service"] or event["@source"]
    # javascript epoch is ms
    payload[:time] = event["@timestamp"].to_i*1000
    payload[:description] = event["message"]
    payload[:meta] = event.to_hash
    payload[:metric] = event["value"]
    if @godot_event
      @godot_event.each do |key, val|
        if ["ttl","metric"].include?(key)
          payload[key.to_sym] = event.sprintf(val).to_f
        else
          payload[key.to_sym] = event.sprintf(val)
        end
      end
    end
    if @map_fields == true
      @my_event = Hash.new
      map_fields(nil, event)
      payload.merge!(@my_event) {|key, val1, val2| val1}
    end
    payload[:tags] = event["tags"] if event["tags"].is_a?(Array)
    @logger.debug("Godot event: ", :godot_event => payload)
    send(payload.to_json)
  end # def receive

  public
  def send(data)
    if @protocol == "udp"
      send_udp(data)
    else
      send_tcp(data)
    end
  end # def send

  public
  def send_udp(data)
    begin
      @socket.send(data, 0, @host, @port);
    rescue Exception => e
      @logger.warn("Unhandled exception in udp",
                  :exception => e, :host => @host, :port => @port)
      sleep(@reconnect_interval)
      retry if @resend_on_failure
    end
  end #def send_udp

  public
  def send_tcp(data)
    begin
      @socket.puts(data)
    rescue Errno::EPIPE, Errno::ECONNRESET => e
      @logger.warn("Unhandled exception in tcp",
                   :exception => e, :host => @host, :port => @port)
      sleep(@reconnect_interval)
      connect
      retry if @resend_on_failure
    end
  end # def send_tcp

end # class LogStash::Outputs::Godot