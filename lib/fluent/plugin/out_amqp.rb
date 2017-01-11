require 'json'
require 'fluent/output'


module Fluent
  ##
  # AMQPOutput to be used as a Fluent MATCHER, sending messages to a RabbitMQ
  # messaging broker
  class AMQPOutput < BufferedOutput
    Plugin.register_output("amqp", self)

    attr_accessor :connection

    #Attribute readers to support testing
    attr_reader :exch
    attr_reader :channel


    config_param :host, :string, :default => nil
    config_param :hosts, :array, :default => nil
    config_param :user, :string, :default => "guest"
    config_param :pass, :string, :default => "guest", :secret => true
    config_param :vhost, :string, :default => "/"
    config_param :port, :integer, :default => 5672
    config_param :ssl, :bool, :default => false
    config_param :verify_ssl, :bool, :default => false
    config_param :heartbeat, :integer, :default => 60
    config_param :exchange, :string, :default => ""
    config_param :exchange_type, :string, :default => "direct"
    config_param :passive, :bool, :default => false
    config_param :durable, :bool, :default => false
    config_param :auto_delete, :bool, :default => false
    config_param :key, :string, :default => nil
    config_param :persistent, :bool, :default => false
    config_param :tag_key, :bool, :default => false
    config_param :tag_header, :string, :default => nil
    config_param :time_header, :string, :default => nil
    config_param :tls, :bool, :default => false
    config_param :tls_cert, :string, :default => nil
    config_param :tls_key, :string, :default => nil
    config_param :tls_ca_certificates, :array, :default => nil
    config_param :tls_verify_peer, :bool, :default => true

    config_section :header, param_name: :headers, multi: true, required: false do
      config_param :name, :string
      config_param :path, default: nil do |val|
        if val.start_with?('[')
          JSON.load(val)
        else
          val.split(',')
        end
      end
      config_param :default, :string, default: nil
    end

    def initialize
      super
      require "bunny"
    end

    def configure(conf)
      super
      @conf = conf
      unless @host || @hosts
        raise ConfigError, "'host' or 'hosts' must be specified."
      end
      unless @key || @tag_key
        raise ConfigError, "Either 'key' or 'tag_key' must be set."
      end
      check_tls_configuration
    end

    def start
      super
      begin
        log.info "Connecting to RabbitMQ..."
        @connection = Bunny.new(get_connection_options) unless @connection
        @connection.start
      rescue Bunny::TCPConnectionFailed => e
        log.error "Connection to #{@host} failed"
      rescue Bunny::PossibleAuthenticationFailureError => e
        log.error "Could not authenticate as #{@user}"
      end

      log.info "Creating new exchange #{@exchange}"
      @channel = @connection.create_channel
      @exch = @channel.exchange(@exchange, :type => @exchange_type.intern,
                              :passive => @passive, :durable => @durable,
                              :auto_delete => @auto_delete)
    end

    def shutdown
      super
      @connection.stop
    end

    def format(tag, time, record)
      [tag, time, record].to_msgpack
    end

    def write(chunk)
      begin
        chunk.msgpack_each do |(tag, time, data)|
          begin
            string_data = data.is_a?(String) ? string_data : JSON.dump(data)
            log.debug "Sending message #{string_data}, :key => #{routing_key( tag)} :headers => #{get_message_headers(tag,time, data)}"
            @exch.publish(string_data, :key => routing_key( tag ), :persistent => @persistent, :headers => get_message_headers( tag, time, data ))
          rescue JSON::GeneratorError => e
            log.error "Failure converting data object to json string: #{e.message}"
            # Debug only - otherwise we may pollute the fluent logs with unparseable events and loop
            log.debug "JSON.dump failure converting [#{data}]"
          rescue StandardError => e
            # This protects against invalid byteranges and other errors at a per-message level
            log.error "Unexpected error during message publishing: #{e.message}"
            log.debug "Failure in publishing message [#{data}]"
          end
        end
      rescue MessagePack::MalformedFormatError => e
        # This has been observed when a server has filled the partition containing
        # the buffer files, and during replay the chunks were malformed
        log.error "Malformed msgpack in chunk - Did your server run out of space during buffering? #{e.message}"
      rescue StandardError => e
        # Just in case theres any other errors during chunk loading.
        log.error "Unexpected error during message publishing: #{e.message}"
      end
    end


    def routing_key( tag )
      if @tag_key
        tag
      else
        @key
      end
    end

    def get_message_headers( tag, time, data )
      {}.tap do |h|
        h[@tag_header] = tag if @tag_header
        h[@time_header] = Time.at(time).utc.to_s if @time_header
        @headers.each do |header|
          header_val = header.default if header.default
          if header.path
            temp_data = data
            temp_path = header.path.dup
            until temp_data.nil? or temp_path.empty?
              temp_data = temp_data[temp_path.shift]
            end
            header_val = temp_data if temp_data
          end
          h[header.name] = header_val if header_val
        end
      end
    end


    private
    def check_tls_configuration()
      if @tls
        unless @tls_key && @tls_cert
            raise ConfigError, "'tls_key' and 'tls_cert' must be all specified if tls is enabled."
        end
      end
    end

    def get_connection_options()
      hosts = @hosts ||= Array.new(1, @host)
      opts = {
        :hosts => hosts, :port => @port, :vhost => @vhost,
        :pass => @pass, :user => @user, :ssl => @ssl,
        :verify_ssl => @verify_ssl, :heartbeat => @heartbeat,
        :tls                 => @tls || nil,
        :tls_cert            => @tls_cert,
        :tls_key             => @tls_key,
        :verify_peer         => @tls_verify_peer
      }
      opts[:tls_ca_certificates] = @tls_ca_certificates if @tls_ca_certificates
      return opts
    end

  end
end
