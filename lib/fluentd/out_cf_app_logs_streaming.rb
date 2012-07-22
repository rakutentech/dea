#
# Copyright (C) 2012 Yohei Sasaki <yohei.sasaki@mail.rakuten.com>
#                    Waldemar Quevedo <waldemar.quevedo@mail.rakuten.com>
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
#
# CloudfoundryAppLogsStreamingOutput plugin
#
# Fluent Plugin to stream application logs from CloudFoundry
#
# Example Configuration:
#
#   <match **>
#      type cf_app_logs_streaming
#      port 22940
#   </source>
#
require 'yajl'

module Fluent
  class CloudfoundryAppLogsStreamingOutput < Output
    Plugin.register_output('cf_app_logs_streaming', self)

    config_param :port, :integer

    def initialize
      @clients = {}
    end

    def configure(conf)
      super(conf)
      unless @port
        raise ConfigError, "Missing 'port' parameter to stream the logs"
      end
      @bind = conf['bind'] || '0.0.0.0'
    end

    def start
      $log.debug "listening log-streaming socket on #{@bind}:#{@port}"
      @loop = Coolio::Loop.new
      @lsock = Coolio::TCPServer.new(@bind, @port, Handler, method(:on_message))
      @loop.attach(@lsock)
      @thread = Thread.new(&method(:run))
    end

    def shutdown
      @lsock.close
      @loop.stop
      @thread.join
    end

    def run
      @loop.run
    rescue
      $log.error "unexpected error in run", :error => $!.to_s
      $log.error_backtrace
    end

    def on_message(client, message)
      register_client(client)
    end

    def register_client(client)
      key = client.object_id
      @clients[key] = {} if not @clients[key]

      @clients[key] = client
      $log.trace { "Add streaming client(#{key})" }
    end

    def unregister_client(key)
      @clients.delete(key)
      $log.trace { "Remove streaming client(#{key})" }
    end

    def emit(tag, es, chain)

      es.each do |time, record|

        if record['name']
          time = Time.at(time).utc.iso8601(3)
          writers = @clients.dup

          if writers.nil? or writers == {}
            $log.debug "No clients registered"
            next
          end

          removes = []
          writers.each do |k, writer|
            begin
              if writer.closed?
                removes << k
              else
                writer.write "#{record.to_json}\n"
                writer.flush
              end
            rescue => e
              $log.error "Unexpected error while streaming", :error => $!.to_s
              $log.error_backtrace
              removes << k
            end
          end

          if removes
            removes.each do |key|
              unregister_client(key)
            end
          end

        else
          $log.debug "Invalid record skipped (tag is '#{tag}')"
        end
      end
    end

    class Handler < Coolio::Socket
      def initialize(io, on_message)
        super(io)
        opt = [1, @timeout.to_i].pack('I!I!')  # { int l_onoff; int l_linger; }
        io.setsockopt(Socket::SOL_SOCKET, Socket::SO_LINGER, opt)
        @on_message = on_message
        @io = io
        @parser = Yajl::Parser.new
      end

      def on_connect
        $log.trace { "open log-streaming socket object_id=#{self.object_id}" }
        @on_message.call(@io, "Hi")
      end

      def on_close
        $log.trace { "close log-streaming socket object_id=#{self.object_id}" }
      end

      def on_read(data)
        @parser.on_parse_complete = lambda do |json|
          @on_message.call(@io, json)
        end

        begin
          # Keep adding data until the parsing callback is done
          @parser << data
        rescue => e
          $log.error { "parse error: #{e} "}
        end
      end
    end
  end
end
