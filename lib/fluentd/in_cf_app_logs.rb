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
# CloudfoundryAppLogsInput plugin
#
# Fluent plugin for Cloudfoundry to monitor application logs from an app
#
# Example configuration:
#
# <source>
#   type cf_app_logs
#   name           sinatra-app
#   instance_id    efd394c6506739fe72e62798ea1ee774
#   instance_index 0
#   instance_dir   /var/vcap.local/dea/apps/sinatra-app-0-efd394c6506739fe72e62798ea1ee774
#   runtime        ruby19
#   framework      sinatra
#   tag            logs.storage
# </source>
#
module Fluent
  class CloudfoundryAppLogsInput < Input
    Plugin.register_input('cf_app_logs', self)

    config_param :name,           :string
    config_param :instance_id,    :string
    config_param :instance_index, :string
    config_param :instance_dir,   :string
    config_param :runtime,        :string
    config_param :framework,      :string
    config_param :tag,            :string,   :default => 'logs.storage'

    def initialize
      @monitors = Hash.new
    end

    def configure(conf)
      # Monitor files every second by default
      @monitoring_interval = conf['monitoring_interval'] || 1
      super
      @instance = {
        'name'      => @name,
        'id'        => @instance_id,
        'index'     => @instance_index,
        'dir'       => @instance_dir,
        'runtime'   => @runtime,
        'framework' => @framework
      }
    end

    def start
      @reactor = Coolio::Loop.new
      @timer = Coolio::TimerWatcher.new(@monitoring_interval, true)
      @timer.attach(@reactor)
      @timer.on_timer(&method(:monitoring_task))
      @mon_thread = Thread.new(&method(:run))
    end

    def run
      @reactor.run
    rescue
      $log.error "unexpected error in run", :error=> $!.to_s
      $log.error_backtrace
    end

    def shutdown
      @reactor.stop
      @mon_thread.join
      @monitors.each do |_, monitor|
        monitor.stop
      end
    end

    def monitoring_task
      instance_log_paths = get_log_paths_for(@instance)
      instance_log_paths.each do |path|
        if File.exists?(path) and @monitors[path].nil?
          monitor = FileMonitor.create(@instance, path, @tag)
          monitor.start
          @monitors[path] = monitor
        end
      end
    end

    def stop_monitors_from_instance
      @monitors.each do |_, monitor|
        monitor.stop
      end
    end

    def get_log_paths_for(instance)
      logs = []

      # Special cases for java and php applications
      case instance['runtime']
      when 'java', 'java7'
        tomcat_logs_dir = File.join(instance['dir'], 'tomcat', 'logs', '*')

        # Get the most recent logs from Java application in tomcat directory
        # since they are rotated daily
        all_java_server_logs = Dir[tomcat_logs_dir].entries.sort_by {|f| File.mtime(f)}.reverse

        logs << all_java_server_logs.find { |log| File.basename(log) =~ /catalina/ }
        logs << all_java_server_logs.find { |log| File.basename(log) =~ /manager/  }
        logs << all_java_server_logs.find { |log| File.basename(log) =~ /host-manager/ }
        logs << all_java_server_logs.find { |log| File.basename(log) =~ /localhost/ }
      when 'php'
        logs << File.join(instance['dir'], 'logs', 'error.log')
      end

      logs << File.join(instance['dir'], 'logs', 'stderr.log')
      logs << File.join(instance['dir'], 'logs', 'stdout.log')

      logs
    end

    class FileMonitor
      attr_reader :path

      def initialize(path, stat, tag)
        @path    = path
        @stat    = stat
        @tag     = tag
      end

      def start
        @loop = Coolio::Loop.new
        @loop.attach Handler.new(@path, @stat, { }, method(:receive_lines))
        @thread = Thread.new(&method(:run))

        $log.info "Monitor started: #{@path}"
      end

      def run
        @loop.run
      rescue
        $log.error "unexpected error", :error => $!.to_s
        $log.error_backtrace
      end

      def stop
        @loop.stop
        @thread.join
        begin
          open(@stat['statPath'], 'w') do |f|
            f.write(@stat.to_json)
          end
        rescue Errno::ENOENT
          # Ignore if file is deleted.
        end
        $log.info "Monitor stopped: #{@path}"
      end

      def receive_lines(lines)
        emits = []
        lines.each {|line|
          begin
            line.rstrip!  # remove \n
            if line
              time = Time.now.to_f
              record = {
                'type'     => @stat['type'],
                'name'     => @stat['name'],
                'instance' => {
                  'index'  => @stat['index'],
                  'id'     => @stat['id']
                },
                'text'      => line,
                'timestamp' => time,
                'file' => @stat['file']
              }

              emits << [time, record]
            end
          rescue
            $log.warn line.dump, :error => $!.to_s
            $log.debug_backtrace
          end
        }
        unless emits.empty?
          $log.debug "#{emits.length} records emitted with #{@tag}"
          Engine.emit_array(@tag, ArrayEventStream.new(emits))
        end
      end

      def to_s
        @path
      end

      # Class methods for FileMonitor
      class << self
        def create(instance, path, tag)
          tmpdir = File.join(instance['dir'], 'fluentd')
          stat_path = FileMonitor.stat_path_for(instance, path, tmpdir)

          stat = if File.exists?(stat_path)
                   JSON.parse(File.read(stat_path))
                 else
                   { 'path' => path, 'statPath' => stat_path, 'position' => 0 }
                 end
          stat.merge!({
                        "type"  => 'serverLog',
                        "name"  => instance['name'],
                        "index" => instance['index'],
                        "id"    => instance['id'],
                        "file"  => File.basename(path, '.log').split('.').first
                      })

          monitor = FileMonitor.new(path, stat, tag)

          monitor
        rescue
          $log.warn 'Failed to create FileMonitor', :error => $!.to_s
          $log.error_backtrace
        end

        def stat_path_for(instance, path, tmpdir)
          stat_identifier = [instance['name'], instance['index'], instance['id']].join('-')
          stat_path = [tmpdir, '/', stat_identifier, '-', File.basename(path, '.log')].join('')
        end
      end
      #
      # Watch the file changes
      #
      class Handler < Coolio::StatWatcher
        def initialize(path, stat, options, callback)
          @stat = stat
          @buffer = ''
          @callback = callback
          super(path)
        end

        def on_change(*args)
          lines = []
          File.open(path) do |f|
            if f.lstat.size < @stat['position']
              # moved or deleted
              @stat['position'] = 0
            else
              f.seek(@stat['position'])
            end

            line = f.gets
            unless line
              return
            end

            @buffer << line
            unless line[line.length-1] == ?\n
              @stat['position'] = f.pos
              return
            end

            lines << @buffer
            @buffer = ''

            while line = f.gets
              unless line[line.length-1] == ?\n
                @buffer = line
                break
              end
              lines << line
            end
            @stat['position'] = f.pos
          end
          @callback.call(lines)
          open(@stat['statPath'], 'w') do |f|
            f.write(@stat.to_json)
          end
        rescue Errno::ENOENT
          # moved or deleted
          @stat['position'] = 0
        end
      end
    end
  end

end
