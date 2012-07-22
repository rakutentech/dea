#
# Copyright (C) 2012 Waldemar Quevedo <waldemar.quevedo@mail.rakuten.com>
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
# CloudfoundryAppForwardOutput plugin
#
# Special plugin that "patches" the emit with info from the Cloudfoundry app,
# before it is converted into a msgpack stream and forwarded to the server
#
#
module Fluent
  class CloudfoundryAppForwardOutput < ForwardOutput
    Plugin.register_output('cf_app_forward', self)

    # Append the configuration info to the forward plugin
    config_param :cf_app_name, :string, :default => 'unknown'
    config_param :cf_app_instance_index, :integer
    config_param :cf_app_instance_id, :string
    config_param :cf_app_user, :string

    #  Append the info from the application to the emit when the conversion to msgpack occurs.
    #  override BufferedOutput#emit for this
    def emit(tag, es, chain)

      data = ''
      es.each do |emit_time, record|
        record.merge!({ 'user' => @cf_app_user,
                        'name' => @cf_app_name,
                        'instance' => {
                          'index' => @cf_app_instance_index,
                          'id' => @cf_app_instance_id
                        }
                    })
        [emit_time, record].to_msgpack(data)
      end

      # Continue forwarding as the original out_forward plugin
      if @buffer.emit(tag, data, chain)  # use key = tag
        submit_flush
      end
    end
  end
end
