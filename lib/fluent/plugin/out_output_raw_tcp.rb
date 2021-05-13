#
# Copyright 2021 Yudai Hashimoto(jp7fkf)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

require "fluent/plugin/output"
require "socket"

module Fluent
  module Plugin
    class OutputRawTcpOutput < Fluent::Plugin::Output
      Fluent::Plugin.register_output("output_raw_tcp", self)

      config_param :host, :string, :default => nil
      config_param :port, :integer, :default => 514
      config_param :send_timeout, :time, :default => 60
      config_param :connect_timeout, :time, :default => 5

      config_section :buffer do
        config_set_default :flush_mode, :interval
        config_set_default :flush_interval, 5
        config_set_default :flush_thread_interval, 0.5
        config_set_default :flush_thread_burst_interval, 0.5
      end

      def configure(conf)
        super
        if @host.nil?
          raise ConfigError, "host is required"
        end
      end

      def initialize()
        super
      end

      def start()
        super
        prefer_buffered_processing()
        prefer_delayed_commit()
      end

      def shutdown()
        super
      end

      #### Non-Buffered Output #############################
      def process(tag, es)
        TCPSocket.open(@host, @port, connect_timeout=@connect_timeout) {|socket|
          opt = [1, @send_timeout.to_i].pack('I!I!')  # { int l_onoff; int l_linger; }
          sock.setsockopt(Socket::SOL_SOCKET, Socket::SO_LINGER, opt)
          opt = [@send_timeout.to_i, 0].pack('L!L!')  # struct timeval
          sock.setsockopt(Socket::SOL_SOCKET, Socket::SO_SNDTIMEO, opt)

          es.each do |time, record|
            socket.send(record << "\n", 0)
          end
        }
      end

      #### Sync Buffered Output ##############################
      def write(chunk)
        return if chunk.empty?

        TCPSocket.open(@host, @port, connect_timeout=@connect_timeout) {|socket|
          opt = [1, @send_timeout.to_i].pack('I!I!')  # { int l_onoff; int l_linger; }
          socket.setsockopt(Socket::SOL_SOCKET, Socket::SO_LINGER, opt)
          opt = [@send_timeout.to_i, 0].pack('L!L!')  # struct timeval
          socket.setsockopt(Socket::SOL_SOCKET, Socket::SO_SNDTIMEO, opt)

          chunk.each do |time, record|
            socket.send(record << "\n", 0)
          end
        }
      end

      private
      def prefer_buffered_processing()
        true
      end

      def prefer_delayed_commit()
        true
      end

    end
  end
end
