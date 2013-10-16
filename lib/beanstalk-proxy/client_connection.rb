require 'beanstalk-proxy/beanstalk_protocol'

class BeanstalkProxy
  class ClientConnection < EventMachine::Connection
    include BeanstalkProtocol
    def self.start(host, port)
      $server = EM.start_server(host, port, self)
      LOGGER.info "Listening on #{host}:#{port}"
      LOGGER.info "Send QUIT to quit after waiting for all connections to finish."
      LOGGER.info "Send TERM or INT to quit after waiting for up to 10 seconds for connections to finish."
    end

    def post_init
      LOGGER.info "Accepted #{peer}"
      @buffer_in = ''
      @buffer_out = ''
      @remote = nil
      @tries = 0
      @connected = false
      @connect_timeout = nil
      @inactivity_timeout = nil
      @connection_request_state = { state: :header}
      @connection_response_state = { state: :init}
      BeanstalkProxy.incr
    end

    def peer
      @peer ||=
      begin
        port, ip = Socket.unpack_sockaddr_in(get_peername)
        "#{ip}:#{port}"
      end
    end

    def receive_data(data)
      @buffer_in << data
      loop do
        bs_cmd = process_request!(@buffer_in, @connection_request_state)
        LOGGER.debug("process_request bs_cmd=#{bs_cmd.inspect}")
        break unless bs_cmd

        proxy_commands = BeanstalkProxy.router.call(bs_cmd, self)

        LOGGER.info "#{peer} #{proxy_commands.inspect}"
        close_connection unless proxy_commands.instance_of?(Hash)

        if proxy_commands[:remote] 
          if !@connected && @remote.nil?
            @connect_timeout = proxy_commands[:connect_timeout]
            @inactivity_timeout = proxy_commands[:inactivity_timeout]
     
            establish_remote_server(proxy_commands[:remote]) 
          end
        end

        if close = proxy_commands[:close]
          LOGGER.debug "close=#{close}"
          if close == true
            close_connection
          else
            send_data(close)
            close_connection_after_writing
          end
        elsif proxy_commands[:noop]
          # do nothing
        else
          reply = proxy_commands[:reply]
          close_connection if @remote.nil? && reply.nil?
          
          send_data(reply) unless reply.nil?

          unless @remote.nil?
            @buffer_out << (proxy_commands[:data] ? proxy_commands[:data] : bs_cmd[:data])
            if @connected
              @server_side.send_data(@buffer_out)
              @buffer_out = ''
            end
          end
        end
      end
    rescue => e
      close_connection
      LOGGER.error "#{e.class} - #{e.message}"
      LOGGER.error e.backtrace.join("\n\t")
    end

    # Called when new data is available from the client but no remote
    # server has been established. If a remote can be established, an
    # attempt is made to connect.
    def establish_remote_server(remote)
      fail "establish_remote_server called with remote established" if @remote
      m, host, port = *remote.match(/^(.+):(.+)$/)
      @remote = [host, port]
      connect_to_server
    end

    # Connect to the remote server
    def connect_to_server
      fail "connect_server called without remote established" if @remote.nil?
      host, port = @remote
      LOGGER.info "Establishing new connection with #{host}:#{port}"
      @server_side = ServerConnection.request(host, port, self)
      @server_side.pending_connect_timeout = @connect_timeout
      @server_side.comm_inactivity_timeout = @inactivity_timeout
    end

    # Called by the server side immediately after the server connection was
    # successfully established. Send any buffer we've accumulated .
    def server_connection_success
      LOGGER.info "Successful connection to #{@remote.join(':')}"
      @connected = true
      @server_side.send_data(@buffer_out)
      @buffer_out = ''
    end

    # Called by the server side when a connection could not be established,
    # either due to a hard connection failure or to a connection timeout.
    # Leave the client connection open and retry the server connection up to
    # 10 times.
    def server_connection_failed
      @server_side = nil
      if @connected
        LOGGER.error "Connection with #{@remote.join(':')} was terminated prematurely."
        close_connection
        BeanstalkProxy.connect_error_callback.call(@remote.join(':'))
      elsif @tries < 10
        @tries += 1
        LOGGER.warn "Retrying connection with #{@remote.join(':')} (##{@tries})"
        EM.add_timer(0.1) { connect_to_server }
      else
        LOGGER.error "Connect #{@remote.join(':')} failed after ten attempts."
        close_connection
        BeanstalkProxy.connect_error_callback.call(@remote.join(':'))
      end
    end

    # Called by the server when an inactivity timeout is detected. The timeout
    # argument is the configured inactivity timeout in seconds as a float; the
    # elapsed argument is the amount of time that actually elapsed since
    # connecting but not receiving any data.
    def server_inactivity_timeout(timeout, elapsed)
      LOGGER.error "Disconnecting #{@remote.join(':')} after #{elapsed}s of inactivity (> #{timeout.inspect})"
      @server_side = nil
      close_connection
      BeanstalkProxy.inactivity_error_callback.call(@remote.join(':'))
    end

    def unbind
      BeanstalkProxy.client_connection_unbind_callback.call(self)
      @server_side.close_connection_after_writing if @server_side
      BeanstalkProxy.decr
    end

    # Proxy connection has been lost
    def proxy_target_unbound
      @server_side = nil
    end
  end
end
