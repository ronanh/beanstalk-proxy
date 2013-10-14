require 'yaml'
require 'eventmachine'
require 'logger'
require 'socket'

require 'beanstalk-proxy/client_connection'
require 'beanstalk-proxy/server_connection'

LOGGER = Logger.new(STDOUT)

class BeanstalkProxy
  VERSION = '0.1.0'

  MAX_FAST_SHUTDOWN_SECONDS = 10

  def self.update_procline
    $0 = "beanstalk-proxy #{VERSION} - #{@@name} #{@@listen} - #{self.stats} cur/max/tot conns"
  end

  def self.stats
    "#{@@counter}/#{@@maxcounter}/#{@@totalcounter}"
  end

  def self.count
    @@counter
  end

  def self.incr
    @@totalcounter += 1
    @@counter += 1
    @@maxcounter = @@counter if @@counter > @@maxcounter
    self.update_procline
    @@counter
  end

  def self.decr
    @@counter -= 1
    if $server.nil?
      LOGGER.info "Waiting for #{@@counter} connections to finish."
    end
    self.update_procline
    EM.stop if $server.nil? and @@counter == 0
    @@counter
  end

  def self.set_router(block)
    @@router = block
  end

  def self.router
    @@router
  end

  def self.graceful_shutdown(signal)
    EM.stop_server($server) if $server
    LOGGER.info "Received #{signal} signal. No longer accepting new connections."
    LOGGER.info "Waiting for #{BeanstalkProxy.count} connections to finish."
    $server = nil
    EM.stop if BeanstalkProxy.count == 0
  end

  def self.fast_shutdown(signal)
    EM.stop_server($server) if $server
    LOGGER.info "Received #{signal} signal. No longer accepting new connections."
    LOGGER.info "Maximum time to wait for connections is #{MAX_FAST_SHUTDOWN_SECONDS} seconds."
    LOGGER.info "Waiting for #{BeanstalkProxy.count} connections to finish."
    $server = nil
    EM.stop if BeanstalkProxy.count == 0
    Thread.new do
      sleep MAX_FAST_SHUTDOWN_SECONDS
      exit!
    end
  end

  def self.set_connect_error_callback(&block)
    @@connect_error_callback = block
  end

  def self.connect_error_callback
    @@connect_error_callback
  end

  def self.set_inactivity_error_callback(&block)
    @@inactivity_error_callback = block
  end

  def self.inactivity_error_callback
    @@inactivity_error_callback
  end

  def self.run(name, host, port)
    @@totalcounter = 0
    @@maxcounter = 0
    @@counter = 0
    @@name = name
    @@listen = "#{host}:#{port}"
    @@connect_error_callback ||= proc { |remote| }
    @@inactivity_error_callback ||= proc { |remote| }
    self.update_procline
    EM.epoll

    EM.run do
      BeanstalkProxy::ClientConnection.start(host, port)
      trap('QUIT') do
        self.graceful_shutdown('QUIT')
      end
      trap('TERM') do
        self.fast_shutdown('TERM')
      end
      trap('INT') do
        self.fast_shutdown('INT')
      end
    end
  end
end

module Kernel
  def proxy(&block)
    BeanstalkProxy.set_router(block)
  end

  def proxy_connect_error(&block)
    BeanstalkProxy.set_connect_error_callback(&block)
  end

  def proxy_inactivity_error(&block)
    BeanstalkProxy.set_inactivity_error_callback(&block)
  end
end
