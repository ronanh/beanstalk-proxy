module BeanstalkProtocol

  class ParserError < StandardError
  end


  # put <pri> <delay> <ttr> <bytes>\r\n
  # <data>\r\n
  PUT_CMD = /\Aput (\d+) (\d+) (\d+) (\d+)\r\n\z/
  # use <tube>\r\n
  USE_CMD = /\Ause ([a-zA-Z0-9+\/;,$_()][a-zA-Z0-9+\/;,$_()\-]{0,199})\r\n\z/
  # reserve\r\n
  RESERVE_CMD = /\Areserve\r\n\z/
  # reserve-with-timeout <seconds>\r\n
  RESERVE_WITH_TIMEOUT_CMD = /\Areserve-with-timeout (\d+)\r\n\z/
  # delete <id>\r\n
  DELETE_CMD = /\Adelete (\d+)\r\n\z/
  # release <id> <pri> <delay>\r\n
  RELEASE_CMD = /\Arelease (\d+) (\d+) (\d+)\r\n\z/
  # bury <id> <pri>\r\n
  BURY_CMD = /\Abury (\d+) (\d+)\r\n\z/
  # touch <id>\r\n
  TOUCH_CMD = /\Atouch (\d+)\r\n\z/
  # watch <tube>\r\n
  WATCH_CMD = /\Awatch ([a-zA-Z0-9+\/;,$_()][a-zA-Z0-9+\/;,$_()\-]{0,199})\r\n\z/
  # ignore <tube>\r\n
  IGNORE_CMD = /\Aignore ([a-zA-Z0-9+\/;,$_()][a-zA-Z0-9+\/;,$_()\-]{0,199})\r\n\z/
  # peek <id>\r\n
  PEEK_CMD = /\Apeek (\d+)\r\n\z/
  # peek-ready\r\n
  PEEK_READY_CMD = /\Apeek-ready\r\n\z/
  # peek-delayed\r\n
  PEEK_DELAYED_CMD = /\Apeek-delayed\r\n\z/
  # peek-buried\r\n
  PEEK_BURIED_CMD = /\Apeek-buried\r\n\z/
  # kick <bound>\r\n
  KICK_CMD = /\Akick (\d+)\r\n\z/
  # kick-job <id>\r\n
  KICK_JOB_CMD = /\Akick-job (\d+)\r\n\z/
  # stats-job <id>\r\n
  STATS_JOB_CMD = /\Astats-job (\d+)\r\n\z/
  # stats-tube <tube>\r\n
  STATS_TUBE_CMD = /\Astats-tube ([a-zA-Z0-9+\/;,$_()][a-zA-Z0-9+\/;,$_()\-]{0,199})\r\n\z/
  # stats\r\n
  STATS_CMD = /\Astats\r\n\z/
  # list-tubes\r\n
  LIST_TUBES_CMD = /\Alist-tubes\r\n\z/
  # list-tube-used\r\n
  LIST_TUBE_USED_CMD = /\Alist-tube-used\r\n\z/
  # list-tubes-watched\r\n
  LIST_TUBES_WATCHED_CMD = /\Alist-tubes-watched\r\n\z/
  # quit\r\n
  QUIT_CMD = /\Aquit\r\n\z/
  # pause-tube <tube-name> <delay>\r\n
  PAUSE_TUBE_CMD = /\Apause-tube ([a-zA-Z0-9+\/;,$_()][a-zA-Z0-9+\/;,$_()\-]{0,199}) (\d+)\r\n\z/


  LINE_DELIMITER = "\r\n".freeze
  
  module_function
  
  def process_request!(request, connection_request_state)
    case connection_request_state[:state]
    when :header
      index = request.index(LINE_DELIMITER)
      if index
        line = request.slice!(0,index+2)
        command = parse_beanstalk_command(line)
        if command[:command] == :put
          LOGGER.debug "processing put size=#{command[:bytes]}"
          connection_request_state[:state] = :body
          connection_request_state[:data_size] = command[:bytes]+2
        end
        return command
      else
        fail 'Beanstalk request header too big' if request.size > 65535
        # no line to read while waiting for header
        return false
      end
    when :body
      if request.size >= connection_request_state[:data_size]
        # end of body + trailer reached
        data = request.slice!(0, connection_request_state[:data_size]-2)
        trailer = request.slice!(0, 2)
        fail "Bad trailer! #{trailer}" if trailer != LINE_DELIMITER
        connection_request_state.delete(:data_size)
        connection_request_state[:state] = :header
        return { command: :put_body_chunk, data: data }
      else
        if request.size < connection_request_state[:data_size] - 2
          connection_request_state[:data_size] -= request.size
          data = request.slice!(0, request.size)
          return { command: :put_body_chunk, data: data}
        else
          # we're only missing the body trailer
          # don't process the data since the state machine is not
          # designed to handle that case. just wait...
          return false
        end
      end

    else
      fail 'Unexepected request connection state'
    end
  end

  def parse_beanstalk_command(line)
    case line
    when PUT_CMD
      { command: :put, pri: Integer($1), delay: Integer($2), ttr: Integer($3), bytes: Integer($4), data: line }
    when USE_CMD
      { command: :use, tube: $1, data: line }
    when RESERVE_CMD
      { command: :reserve, data: line }
    when RESERVE_WITH_TIMEOUT_CMD
      { command: :reserve_with_timeout, timeout: Integer($1), data: line }
    when DELETE_CMD
      { command: :delete, id: Integer($1), data: line }
    when RELEASE_CMD
      { command: :release, id: Integer($1), pri: Integer($2), delay: Integer($3), data: line }
    when BURY_CMD
      { command: :bury, id: Integer($1), pri: Integer($2), data: line }
    when TOUCH_CMD
      { command: :touch, id: Integer($1), data: line }
    when WATCH_CMD
      { command: :watch, tube: $1, data: line }
    when IGNORE_CMD
      { command: :ignore, tube: $1, data: line }
    when PEEK_CMD
      { command: :peek, id: Integer($1), data: line }
    when PEEK_READY_CMD
      { command: :peek_ready, data: line }
    when PEEK_DELAYED_CMD
      { command: :peek_delayed, data: line }
    when PEEK_BURIED_CMD
      { command: :peek_buried, data: line }
    when KICK_CMD
      { command: :kick, bound: Integer($1), data: line }
    when KICK_JOB_CMD
      { command: :kick_job, id: Integer($1), data: line }
    when STATS_JOB_CMD
      { command: :stats_job, id: Integer($1), data: line }
    when STATS_TUBE_CMD
      { command: :stats_tube, tube: $1, data: line }
    when STATS_CMD
      { command: :stats, data: line }
    when LIST_TUBES_CMD
      { command: :list_tubes, data: line }
    when LIST_TUBE_USED_CMD
      { command: :list_tube_used, data: line }
    when LIST_TUBES_WATCHED_CMD
      { command: :list_tubes_watched, data: line }
    when QUIT_CMD
      { command: :quit, data: line }
    when PAUSE_TUBE_CMD
      { command: :pause_tube, tube: $1, delay: Integer($2), data: line }
    else
      LOGGER.error "Unknown beanstalk command: #{line}"
      fail ParserError
    end
  end

  def process_response!(response, connection_response_state)
  end

end