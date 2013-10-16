module BeanstalkProtocol

  class ParserError < StandardError
  end


  # put <pri> <delay> <ttr> <bytes>\r\n
  # <data>\r\n
  PUT_CMD = /\Aput (\d+) (\d+) (\d+) (\d+)\r\n\z/
  # use <tube>\r\n
  USE_CMD = /\Ause ([a-zA-Z0-9+\/;,$_()][a-zA-Z0-9+\/;,$_()\-]{0,199})\r\n\z/
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
    else
      LOGGER.error "Unknown beanstalk command: #{line}"
      fail ParserError
    end
  end

  def process_response!(response, connection_response_state)
  end

end