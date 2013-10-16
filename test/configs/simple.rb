LOGGER = Logger.new(File.new(File.expand_path('../../../tmp/proxy.log', __FILE__), 'w'))

proxy do |bs_cmd, client_connection|
  LOGGER.debug("proxy: #{client_connection.peer}")
  if bs_cmd[:command] == :use && bs_cmd[:tube] == 'a'
    { :remote => "localhost:9980" }
  elsif bs_cmd[:command] == :use && bs_cmd[:tube] == 'b'
    { :remote => "localhost:9981" }
  elsif bs_cmd[:command] == :use && bs_cmd[:tube] == 'c'
    { :remote => "localhost:9980", :data => 'ccc'  }
  elsif bs_cmd[:command] == :use && bs_cmd[:tube] == 'd'
    { :close => 'ddd' }
  elsif bs_cmd[:command] == :use && bs_cmd[:tube] == 'g'
    { :remote => "localhost:9980", :data => 'g2', :reply => 'g3-' }
  elsif bs_cmd[:command] == :use && bs_cmd[:tube] == ('e' * 180 + 'f')
    { :remote => "localhost:9980" }
  elsif bs_cmd[:command] == :use && bs_cmd[:tube] == 'connect_reject'
    { :remote => "localhost:9989" }
  elsif bs_cmd[:command] == :use && bs_cmd[:tube] == 'inactivity'
    { :remote => "localhost:9980", :data => 'sleep 3', :inactivity_timeout => 1 }
  else
    { :close => true }
  end

=begin  if data == 'a'
    { :remote => "localhost:9980" }
  elsif data == 'b'
    { :remote => "localhost:9981" }
  elsif data == 'c'
    { :remote => "localhost:9980", :data => 'ccc' }
  elsif data == 'd'
    { :close => 'ddd' }
  elsif data == 'e' * 2048
    { :noop => true }
  elsif data == 'e' * 2048 + 'f'
    { :remote => "localhost:9980" }
  elsif data == 'g'
    { :remote => "localhost:9980", :data => 'g2', :reply => 'g3-' }
  elsif data == 'connect reject'
    { :remote => "localhost:9989" }
  elsif data == 'inactivity'
    { :remote => "localhost:9980", :data => 'sleep 3', :inactivity_timeout => 1 }
  else
    { :close => true }
  end
=end
end

ERROR_FILE = File.expand_path('../../proxy_error', __FILE__)

proxy_connect_error do |remote|
  File.open(ERROR_FILE, 'wb') { |fd| fd.write("connect error: #{remote}") }
end

proxy_inactivity_error do |remote|
  File.open(ERROR_FILE, 'wb') { |fd| fd.write("activity error: #{remote}") }
end

proxy_client_connection_unbind do |client_connection|
  
end