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
  elsif bs_cmd[:command] == :put
    { :noop => true }
  elsif bs_cmd[:command] == :put_body_chunk
    { :close => bs_cmd[:data] }
  elsif bs_cmd[:command] == :use && bs_cmd[:tube] == 'test_use'
    { :close => bs_cmd[:tube] }
  elsif bs_cmd[:command] == :reserve 
    { :close => 'reserve' }
  elsif bs_cmd[:command] == :reserve_with_timeout
    { :close => "reserve-with-timeout #{bs_cmd[:timeout]}" }
  elsif bs_cmd[:command] == :delete
    { :close => "delete #{bs_cmd[:id]}" }
  elsif bs_cmd[:command] == :release
    { :close => "release #{bs_cmd[:id]}" }
  elsif bs_cmd[:command] == :bury
    { :close => "bury #{bs_cmd[:id]}" }
  elsif bs_cmd[:command] == :touch
    { :close => "touch #{bs_cmd[:id]}" }
  elsif bs_cmd[:command] == :watch
    { :close => "watch #{bs_cmd[:tube]}" }
  elsif bs_cmd[:command] == :ignore
    { :close => "ignore #{bs_cmd[:tube]}" }
  elsif bs_cmd[:command] == :peek
    { :close => "peek #{bs_cmd[:id]}" }
  elsif bs_cmd[:command] == :peek_ready
    { :close => "peek-ready" }
  elsif bs_cmd[:command] == :peek_delayed
    { :close => "peek-delayed" }
  elsif bs_cmd[:command] == :peek_buried
    { :close => "peek-buried" }
  elsif bs_cmd[:command] == :kick
    { :close => "kick #{bs_cmd[:bound]}" }
  elsif bs_cmd[:command] == :kick_job
    { :close => "kick-job #{bs_cmd[:id]}" }
  elsif bs_cmd[:command] == :stats_job
    { :close => "stats-job #{bs_cmd[:id]}" }
  elsif bs_cmd[:command] == :stats_tube
    { :close => "stats-tube #{bs_cmd[:tube]}" }
  elsif bs_cmd[:command] == :stats
    { :close => "stats" }
  elsif bs_cmd[:command] == :list_tubes
    { :close => "list-tubes" }
  elsif bs_cmd[:command] == :list_tube_used
    { :close => "list-tube-used" }
  elsif bs_cmd[:command] == :list_tubes_watched
    { :close => "list-tubes-watched" }
  elsif bs_cmd[:command] == :quit
    { :close => "quit" }
  elsif bs_cmd[:command] == :pause_tube
    { :close => "pause-tube #{bs_cmd[:tube]}" }
  else
    { :close => true }
  end

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