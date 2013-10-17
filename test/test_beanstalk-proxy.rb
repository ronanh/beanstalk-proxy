require 'test_helper'

def assert_proxy(host, port, send, recv)
  sock = TCPSocket.new(host, port)
  sock.write(send)
  assert_equal recv, sock.read
  sock.close
end

class BeanstalkProxyTest < Test::Unit::TestCase
  def setup
    @proxy_error_file = "#{File.dirname(__FILE__)}/proxy_error"
  end

  def teardown
    File.unlink(@proxy_error_file) rescue nil
  end

  should "handle simple routing" do
    assert_proxy('localhost', 9990, "use a\r\n", "9980:use a\r\n")
    assert_proxy('localhost', 9990, "use b\r\n", "9981:use b\r\n")
  end

  should "handle connection closing" do
    sock = TCPSocket.new('localhost', 9990)
    sock.write("xxx\r\n")
    assert_equal nil, sock.read(1)
    sock.close
  end

  should "handle rewrite routing" do
    assert_proxy('localhost', 9990, "use c\r\n", "9980:ccc")
  end

  should "handle rewrite closing" do
    assert_proxy('localhost', 9990, "use d\r\n", 'ddd')
  end

  should "handle data plus reply" do
    assert_proxy('localhost', 9990, "use g\r\n", 'g3-9980:g2')
  end

  should "handle splitted messages" do
    sock = TCPSocket.new('localhost', 9990)
    sock.write( 'use ' + 'e' * 180)
    sock.flush
    sock.write('f')
    sock.flush
    sock.write("\r\n")
    assert_equal '9980:use ' + 'e' * 180 + "f\r\n", sock.read
    sock.close
  end

 should "call proxy_connect_error when a connection is rejected" do
    sock = TCPSocket.new('localhost', 9990)
    sock.write("use connect_reject\r\n")
    sock.flush
    assert_equal "", sock.read
    sock.close
    assert_equal "connect error: localhost:9989", File.read(@proxy_error_file)
  end

  should "call proxy_inactivity_error when initial read times out" do
    sock = TCPSocket.new('localhost', 9990)
    sent = Time.now
    sock.write("use inactivity\r\n")
    sock.flush
    assert_equal "", sock.read
    assert_operator Time.now - sent, :>=, 1.0
    assert_equal "activity error: localhost:9980", File.read(@proxy_error_file)
    sock.close
  end

  should "not consider client disconnect a server error" do
    sock = TCPSocket.new('localhost', 9990)
    sock.write("use inactivity\r\n")
    sock.close
    sleep 3.1
    assert !File.exist?(@proxy_error_file)
  end
  should "handle put command" do
    assert_proxy('localhost', 9990, "put 10 0 10 5\r\n12345\r\n", "12345")
  end

  should "handle use command" do
    assert_proxy('localhost', 9990, "use test_use\r\n", "test_use")
  end

  should "handle reserve command" do
    assert_proxy('localhost', 9990, "reserve\r\n", "reserve")
  end

  should "handle reserve_with_timeout command" do
    assert_proxy('localhost', 9990, "reserve-with-timeout 42\r\n", "reserve-with-timeout 42")
  end

  should "handle delete command" do
    assert_proxy('localhost', 9990, "delete 55\r\n", "delete 55")
  end

  should "handle release command" do
    assert_proxy('localhost', 9990, "release 66 11 1\r\n", "release 66")
  end

  should "handle bury command" do
    assert_proxy('localhost', 9990, "bury 77 12\r\n", "bury 77")
  end

  should "handle touch command" do
    assert_proxy('localhost', 9990, "touch 32\r\n", "touch 32")
  end

  should "handle watch command" do
    assert_proxy('localhost', 9990, "watch test_watch44\r\n", "watch test_watch44")
  end

  should "handle ignore command" do
    assert_proxy('localhost', 9990, "ignore test/ignore79\r\n", "ignore test/ignore79")
  end

  should "handle peek command" do
    assert_proxy('localhost', 9990, "peek 48\r\n", "peek 48")
  end

  should "handle peek-ready command" do
    assert_proxy('localhost', 9990, "peek-ready\r\n", "peek-ready")
  end

  should "handle peek-delayed command" do
    assert_proxy('localhost', 9990, "peek-delayed\r\n", "peek-delayed")
  end

  should "handle peek-buried command" do
    assert_proxy('localhost', 9990, "peek-buried\r\n", "peek-buried")
  end

  should "handle kick command" do
    assert_proxy('localhost', 9990, "kick 11\r\n", "kick 11")
  end

  should "handle kick-job command" do
    assert_proxy('localhost', 9990, "kick-job 12\r\n", "kick-job 12")
  end

  should "handle stats-job command" do
    assert_proxy('localhost', 9990, "stats-job 15\r\n", "stats-job 15")
  end

  should "handle stats-tube command" do
    assert_proxy('localhost', 9990, "stats-tube tube16\r\n", "stats-tube tube16")
  end

  should "handle stats command" do
    assert_proxy('localhost', 9990, "stats\r\n", "stats")
  end

  should "handle list-tubes command" do
    assert_proxy('localhost', 9990, "list-tubes\r\n", "list-tubes")
  end

  should "handle list-tube-used command" do
    assert_proxy('localhost', 9990, "list-tube-used\r\n", "list-tube-used")
  end

  should "handle list-tubes-watched command" do
    assert_proxy('localhost', 9990, "list-tubes-watched\r\n", "list-tubes-watched")
  end

  should "handle quit command" do
    assert_proxy('localhost', 9990, "quit\r\n", "quit")
  end

  should "handle pause-tube command" do
    assert_proxy('localhost', 9990, "pause-tube tube99 10\r\n", "pause-tube tube99")
  end

end
