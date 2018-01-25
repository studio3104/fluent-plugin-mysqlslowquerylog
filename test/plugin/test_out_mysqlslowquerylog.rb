# coding: utf-8

require 'helper'

class MySQLSlowQueryLogOutputTest < Test::Unit::TestCase
  def setup
    Fluent::Test.setup
  end

  CONFIG = %[
    add_tag_prefix cocatenated.
  ]

  def create_driver(conf = CONFIG, tag='test')
    Fluent::Test::OutputTestDriver.new(Fluent::MySQLSlowQueryLogOutput, tag).configure(conf)
  end

  def test_emit
    1.times do

    d1 = create_driver
    d2 = create_driver(CONFIG,'test2')
    d3 = create_driver(CONFIG,'test3')
    d4 = create_driver(CONFIG,'test4')

    d1.run do
      d1.emit('message' => "/usr/sbin/mysqld, Version: 5.5.28-0ubuntu0.12.04.2-log ((Ubuntu)). started with:")
      d1.emit('message' => "Tcp port: 3306  Unix socket: /var/run/mysqld/mysqld.sock")
      d1.emit('message' => "Time                 Id Command    Argument")
      d1.emit('message' => "# Time: 130105 16:43:42")
      d1.emit('message' => "# User@Host: debian-sys-maint[debian-sys-maint] @ localhost []")
      d1.emit('message' => "# Query_time: 0.000167  Lock_time: 0.000057 Rows_sent: 1  Rows_examined: 7")
      d1.emit('message' => "SET timestamp=1357371822;")
      d1.emit('message' => "SELECT count(*) FROM mysql.user WHERE user='root' and password='';")
    end

    d2.run do
      d2.emit('message' => "# User@Host: debian-sys-maint[debian-sys-maint] @ localhost []")
      d2.emit('message' => "# Query_time: 0.002998  Lock_time: 0.000078 Rows_sent: 31  Rows_examined: 81")
      d2.emit('message' => "SET timestamp=61357371822;")
      d2.emit('message' => "select concat('select count(*) into @discard from `',")
      d2.emit('message' => "                    TABLE_SCHEMA, '`.`', TABLE_NAME, '`')")
      d2.emit('message' => "      from information_schema.TABLES where ENGINE='MyISAM';")

      d2.emit('message' => "# Time: 130105 18:04:21")
      d2.emit('message' => "# User@Host: root[root] @ localhost []")
      d2.emit('message' => "# Query_time: 0.000398  Lock_time: 0.000117 Rows_sent: 7  Rows_examined: 7")
      d2.emit('message' => "use mysql;")
      d2.emit('message' => "SET timestamp=1357376661;")
      d2.emit('message' => "select * from user;")
    end

    d3.run do
      d3.emit('message' => "# User@Host: debian-sys-maint[debian-sys-maint] @ localhost []")
      d3.emit('message' => "# Query_time: 0.014260  Lock_time: 0.000182 Rows_sent: 0  Rows_examined: 808")

      d4.emit('message' => "# User@Host: debian-sys-maint[debian-sys-maint] @ localhost []")
      d4.emit('message' => "# Query_time: 0.000262  Lock_time: 0.000200 Rows_sent: 0  Rows_examined: 0")
      d4.emit('message' => "SET timestamp=1357371822;")
      d4.emit('message' => "select count(*) into @discard from `information_schema`.`EVENTS`;")

      d3.emit('message' => "SET timestamp=1357371822;")
      d3.emit('message' => "select count(*) into @discard from `information_schema`.`COLUMNS`;")
    end

    assert_equal 1, d1.emits.size
    assert_equal 2, d2.emits.size
    assert_equal 2, d3.emits.size

    assert Time.at(d1.emits[0][1]).to_s.encode("UTF-8").start_with?('2013-01-05 16:43:42')
    assert Time.at(d2.emits[1][1]).to_s.encode("UTF-8").start_with?('2013-01-05 18:04:21')

    assert_equal 'cocatenated.test',  d1.emits[0][0]
    assert_equal 'cocatenated.test2', d2.emits[0][0]
    assert_equal 'cocatenated.test2', d2.emits[1][0]
    assert_equal 'cocatenated.test4', d3.emits[0][0]
    assert_equal 'cocatenated.test3', d3.emits[1][0]

    assert_equal({
      "user"          => "debian-sys-maint[debian-sys-maint]",
      "host"          => "localhost",
      "query_time"    => 0.000167,
      "lock_time"     => 5.7e-05,
      "rows_sent"     => 1,
      "rows_examined" => 7,
      "sql"           => "SET timestamp=1357371822; SELECT count(*) FROM mysql.user WHERE user='root' and password='';"
    }, d1.emits[0][2])

    assert_equal({
      "user"          => "debian-sys-maint[debian-sys-maint]",
      "host"          => "localhost",
      "query_time"    => 0.002998,
      "lock_time"     => 7.8e-05,
      "rows_sent"     => 31,
      "rows_examined" => 81,
      "sql"           => "SET timestamp=61357371822; select concat('select count(*) into @discard from `', TABLE_SCHEMA, '`.`', TABLE_NAME, '`') from information_schema.TABLES where ENGINE='MyISAM';"
    }, d2.emits[0][2])

    assert_equal({
      "user"          => "root[root]",
      "host"          => "localhost",
      "query_time"    => 0.000398,
      "lock_time"     => 0.000117,
      "rows_sent"     => 7,
      "rows_examined" => 7,
      "sql"           => "use mysql; SET timestamp=1357376661; select * from user;"
    }, d2.emits[1][2])

    assert_equal({
      "user"          => "debian-sys-maint[debian-sys-maint]",
      "host"          => "localhost",
      "query_time"    => 0.000262,
      "lock_time"     => 0.0002,
      "rows_sent"     => 0,
      "rows_examined" => 0,
      "sql"           => "SET timestamp=1357371822; select count(*) into @discard from `information_schema`.`EVENTS`;"
    }, d3.emits[0][2])

    assert_equal({
      "user"          => "debian-sys-maint[debian-sys-maint]",
      "host"          => "localhost",
      "query_time"    => 0.01426,
      "lock_time"     => 0.000182,
      "rows_sent"     => 0,
      "rows_examined" => 808,
      "sql"           => "SET timestamp=1357371822; select count(*) into @discard from `information_schema`.`COLUMNS`;"
    }, d3.emits[1][2])

    end
  end
end
