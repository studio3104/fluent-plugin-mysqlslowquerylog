require 'helper'

class MySQLSlowQueryLogOutputTest < Test::Unit::TestCase
  def setup
    Fluent::Test.setup
  end

  CONFIG = %[
    add_tag_prefix concated
  ]

  def create_driver(conf = CONFIG, tag='test')
    Fluent::Test::OutputTestDriver.new(Fluent::MySQLSlowQueryLogOutput, tag).configure(conf)
  end

  def test_emit
    d1 = create_driver
    d2 = create_driver(CONFIG,'test2')

    d1.run do
      d2.emit('message' => '# Time: 130105 16:43:42')
      d1.emit('message' => '# User@Host: debian-sys-maint[debian-sys-maint] @ localhost []')
      d1.emit('message' => '# Query_time: 0.000167  Lock_time: 0.000057 Rows_sent: 1  Rows_examined: 7')
      d1.emit('message' => 'SET timestamp=1357371822;')
      d1.emit('message' => "SELECT count(*) FROM mysql.user WHERE user='root' and password='';")
    end

    d2.run do
      d2.emit('message' => '# User@Host: debian-sys-maint[debian-sys-maint] @ localhost []')
      d2.emit('message' => '# Query_time: 0.002998  Lock_time: 0.000078 Rows_sent: 31  Rows_examined: 81')
      d2.emit('message' => 'SET timestamp=61357371822;')
      d2.emit('message' => "select concat('select count(*) into @discard from `',")
      d2.emit('message' => "                    TABLE_SCHEMA, '`.`', TABLE_NAME, '`')")
      d2.emit('message' => "      from information_schema.TABLES where ENGINE='MyISAM';")
      
    end
    assert_equal 1, d1.emits.size
    assert_equal 1, d2.emits.size
  end

#  def test_emit
#    d = create_driver
#    d.run do
#      d.emit( 'foo' => '{"bar" : "baz"}', 'hoge' => 'fuga' )
#    end
#
#    assert_equal 1, d.emits.size
#  end

  def test_configure
    #### set configurations
    # d = create_driver %[
    #   path test_path
    #   compress gz
    # ]
    #### check configurations
    # assert_equal 'test_path', d.instance.path
    # assert_equal :gz, d.instance.compress
  end

  def test_format
    d = create_driver

    # time = Time.parse("2011-01-02 13:14:15 UTC").to_i
    # d.emit({"a"=>1}, time)
    # d.emit({"a"=>2}, time)

    # d.expect_format %[2011-01-02T13:14:15Z\ttest\t{"a":1}\n]
    # d.expect_format %[2011-01-02T13:14:15Z\ttest\t{"a":2}\n]

    # d.run
  end

  def test_write
    d = create_driver

    # time = Time.parse("2011-01-02 13:14:15 UTC").to_i
    # d.emit({"a"=>1}, time)
    # d.emit({"a"=>2}, time)

    # ### FileOutput#write returns path
    # path = d.run
    # expect_path = "#{TMP_DIR}/out_file_test._0.log.gz"
    # assert_equal expect_path, path
  end
end
