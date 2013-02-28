class Fluent::MySQLDumpSlowOutput < Fluent::Output
  Fluent::Plugin.register_output('mysqldumpslow', self)
  include Fluent::HandleTagNameMixin

  config_param :dump_interval, :time,   :default => nil
  config_param :unit,          :string, :default => 'hour'
  config_param :aggregate,     :string, :default => 'tag'

  def start
    super
  end

  def shutdown
    super
  end

  def configure(conf)
    super
    @messages = {}

    if @dump_interval
      @tick = @dump_interval.to_i
    else
      @tick = case @unit
              when 'minute' then 60
              when 'hour'   then 3600
              when 'day'    then 86400
              else
                raise RuntimeError, "@unit must be one of minute/hour/day"
              end
    end

    @aggregate = case @aggregate
                 when 'tag' then :tag
                 when 'all' then :all
                 else
                   raise Fluent::ConfigError, "out_mysqldumpslow aggregate allows tag/all"
                 end

    if !@remove_tag_prefix && !@remove_tag_suffix && !@add_tag_prefix && !@add_tag_suffix
      raise Fluent::ConfigError, "out_mysqldumpslow: At least one of option, remove_tag_prefix, remove_tag_suffix, add_tag_prefix or add_tag_suffix is required to be set."
    end

    @mutex = Mutex.new
  end

  def emit(tag, es, chain)
    if !@messages[tag].instance_of?(Hash)
      @messages[tag] = { 'last_check' => Fluent::Engine.now }
    end
    es.each do |time, record|
      dumpslow(tag, record.clone)
    end

    chain.next
  end

  def dumpslow(tag, r)
    if @aggregate == :all
      tag = 'all'
    end
    r['sql'].sub! /^use \w+;\s*/, ''
    r['sql'].sub! /\s?SET timestamp=\d+;\s*/, ''
    r['sql'].gsub! /\b\d+\b/, "N"
    r['sql'].gsub! /\b0x[0-9A-Fa-f]+\b/, "N"
    r['sql'].gsub! /''/, "'S'"
    r['sql'].gsub! /""/, "\"S\""
    r['sql'].gsub! /(\\')/, ''
    r['sql'].gsub! /(\\")/, ''
    r['sql'].gsub! /'[^']+'/, "'S'"
    r['sql'].gsub! /"[^"]+"/, "\"S\""
    r['sql'].gsub! /(([NS],){100,})/, "#{sprintf("\\2,{repeated %d times}",('\1').size/2)}"

    @messages[tag]["#{r['sql']}"] ||= {
      'users'      => {},
      'hosts'      => {},
      'count'      => 0,
      'query_time' => 0,
      'lock_time'  => 0,
      'rows_sent'  => 0,
    }

    @messages[tag]["#{r['sql']}"]['count']      += 1
    @messages[tag]["#{r['sql']}"]['query_time'] += r['query_time']
    @messages[tag]["#{r['sql']}"]['lock_time']  += r['lock_time']
    @messages[tag]["#{r['sql']}"]['rows_sent']  += r['rows_sent']
    if r['user']
      @messages[tag]["#{r['sql']}"]['users']["#{r['user']}"] ||= 0
      @messages[tag]["#{r['sql']}"]['users']["#{r['user']}"] +=  1
    end
    if r['host']
      @messages[tag]["#{r['sql']}"]['hosts']["#{r['host']}"] ||= 0
      @messages[tag]["#{r['sql']}"]['hosts']["#{r['host']}"] +=  1
    end

    if Fluent::Engine.now - @messages[tag]['last_check'] >= @tick
      flush_emit(tag, generate_message(@messages[tag]))
    end
  end

  def generate_message(message)
    ret = []
    message.each_with_index do |(k, v), i|
      unless k == 'last_check'
        ret[i-1] = v
        ret[i-1]['sql'] = k
      end
    end
    ret
  end

  def flush_emit(tag, message)
    @messages[tag].clear
    time = Fluent::Engine.now
    _tag = tag.clone
    filter_record(_tag, time, message)
    if tag != _tag
      Fluent::Engine.emit(_tag, time, message)
    else
      $log.warn "Can not emit message because the tag has not changed. Dropped record #{@s}"
    end

    @messages[tag]['last_check'] = Fluent::Engine.now
  end
end

class Fluent::MySQLSlowQueryLogOutput < Fluent::Output
  Fluent::Plugin.register_output('mysqlslowquerylog', self)
  include Fluent::HandleTagNameMixin
  config_param :explain,  :bool,   :default => false
  config_param :username, :string, :default => nil
  config_param :password, :string, :default => nil
  config_param :hostname, :string, :default => nil

  attr_reader :host

  def configure(conf)
    super

    @slowlogs = {}

    if !@remove_tag_prefix && !@remove_tag_suffix && !@add_tag_prefix && !@add_tag_suffix
      raise ConfigError, "out_mysqlslowquerylog: At least one of option, remove_tag_prefix, remove_tag_suffix, add_tag_prefix or add_tag_suffix is required to be set."
    end

    if @explain
      require 'mysql2-cs-bind'

      if @username && @password && @hostname
        @host = {
          :hostname     => @hostname,
          :connect_fail => false,
          :target_db    => nil
        }
      else
        raise ConfigError, "out_myslowquerylog: In order to explain, username, password and hostname are required to be set."
      end
    end
  end

  def set_dbhandler
    begin
      @host[:mysqlclient] = Mysql2::Client.new(
        :host     => @host[:hostname],
        :username => @username,
        :password => @password,
        :database => "information_schema"
      )
    rescue
      $log.warn "Can not connect for user '#{@username}'@'#{@host[:hostname]}'."
      @host[:connect_fail] = true
    end
  end

  def start
    super
  end

  def shutdown
    super
  end

  def emit(tag, es, chain)
    if !@slowlogs[:"#{tag}"].instance_of?(Array)
      @slowlogs[:"#{tag}"] = []
    end
    es.each do |time, record|
      concat_messages(tag, time, record)
    end

    chain.next
  end

  def concat_messages(tag, time, record)
    record.each do |key, value|
      @slowlogs[:"#{tag}"] << value
      if value.end_with?(';') && !value.upcase.start_with?('USE ', 'SET TIMESTAMP=')
        parse_message(tag, time)
      end
    end
  end

  REGEX1 = /^#? User\@Host:\s+(\S+)\s+\@\s+(\S+).*/
  REGEX2 = /^# Query_time: ([0-9.]+)\s+Lock_time: ([0-9.]+)\s+Rows_sent: ([0-9.]+)\s+Rows_examined: ([0-9.]+).*/
  def parse_message(tag, time)
    record = {}
    date   = nil

    # Skip the message that is output when after flush-logs or restart mysqld.
    # e.g.) /usr/sbin/mysqld, Version: 5.5.28-0ubuntu0.12.04.2-log ((Ubuntu)). started with:
    begin
      message = @slowlogs[:"#{tag}"].shift
    end while !message.start_with?('#')

    if message.start_with?('# Time: ')
      date    = Time.parse(message[8..-1].strip)
      message = @slowlogs[:"#{tag}"].shift
    end

    message =~ REGEX1
    record['user'] = $1
    record['host'] = $2
    message = @slowlogs[:"#{tag}"].shift

    message =~ REGEX2
    record['query_time']    = $1.to_f
    record['lock_time']     = $2.to_f
    record['rows_sent']     = $3.to_i
    record['rows_examined'] = $4.to_i

    record['sql'] = @slowlogs[:"#{tag}"].map {|m| m.strip}.join(' ')
    record['explain'] = explain(record['sql']) if @explain

    time   = date.to_i if date
    record = dumpslow(record) if @dumpslow
    flush_emit(tag, time, record)
  end

  def explain(query)
    if !@host[:mysqlclient] && !@host[:connect_fail]
      set_dbhandler
    end

    if @host[:mysqlclient] && !@host[:connect_fail]
      if query =~ /(select[^\;]+)/i
        select_statement  = $1
      end
      if query =~ /^use ([^\;]+)/i
        @host[:target_db] = "`#{$1}`"
      end

      if @host[:target_db]
        if select_statement
          @host[:mysqlclient].query("use #{@host[:target_db]}")
          return @host[:mysqlclient].query("EXPLAIN #{select_statement}").each
        end
      else
        longquerytime = @host[:mysqlclient].query("SELECT VARIABLE_VALUE FROM GLOBAL_VARIABLES WHERE VARIABLE_NAME = 'LONG_QUERY_TIME'").first["VARIABLE_VALUE"].to_f
        @host[:mysqlclient].query("SELECT SLEEP(#{longquerytime}) -- This query was issued by fluent-plugin-mysqlslowquerylog")
      end
    end

    return nil
  end

  def flush_emit(tag, time, record)
    @slowlogs[:"#{tag}"].clear
    _tag = tag.clone
    filter_record(_tag, time, record)
    if tag != _tag
      Fluent::Engine.emit(_tag, time, record)
    else
      $log.warn "Can not emit message because the tag has not changed. Dropped record #{record}"
    end
  end
end
