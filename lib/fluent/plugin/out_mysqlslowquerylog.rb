class Fluent::MySQLSlowQueryLogOutput < Fluent::Output
  Fluent::Plugin.register_output('mysqlslowquerylog', self)
  include Fluent::HandleTagNameMixin

  config_param :explain,    :bool,   :default => false
  config_param :dbuser,     :string, :default => nil
  config_param :dbpassword, :string, :default => nil

  def configure(conf)
    super
    @slowlogs = Hash.new

    if !@remove_tag_prefix && !@remove_tag_suffix && !@add_tag_prefix && !@add_tag_suffix
      raise ConfigError, "out_slowquery: At least one of option, remove_tag_prefix, remove_tag_suffix, add_tag_prefix or add_tag_suffix is required to be set."
    end
  end

  def start
    super
  end

  def shutdown
    super
  end

  def emit(tag, es, chain)
    if !@slowlogs[:"#{tag}"]
      @slowlogs[:"#{tag}"] = Array.new
    end
    es.each do |time, record|
      concat_messages(tag, time, record)
    end

    chain.next
  end

  def concat_messages(tag, time, record)
    record.each do |key, value|
      @slowlogs[:"#{tag}"] << value
      if value !~ /^set timestamp=\d+\;$/i && value !~ /^use /i && value.end_with?(';')
        parse_message(tag, time)
      end
    end
  end

  def parse_message(tag, time)
    record = {}
    date   = nil

    # Skip the message that is output when after flush-logs or restart mysqld.
    # e.g.) /usr/sbin/mysqld, Version: 5.5.28-0ubuntu0.12.04.2-log ((Ubuntu)). started with:
    message = @slowlogs[:"#{tag}"].shift
    while !message.start_with?('#')
      message = @slowlogs[:"#{tag}"].shift
    end

    if message.start_with?('# Time: ')
      date    = Time.parse(message[8..-1].strip)
      message = @slowlogs[:"#{tag}"].shift
    end

    message =~ /^#? User\@Host:\s+(\S+)\s+\@\s+(\S+).*/
    record[:user] = $1
    record[:host] = $2
    message = @slowlogs[:"#{tag}"].shift

    message =~ /^# Query_time: ([0-9.]+)\s+Lock_time: ([0-9.]+)\s+Rows_sent: ([0-9.]+)\s+Rows_examined: ([0-9.]+).*/
    record[:query_time]    = $1.to_f
    record[:lock_time]     = $2.to_f
    record[:rows_sent]     = $3.to_i
    record[:rows_examined] = $4.to_i

    query = []
    @slowlogs[:"#{tag}"].each do |m|
      query << m.strip
    end
    record[:sql] = query.join(' ')

    if date
      flush_emit(tag, date.to_i, record)
    else
      flush_emit(tag, time, record)
    end
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
