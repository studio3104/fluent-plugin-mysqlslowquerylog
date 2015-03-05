class Fluent::MySQLSlowQueryLogOutput < Fluent::Output
  Fluent::Plugin.register_output('mysqlslowquerylog', self)
  include Fluent::HandleTagNameMixin

  def configure(conf)
    super
    @slowlogs = {}

    if !@remove_tag_prefix && !@remove_tag_suffix && !@add_tag_prefix && !@add_tag_suffix
      raise ConfigError, "out_myslowquerylog: At least one of option, remove_tag_prefix, remove_tag_suffix, add_tag_prefix or add_tag_suffix is required to be set."
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

  REGEX1 = /^#? User\@Host:\s+(\S+)\[(\S+)\]\s+\@\s+((\S+)\s+)*\[(\S+)\](\s+Id:\s+(\d+))*.*/
  REGEX2 = /^#? (Thread_id:\s+(\d+)\s+)*Schema:\s+(\S+)\s+Last_errno:\s+(\d+)\s+Killed:\s+(\d+).*/
  REGEX3 = /^#? Query_time:\s+([0-9.]+)\s+Lock_time:\s+([0-9.]+)\s+Rows_sent:\s+([0-9.]+)\s+Rows_examined:\s+([0-9.]+)(\s+Rows_affected:\s+([0-9.]+))*(\s+Rows_read:\s+([0-9.]+))*.*/
  REGEX4 = /^#? Bytes_sent:\s+(\d+)\s+Tmp_tables:\s+(\d+)\s+Tmp_disk_tables:\s+(\d+)\s+Tmp_table_sizes:\s+(\d+).*/
  REGEX5 = /^#? Stored\sroutine:\s+(\S+).*/
  REGEX6 = /^#? InnoDB_trx_id:\s+(\S+).*/
  REGEX7 = /^#? QC_Hit:\s+([a-zA-Z]+)\s+Full_scan:\s+([a-zA-Z]+)\s+Full_join:\s+([a-zA-Z]+)\s+Tmp_table:\s+([a-zA-Z]+)\s+Tmp_table_on_disk:\s+([a-zA-Z]+).*/
  REGEX8 = /^#? Filesort:\s+([a-zA-Z]+)\s+Filesort_on_disk:\s+([a-zA-Z]+)\s+Merge_passes:\s+(\d+).*/
  REGEX9 = /^#?\s+InnoDB_IO_r_ops:\s+(\d+)\s+InnoDB_IO_r_bytes:\s+(\d+)\s+InnoDB_IO_r_wait:\s+(\d+\.\d+).*/
  REGEX10 = /^#?\s+InnoDB_rec_lock_wait:\s+(\d+\.\d+)\s+InnoDB_queue_wait:\s+(\d+\.\d+).*/
  REGEX11 = /^#?\s+InnoDB_pages_distinct:\s+(\d+).*/
  REGEX12 = /^#?\s+Log_slow_rate_type:\s(\S+)\s+Log_slow_rate_limit:\s+(\d+).*/
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
    record['user_second'] = $2
    record['hostname'] = $4
    record['client_ip'] = $5
    if $6.to_s != ''
      record['thread_id'] = $6.to_i
    end
    message = @slowlogs[:"#{tag}"].shift

    if message.include?('Thread_id') || message.include?('Schema')
      message =~ REGEX2
      if $2.to_s != ''
        record['thread_id'] = $2.to_i
      end
      record['schema'] = $3
      record['last_errno'] = $4.to_i
      record['killed'] = $5.to_i
      message = @slowlogs[:"#{tag}"].shift
    end

    if message.include?('Query_time')
      message =~ REGEX3
      record['query_time']    = $1.to_f
      record['lock_time']     = $2.to_f
      record['rows_sent']     = $3.to_i
      record['rows_examined'] = $4.to_i
      if $6.to_s != ''
        record['rows_affected'] = $6.to_i
      end
      if $8.to_s != ''
        record['rows_read'] = $8.to_i
      end
      message = @slowlogs[:"#{tag}"].shift
    end

    if message.include?('Bytes_sent')
      message =~ REGEX4
      record['bytes_sent'] = $1.to_i
      record['tmp_tables'] = $2.to_i
      record['tmp_disk_tables'] = $3.to_i
      record['tmp_table_sizes'] = $4.to_i
      message = @slowlogs[:"#{tag}"].shift
    end

    if message.include?('Stored')
      message =~ REGEX5
      record['stored_routine'] = $1
      message = @slowlogs[:"#{tag}"].shift
    end

    if message.include?('InnoDB_trx_id')
      message =~ REGEX6
      record['innoDB_trx_id'] = $1
      message = @slowlogs[:"#{tag}"].shift
    end

    if message.include?('QC_Hit')
      message =~ REGEX7
      record['qc_hit'] = $1
      record['full_scan'] = $2
      record['full_join'] = $3
      record['tmp_table'] = $4
      record['tmp_table_on_disk'] = $5
      message = @slowlogs[:"#{tag}"].shift
    end

    if message.include?('Filesort')
      message =~ REGEX8
      record['filesort'] = $1
      record['filesort_on_disk'] = $2
      record['merge_passes'] = $3
      message = @slowlogs[:"#{tag}"].shift
    end

    if message.include?('InnoDB_IO_r_ops')
      message =~ REGEX9
      record['innoDB_io_r_ops'] = $1.to_i
      record['innoDB_io_r_bytes'] = $2.to_i
      record['innoDB_io_r_wait'] = $3.to_f
      message = @slowlogs[:"#{tag}"].shift
    end

    if message.include?('InnoDB_rec_lock_wait')
      message =~ REGEX10
      record['innodb_rec_lock_wait'] = $1.to_f
      record['innodb_queue_wait'] = $2.to_f
      message = @slowlogs[:"#{tag}"].shift
    end

    if message.include?('InnoDB_pages_distinct')
      message =~ REGEX11
      record['innodb_pages_distinct'] = $1.to_i
      message = @slowlogs[:"#{tag}"].shift
    end

    if message.include?('Log_slow_rate_type')
      message =~ REGEX12
      record['Log_slow_rate_type'] = $1
      record['Log_slow_rate_limit'] = $2.to_i
      message = @slowlogs[:"#{tag}"].shift
    end

    record['sql'] = @slowlogs[:"#{tag}"].map {|m| m.strip}.join(' ')

    time = date.to_i if date
    flush_emit(tag, time, record)
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
