# fluent-plugin-mysqlslowquerylog, a plugin for [Fluentd](http://fluentd.org)

## Component

### MySQL Slow Query Log Output

Fluentd plugin to concat MySQL slowquerylog.

## Configuration

Input Messages (Slow Query Log)
```
# Time: 130107 11:36:21
# User@Host: root[root] @ localhost []
# Query_time: 0.000378  Lock_time: 0.000111 Rows_sent: 7  Rows_examined: 7
SET timestamp=1357526181;
select * from user;
# Time: 130107 11:38:47
# User@Host: root[root] @ localhost []
# Query_time: 0.002142  Lock_time: 0.000166 Rows_sent: 142  Rows_examined: 142
use information_schema;
SET timestamp=1357526327;
select * from INNODB_BUFFER_PAGE_LRU;
```

Output Messages
```
2013-01-07T11:36:21+09:00    cocatenated.mysql.slowlog	{"user":"root[root]","host":"localhost","query_time":0.000378,"lock_time":0.000111,"rows_sent":7,"rows_examined":7,"sql":"SET timestamp=1357526181; select * from user;"}
2013-01-07T11:38:47+09:00	cocatenated.mysql.slowlog	{"user":"root[root]","host":"localhost","query_time":0.002142,"lock_time":0.000166,"rows_sent":142,"rows_examined":142,"sql":"use information_schema; SET timestamp=1357526327; select * from INNODB_BUFFER_PAGE_LRU;"}
```

### Example Settings
sender (fluent-agent-lite)
```
TAG_PREFIX="mysql"
LOGS=$(cat <<"EOF"
slowlog.db01 /var/log/mysql/mysql-slow.log
EOF
)
PRIMARY_SERVER="log_server:24224"
```

sender (td-agent)
```
<source>
  type tail
  path   /var/log/mysql/mysql-slow.log
  format /^(?<message>.+)$/
  tag    mysql.slowlog.db01
</source>
<match>
  type forward
  host log_server
</match>
```

reciever
```
<source>
  type forward
</source>
<match mysql.slowlog.*>
  type mysqlslowquerylog
  add_tag_prefix cocatenated.
</match>
<match cocatenated.mysql.slowlog.*>
  type file
  path /tmp/slowtest
</match>
```

## Installation

Add this line to your application's Gemfile:

    gem 'fluent-plugin-mysqlslowquerylog'

And then execute:

    $ bundle

Or install it yourself as:

    $ gem install fluent-plugin-mysqlslowquerylog

## Contributing

1. Fork it
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create new Pull Request

## Copyright

### Copyright
Copyright (c) 2012- Satoshi SUZUKI (@studio3104)

### License
Apache License, Version 2.0
