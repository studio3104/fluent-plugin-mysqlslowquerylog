# -*- encoding: utf-8 -*-

Gem::Specification.new do |gem|
  gem.name          = "fluent-plugin-mysqlslowquerylog"
  gem.version       = "0.0.4"
  gem.authors       = ["Satoshi SUZUKI", "traxo-xx"]
  gem.email         = ["studio3104.com@gmail.com"]
  gem.description   = %q{Fluentd plugin to concat MySQL slowquerylog. This is a modified Verion that also supports Percona DB.}
  gem.summary       = %q{Fluentd plugin to concat MySQL slowquerylog.}
  gem.homepage      = "https://github.com/traxo-xx/fluent-plugin-mysqlslowquerylog"
  gem.license     = 'Apache License, Version 2.0'

  gem.files         = `git ls-files`.split($/)
  gem.executables   = gem.files.grep(%r{^bin/}).map{ |f| File.basename(f) }
  gem.test_files    = gem.files.grep(%r{^(test|spec|features)/})
  gem.require_paths = ["lib"]

  gem.add_runtime_dependency "fluentd"
end