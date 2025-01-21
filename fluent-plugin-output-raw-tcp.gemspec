require_relative 'lib/fluent/plugin/version'

Gem::Specification.new do |spec|
  spec.name    = "fluent-plugin-output-raw-tcp"
  spec.version = "0.1.0"
  spec.authors = ["Yudai Hashimoto"]
  spec.email   = ["jp7fkf@gmail.com"]

  spec.summary       = %q{fluentd output plugin for TCP output.}
  spec.description   = %q{fluentd output plugin for TCP output..}
  spec.homepage      = "https://github.com/jp7fkf/fluent-plugin-output-raw-tcp"
  spec.license       = "Apache-2.0"
  spec.required_ruby_version = Gem::Requirement.new(">= 2.3.0")

  spec.metadata["allowed_push_host"] = "TODO: Set to 'http://mygemserver.com'"

  spec.metadata["homepage_uri"] = spec.homepage
  spec.metadata["source_code_uri"] = "https://github.com/jp7fkf/fluent-plugin-output-raw-tcp"
  # spec.metadata["changelog_uri"] = "TODO: Put your gem's CHANGELOG.md URL here."

  # Specify which files should be added to the gem when it is released.
  # The `git ls-files -z` loads the files in the RubyGem that have been added into git.
  spec.files         = Dir.chdir(File.expand_path('..', __FILE__)) do
    `git ls-files -z`.split("\x0").reject { |f| f.match(%r{^(test|spec|features)/}) }
  end
  spec.bindir        = "exe"
  spec.executables   = spec.files.grep(%r{^exe/}) { |f| File.basename(f) }
  spec.require_paths = ["lib"]
end
