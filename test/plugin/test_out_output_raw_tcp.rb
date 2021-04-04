require "helper"
require "fluent/plugin/out_output_raw_tcp.rb"

class OutputRawTcpOutputTest < Test::Unit::TestCase
  setup do
    Fluent::Test.setup
  end

  test "failure" do
    flunk
  end

  private

  def create_driver(conf)
    Fluent::Test::Driver::Output.new(Fluent::Plugin::OutputRawTcpOutput).configure(conf)
  end
end
