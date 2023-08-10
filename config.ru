require 'rack'
require 'ox'
require 'typhoeus'
require 'prometheus/client'
require 'prometheus/middleware/collector'
require 'prometheus/middleware/exporter'

BASE = ENV.fetch('ETA_URI')

HEAT_BLOCK = ->(sensor) {
  case  sensor&.attributes.to_h[:strValue]
  when 'Heizen' then 1
  when 'Absenken' then 2
  else
    0
  end
}

SENSOR_MAP = {
  heat_buffer_load_percentage: '/120/10601/0/0/12528',
  heat_buffer_top_temperature: '/120/10601/0/11327/0',
  heat_buffer_high_temperature: '/120/10601/0/11328/0',
  heat_buffer_mid_temperature: '/120/10601/0/11329/0',
  heat_buffer_low_temperature: '/120/10601/0/11330/0',
  heat_buffer_liters_total: '/120/10601/0/0/13520',

  heat_circuit_flow_temperature: '/120/10101/0/11060/0',
  heat_circuit_set_temperature: '/120/10101/0/11125/2120',
  heat_circuit_actual_temperature: '/120/10101/0/11125/2121',
  heat_circuit_position_percentage: '/120/10101/0/11125/2127',
  heat_circuit_mixer_runtime_seconds: ['/120/10101/0/11125/2124', Prometheus::Client::Counter],
  heat_circuit_pump_enabled: ['/120/10101/0/0/19404', Prometheus::Client::Gauge, ->(sensor){ sensor&.attributes.to_h[:strValue] == 'Ein' ? 1 : 0 }], 
  heat_circuit_heating: ['/120/10101/0/0/19404', Prometheus::Client::Gauge, HEAT_BLOCK],

 
  heat_circuit_flow_eg_temperature: '/120/10102/0/11060/0',
  heat_circuit_set_eg_temperature: '/120/10102/0/11125/2120',
  heat_circuit_actual_eg_temperature: '/120/10102/0/11125/2121',
  heat_circuit_position_eg_percentage: '/120/10102/0/11125/2127',
  heat_circuit_mixer_eg_runtime_seconds: ['/120/10102/0/11125/2124', Prometheus::Client::Counter],
  heat_circuit_eg_pump_enabled: ['/120/10102/0/0/19404', Prometheus::Client::Gauge, ->(sensor){ sensor&.attributes.to_h[:strValue] == 'Ein' ? 1 : 0 }], 
  heat_circuit_eg_heating: ['/120/10102/0/0/19404', Prometheus::Client::Gauge, HEAT_BLOCK],

  ambient_temperature: '/120/10601/0/0/12197',

  heat_buffer_fwm: '/79/10531/0/0/12243',
  fwm_primary_return: '/79/10531/0/11186/0',
  fwm_primary_flow: '/79/10531/0/11243/0',
  fwm_temperature: '/79/10531/0/11148/0',
  fwm_flow_rate: '/79/10531/12785/0/0',

  boiler_remaining_oxygen: '/48/10391/0/11108/0',
  boiler_exhaust_gas_temperature: '/48/10391/0/11110/0',
  boiler_temperature: '/48/10391/0/11109/0',
  boiler_return_temperature: '/48/10391/0/11160/0',
  boiler_exhaust_fan_rpm: '/48/10391/0/0/12165',
  boiler_pump_percentage: '/48/10391/0/11123/0',
  boiler_full_duty_runtime_seconds: ['/48/10391/0/0/12153', Prometheus::Client::Counter],
  boiler_exhaust_fan_runtime_seconds: ['/48/10391/0/0/12153', Prometheus::Client::Counter],
  boiler_heat_count: ['/48/10391/0/0/12017', Prometheus::Client::Counter],
  boiler_overheat_count: ['/48/10391/0/0/12540', Prometheus::Client::Counter],
  boiler_fallback_limiter_count: ['/48/10391/0/0/12081', Prometheus::Client::Counter],
  boiler_heat_output_watts: '/48/10391/0/11108/2057',
  boiler_heat_power_ampere: '/48/10391/0/11108/2061',
  boiler_heat_voltage: '/48/10391/0/11108/2069',
  boiler_return_mixer_position_percentage: '/48/10391/0/11163/2127',
  boiler_actuator_top_actual_percentage: '/48/10391/0/11094/2071',
  boiler_actuator_top_set_percentage: '/48/10391/0/11094/2070',
  boiler_actuator_bottom_actual_percentage: '/48/10391/0/11095/2071',
  boiler_actuator_bottom_set_percentage: '/48/10391/0/11095/2070',

  boiler_door_open: ['/48/10391/0/11193/0', Prometheus::Client::Gauge, ->(sensor){ sensor&.attributes.to_h[:strValue] == 'Geschlossen' ? 0 : 1 }], 

  pellets_full_duty_runtime_seconds: ['/40/10401/0/0/12153', Prometheus::Client::Counter],
  pellets_total_usage_kilograms: ['/40/10401/0/0/12016', Prometheus::Client::Counter],
  pellets_storage_loaded_kilograms: '/40/10401/0/0/12011',
  pellets_used_total: '/40/10401/0/0/12017',
  pellets_ignitions_total: '/40/10401/0/0/12018',
  pellets_storage_refill_runtime_seconds: '/40/10401/0/0/12156',
}

$registry = {}

module ETAExporter

  module_function

  def request(path)
    Typhoeus::Request.new([BASE, path].join('/')).run
  end

  def find_or_register(name, type:)
    type = ($registry[name] || type.new(name, docstring: name.to_s))
    yield(type)
    return if $registry[name]

    Prometheus::Client.registry.register(type)
    $registry[name] = type
  end

  def parse(path)
    r = request(path)
    raise "request failed #{r.inspect}" unless r.success?

    Ox.parse(r.body)
  end

  def sensor(uri)
    parse("/user/var#{uri}")
  end

  def sensor_node(uri)
    sensor_node = sensor(uri)&.nodes.to_a[0]&.nodes.to_a[0]
    return if sensor_node.nil?

    sensor_node
  end

  def sensor_value(sensor_node)
    scale = sensor_node.attributes[:scaleFactor].to_f
    value = sensor_node.nodes.first.to_f
    return value if scale.zero?

    value / scale
  end

  def sensors!
    SENSOR_MAP.map do |name, uri|
      uri, type, block = *uri if uri.is_a?(Array)
      type ||= Prometheus::Client::Gauge
      node = sensor_node(uri)
      value = sensor_value(node)
      value = block.call(node) if block
      find_or_register(name, type: type) do |metric|
        metric.instance_variable_get(:@store).set(val: value, labels: {})
      end
    end
  end

  class Rack
    def initialize(app)
      @app = app
    end

    def call(env)
      ETAExporter.sensors!
      @app.call(env)
    end
  end
end

use Rack::Deflater
use ETAExporter::Rack
use Prometheus::Middleware::Collector
use Prometheus::Middleware::Exporter

run ->(_) { [200, {'Content-Type' => 'text/html'}, ['OK']] }
