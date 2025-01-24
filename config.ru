require 'rack'
require 'ox'
require 'typhoeus'
require 'prometheus/client'
require 'prometheus/middleware/collector'
require 'prometheus/middleware/exporter'
require 'json'
require 'mqtt'

BASE = ENV.fetch('ETA_URI')

HEAT_BLOCK = ->(sensor) {
  case  sensor&.attributes.to_h[:strValue]
  when 'Heizen' then 1
  when 'Absenken' then 2
  else
    0
  end
}

DEVICE_ID = "eta-#{Digest::SHA256.hexdigest(BASE)[0...6]}".downcase
DEVICE_INFO = {
  ids: DEVICE_ID,
  mf: 'ETA Heiztechnik GmbH',
  model: 'ETA SH-TWIN 20',
  name: 'Scheitholzkessel',
}

SENSOR_MAP = {
  heat_buffer_load_percentage: [
    '/120/10601/0/0/12528',
    Prometheus::Client::Gauge,
    { unit: '%', device_class: nil, names: { en: 'Heat Buffer Load Percentage', de: 'Wärmespeicher Ladezustand' }}
  ],
  heat_buffer_top_temperature: [
    '/120/10601/0/11327/0',
    Prometheus::Client::Gauge,
    { unit: '°C', device_class: 'temperature', names: { en: 'Heat Buffer Top Temperature', de: 'Wärmespeicher Obere Temperatur' }}
  ],
  heat_buffer_high_temperature: [
    '/120/10601/0/11328/0',
    Prometheus::Client::Gauge,
    { unit: '°C', device_class: 'temperature', names: { en: 'Heat Buffer High Temperature', de: 'Wärmespeicher Hohe Temperatur' }}
  ],
  heat_buffer_mid_temperature: [
    '/120/10601/0/11329/0',
    Prometheus::Client::Gauge,
    { unit: '°C', device_class: 'temperature', names: { en: 'Heat Buffer Mid Temperature', de: 'Wärmespeicher Mittlere Temperatur' }}
  ],
  heat_buffer_low_temperature: [
    '/120/10601/0/11330/0',
    Prometheus::Client::Gauge,
    { unit: '°C', device_class: 'temperature', names: { en: 'Heat Buffer Low Temperature', de: 'Wärmespeicher Niedrige Temperatur' }}
  ],
  heat_buffer_liters_total: [
    '/120/10601/0/0/13520',
    Prometheus::Client::Counter,
    { unit: 'L', device_class: 'volume', names: { en: 'Heat Buffer Total Liters', de: 'Wärmespeicher Gesamtliter' }}
  ],
  heat_circuit_flow_temperature: [
    '/120/10101/0/11060/0',
    Prometheus::Client::Gauge,
    { unit: '°C', device_class: 'temperature', names: { en: 'Heat Circuit Flow Temperature', de: 'Heizkreis Vorlauftemperatur' }}
  ],
  heat_circuit_set_temperature: [
    '/120/10101/0/11125/2120',
    Prometheus::Client::Gauge,
    { unit: '°C', device_class: 'temperature', names: { en: 'Heat Circuit Set Temperature', de: 'Heizkreis Solltemperatur' }}
  ],
  heat_circuit_actual_temperature: [
    '/120/10101/0/11125/2121',
    Prometheus::Client::Gauge,
    { unit: '°C', device_class: 'temperature', names: { en: 'Heat Circuit Actual Temperature', de: 'Heizkreis Isttemperatur' }}
  ],
  heat_circuit_position_percentage: [
    '/120/10101/0/11125/2127',
    Prometheus::Client::Gauge,
    { unit: '%', device_class: nil, names: { en: 'Heat Circuit Position Percentage', de: 'Heizkreis Positionsprozentsatz' }}
  ],
  heat_circuit_mixer_runtime_seconds: [
    '/120/10101/0/11125/2124',
    Prometheus::Client::Counter,
    { unit: 's', device_class: 'duration', names: { en: 'Heat Circuit Mixer Runtime', de: 'Heizkreis Mischer Laufzeit' }}
  ],
  heat_circuit_pump_enabled: [
    '/120/10101/0/0/19404',
    Prometheus::Client::Gauge,
    { unit: 'boolean', device_class: nil, names: { en: 'Heat Circuit Pump', de: 'Heizkreis Pumpe' }, processing: ->(sensor){ sensor&.attributes.to_h[:strValue] == 'Ein' ? 1 : 0 }}
  ],
  heat_circuit_heating: [
    '/120/10101/0/0/19404',
    Prometheus::Client::Gauge,
    { unit: 'mode', device_class: nil, names: { en: 'Heat Circuit Heating', de: 'Heizkreis Heizung' }, processing: HEAT_BLOCK}
  ],
  heat_circuit_flow_eg_temperature: [
    '/120/10102/0/11060/0',
    Prometheus::Client::Gauge,
    { unit: '°C', device_class: 'temperature', names: { en: 'EG Flow Temperature', de: 'EG Vorlauftemperatur' }}
  ],
  heat_circuit_set_eg_temperature: [
    '/120/10102/0/11125/2120',
    Prometheus::Client::Gauge,
    { unit: '°C', device_class: 'temperature', names: { en: 'EG Set Temperature', de: 'EG Solltemperatur' }}
  ],
  heat_circuit_actual_eg_temperature: [
    '/120/10102/0/11125/2121',
    Prometheus::Client::Gauge,
    { unit: '°C', device_class: 'temperature', names: { en: 'EG Actual Temperature', de: 'EG Isttemperatur' }}
  ],
  heat_circuit_position_eg_percentage: [
    '/120/10102/0/11125/2127',
    Prometheus::Client::Gauge,
    { unit: '%', device_class: nil, names: { en: 'EG Position Percentage', de: 'EG Positionsprozentsatz' }}
  ],
  heat_circuit_mixer_eg_runtime_seconds: [
    '/120/10102/0/11125/2124',
    Prometheus::Client::Counter,
    { unit: 's', device_class: 'duration', names: { en: 'EG Mixer Runtime', de: 'EG Mischer Laufzeit' }}
  ],
  heat_circuit_eg_pump_enabled: [
    '/120/10102/0/0/19404',
    Prometheus::Client::Gauge,
    { unit: 'boolean', device_class: nil, names: { en: 'EG Pump', de: 'EG Pumpe' }, processing: ->(sensor){ sensor&.attributes.to_h[:strValue] == 'Ein' ? 1 : 0 }}
  ],
  heat_circuit_eg_heating: [
    '/120/10102/0/0/19404',
    Prometheus::Client::Gauge,
    { unit: 'mode', device_class: nil, names: { en: 'EG Heating', de: 'EG Heizung' }, processing: HEAT_BLOCK }
  ],
  ambient_temperature: [
    '/120/10601/0/0/12197',
    Prometheus::Client::Gauge,
    { unit: '°C', device_class: 'temperature', names: { en: 'Ambient Temperature', de: 'Umgebungstemperatur' }}
  ],
  heat_buffer_fwm: [
    '/79/10531/0/0/12243',
    Prometheus::Client::Gauge,
    { unit: '%', device_class: nil, names: { en: 'FWM', de: 'FWM' }}
  ],
  fwm_primary_return: [
    '/79/10531/0/11186/0',
    Prometheus::Client::Gauge,
    { unit: '°C', device_class: 'temperature', names: { en: 'FWM Primary Return', de: 'FWM primär Rücklauf' }}
  ],
  fwm_primary_flow: [
    '/79/10531/0/11243/0',
    Prometheus::Client::Gauge,
    { unit: '°C', device_class: 'temperature', names: { en: 'FWM Primary Flow', de: 'FWM primär Fluss' }}
  ],
  fwm_temperature: [
    '/79/10531/0/11148/0',
    Prometheus::Client::Gauge,
    { unit: '°C', device_class: 'temperature', names: { en: 'FWM Temperature', de: 'FWM Temperatur' }}
  ],
  fwm_flow_rate: [
    '/79/10531/12785/0/0',
    Prometheus::Client::Gauge,
    { unit: 'L/min', device_class: nil, names: { en: 'FWM Flow Rate', de: 'FWM Flussrate' }}
  ],
  boiler_remaining_oxygen: [
    '/48/10391/0/11108/0',
    Prometheus::Client::Gauge,
    { unit: '%', device_class: nil, names: { en: 'Boiler Remaining Oxygen', de: 'Kessel Rest-Sauerstoff' }}
  ],
  boiler_exhaust_gas_temperature: [
    '/48/10391/0/11110/0',
    Prometheus::Client::Gauge,
    { unit: '°C', device_class: 'temperature', names: { en: 'Boiler Exhaust Gas Temperature', de: 'Kessel Abgastemperatur' }}
  ],
  boiler_temperature: [
    '/48/10391/0/11109/0',
    Prometheus::Client::Gauge,
    { unit: '°C', device_class: 'temperature', names: { en: 'Boiler Temperature', de: 'Kessel Temperatur' }}
  ],
  boiler_return_temperature: [
    '/48/10391/0/11160/0',
    Prometheus::Client::Gauge,
    { unit: '°C', device_class: 'temperature', names: { en: 'Boiler Return Temperature', de: 'Kessel Rücklauftemperatur' }}
  ],
  boiler_exhaust_fan_rpm: [
    '/48/10391/0/0/12165',
    Prometheus::Client::Gauge,
    { unit: 'rpm', device_class: nil, names: { en: 'Boiler Exhaust Fan RPM', de: 'Kessel Abluftventilator U/min' }}
  ],
  boiler_pump_percentage: [
    '/48/10391/0/11123/0',
    Prometheus::Client::Gauge,
    { unit: '%', device_class: nil, names: { en: 'Boiler Pump Percentage', de: 'Kessel Pumpenprozent' }}
  ],
  boiler_full_duty_runtime_seconds: [
    '/48/10391/0/0/12153',
    Prometheus::Client::Counter,
    { unit: 's', device_class: 'duration', names: { en: 'Boiler Full Duty Runtime', de: 'Kessel Vollbetrieb Laufzeit' }}
  ],
  boiler_exhaust_fan_runtime_seconds: [
    '/48/10391/0/0/12153',
    Prometheus::Client::Counter,
    { unit: 's', device_class: 'duration', names: { en: 'Boiler Exhaust Fan Runtime', de: 'Kessel Abluftventilator Laufzeit' }}
  ],
  boiler_heat_count: [
    '/48/10391/0/0/12017',
    Prometheus::Client::Counter,
    { unit: 'count', device_class: nil, names: { en: 'Boiler Heat Count', de: 'Kessel Heizzyklen' }}
  ],
  boiler_overheat_count: [
    '/48/10391/0/0/12540',
    Prometheus::Client::Counter,
    { unit: 'count', device_class: nil, names: { en: 'Boiler Overheat Count', de: 'Kessel Überhitzungszyklen' }}
  ],
  boiler_fallback_limiter_count: [
    '/48/10391/0/0/12081',
    Prometheus::Client::Counter,
    { unit: 'count', device_class: nil, names: { en: 'Boiler Fallback Limiter Count', de: 'Kessel Fallback-Limiter Zyklen' }}
  ],
  boiler_heat_output_watts: [
    '/48/10391/0/11108/2057',
    Prometheus::Client::Gauge,
    { unit: 'W', device_class: 'power', names: { en: 'Boiler Heat Output', de: 'Kessel Heizleistung' }}
  ],
  boiler_heat_power_ampere: [
    '/48/10391/0/11108/2061',
    Prometheus::Client::Gauge,
    { unit: 'A', device_class: 'current', names: { en: 'Boiler Heat Current', de: 'Kessel Heizstrom' }}
  ],
  boiler_heat_voltage: [
    '/48/10391/0/11108/2069',
    Prometheus::Client::Gauge,
    { unit: 'V', device_class: 'voltage', names: { en: 'Boiler Heat Voltage', de: 'Kessel Heizspannung' }}
  ],
  boiler_return_mixer_position_percentage: [
    '/48/10391/0/11163/2127',
    Prometheus::Client::Gauge,
    { unit: '%', device_class: nil, names: { en: 'Boiler Return Mixer Position', de: 'Kessel Rücklaufmischerposition' }}
  ],
  boiler_actuator_top_actual_percentage: [
    '/48/10391/0/11094/2071',
    Prometheus::Client::Gauge,
    { unit: '%', device_class: nil, names: { en: 'Boiler Top Actuator Position', de: 'Kessel Oberer Aktuatorposition' }}
  ],
  boiler_actuator_top_set_percentage: [
    '/48/10391/0/11094/2070',
    Prometheus::Client::Gauge,
    { unit: '%', device_class: nil, names: { en: 'Boiler Top Actuator Set Position', de: 'Kessel Oberer Aktuator Sollposition' }}
  ],
  boiler_actuator_bottom_actual_percentage: [
    '/48/10391/0/11095/2071',
    Prometheus::Client::Gauge,
    { unit: '%', device_class: nil, names: { en: 'Boiler Bottom Actuator Position', de: 'Kessel Unterer Aktuatorposition' }}
  ],
  boiler_actuator_bottom_set_percentage: [
    '/48/10391/0/11095/2070',
    Prometheus::Client::Gauge,
    { unit: '%', device_class: nil, names: { en: 'Boiler Bottom Actuator Set Position', de: 'Kessel Unterer Aktuator Sollposition' }}
  ],
  boiler_door_open: [
    '/48/10391/0/11193/0',
    Prometheus::Client::Gauge,
    { unit: 'boolean', device_class: nil, names: { en: 'Boiler Door Open', de: 'Kessel Tür Offen' }, processing: ->(sensor){ sensor&.attributes.to_h[:strValue] == 'Geschlossen' ? 0 : 1 }}
  ],
  pellets_full_duty_runtime_seconds: [
    '/40/10401/0/0/12153',
    Prometheus::Client::Counter,
    { unit: 's', device_class: 'duration', names: { en: 'Pellets Full Duty Runtime', de: 'Pellets Vollbetrieb Laufzeit' }}
  ],
  pellets_total_usage_kilograms: [
    '/40/10401/0/0/12016',
    Prometheus::Client::Counter,
    { unit: 'kg', device_class: 'weight', names: { en: 'Pellets Total Usage', de: 'Pellets Gesamtverbrauch' }}
  ],
  pellets_storage_loaded_kilograms: [
    '/40/10401/0/0/12011',
    Prometheus::Client::Gauge,
    { unit: 'kg', device_class: 'weight', names: { en: 'Pellets Storage Loaded', de: 'Pellets Lagerbestand' }}
  ],
  pellets_used_total: [
    '/40/10401/0/0/12017',
    Prometheus::Client::Counter,
    { unit: 'kg', device_class: 'weight', names: { en: 'Pellets Used Total', de: 'Pellets Gesamtverbrauch' }}
  ],
  pellets_ignitions_total: [
    '/40/10401/0/0/12018',
    Prometheus::Client::Counter,
    { unit: 'count', device_class: nil, names: { en: 'Pellet Ignitions Total', de: 'Pellets Zündungen Gesamt' }}
  ],
  pellets_storage_refill_runtime_seconds: [
    '/40/10401/0/0/12156',
    Prometheus::Client::Counter,
    { unit: 's', device_class: 'duration', names: { en: 'Pellets Storage Refill Runtime', de: 'Pellets Lager Neubefüllungszeit' }}
  ],
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

  def mqtt_client
    topic = "homeassistant/sensor/#{DEVICE_ID}/availability"
    @mqtt_client ||= MQTT::Client.connect(
      host: ENV.fetch("MQTT_HOST"),
      port: ENV.fetch("MQTT_PORT", 1883).to_i,
      will_topic: topic,
      will_payload: "offline",
      will_retain: true
    ).tap do |client|
      client.publish(topic, "online", true)
    end
  end

  def publish_mqtt_config(name, type, sensor_info, language = :de)
    config_payload = {
      uniq_id: "#{DEVICE_ID}-#{name}", 
      object_id: sensor_info[:names][:en],
      name: sensor_info[:names][language],
      stat_t: "homeassistant/sensor/#{DEVICE_ID}/#{name}/state",
      unit_of_meas: sensor_info[:unit],
      dev_cla: sensor_info[:device_class],
      stat_cla: type == Prometheus::Client::Gauge ? 'measurement' : 'total_increasing',
      availability_topic: "homeassistant/sensor/#{DEVICE_ID}/availability",
      exp_after: 300,
      dev: DEVICE_INFO,
    }
    if sensor_info[:device_class].nil?
      config_payload.delete(:dev_cla)
      config_payload[:unit_of_meas] = nil
    end

    mqtt_client.publish(
      "homeassistant/sensor/#{DEVICE_ID}/#{name}/config", config_payload.to_json, false)
  end

  def publish_mqtt_state(name, value)
    return unless mqtt?

    mqtt_client.publish("homeassistant/sensor/#{DEVICE_ID}/#{name}/state", value.to_s, false)
  end

  def mqtt?
    ENV.key? 'MQTT_HOST'
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
      uri, type, data = *uri if uri.is_a?(Array)
      block = data[:processing]
      type ||= Prometheus::Client::Gauge
      node = sensor_node(uri)
      value = sensor_value(node)
      value = block.call(node) if block
      find_or_register(name, type: type) do |metric|
        metric.instance_variable_get(:@store).set(val: value, labels: {})
      end
      publish_mqtt_state(name, value)
    end
  end

  def init_mqtt!
    return unless mqtt?

    SENSOR_MAP.map do |name, uri|
      _uri, type, data = *uri if uri.is_a?(Array)
      publish_mqtt_config(name, type, data)
    end
  end

  class Rack
    def initialize(app)
      @app = app
      Thread.new do
        loop do
          ETAExporter.init_mqtt!
          sleep(90)
        end
      end
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
