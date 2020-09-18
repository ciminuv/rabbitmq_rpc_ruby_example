require 'bunny'
require 'thread'
require 'json'

class RpcClient
  attr_accessor :call_id, :response, :lock, :condition,
                :channel, :server_queue_name, :reply_queue, :exchange

  def initialize(exchange, server_queue_name)
    amqp_url = ENV['AMQP_URL']
    raise "AMQP_URL environment variable is required!" unless amqp_url
    connection = Bunny.new(amqp_url)
    connection.start

    @channel = connection.create_channel
    @exchange = channel.direct(exchange, durable: true)
    @server_queue_name = server_queue_name

    setup_reply_queue
  end

  def call(record_id)
    @call_id = generate_uuid

    exchange.publish(JSON.generate({id: record_id}),
                     routing_key: server_queue_name,
                     correlation_id: call_id,
                     reply_to: reply_queue.name,
                     content_type: 'application/json')

    # wait for the signal to continue the execution
    lock.synchronize { condition.wait(lock) }

    response
  end

  private

  def setup_reply_queue
    @lock = Mutex.new
    @condition = ConditionVariable.new
    that = self
    reply_queue_name = "#{server_queue_name}.response.#{rand}"
    @reply_queue = channel.queue(reply_queue_name, exclusive: true)
    @reply_queue.bind(exchange, routing_key: reply_queue_name)

    reply_queue.subscribe do |_delivery_info, properties, payload|
      if properties[:correlation_id] == that.call_id
        that.response = JSON.parse(payload)

        # sends the signal to continue the execution of #call
        that.lock.synchronize { that.condition.signal }
      end
    end
  end

  def generate_uuid
    # very naive but good enough for code examples
    "#{rand}#{rand}#{rand}"
  end
end
