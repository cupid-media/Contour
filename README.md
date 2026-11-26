 ![logo](https://cloud.githubusercontent.com/assets/888475/19428671/00fd0fb8-9454-11e6-8d70-a9355ea3bbda.png) Contour
========

## About

Contour is the message bus implementation which provides communication between .NET services via different transport protocols.

**Contour builds all the topology (exchanges, queues with all properties set up) for you.** It supports two transports:

* **AMQP/RabbitMQ** — full duplex bus: publish/subscribe, request/response, topology building, fault queues (`Contour.Transport.RabbitMQ`).
* **Kafka** — full `IBus`: topic listeners with graceful shutdown, manual offset checkpointing and rebalance-aware commits, publishing through `EmitAsync`, and request/response over a reply topic (`Contour.Transport.Kafka`).

## Configuration

Contour supports two main configuration approaches:

1. **Code-based configuration** - Using fluent API with `RabbitBusConfigurator` / `KafkaBusConfigurator`
2. **IEndpoint configuration** - Using app.config files

### Configuring via C# Code 

The modern approach uses fluent API through `BusFactory` and `RabbitBusConfigurator`:

#### Basic Usage

```csharp
using Contour;
using Contour.Transport.RabbitMQ.Configuration;

// Create and start a bus with RabbitMQ transport
var bus = await BusFactory.CreateRmqBusAsync(cfg =>
{
    cfg.WithEndpointName("my-service");
    cfg.WithConnectionString("amqp://localhost:5672/");
    
    // Configure outgoing routes
    cfg.Route("user.created")
        .WithConfirmation()
        .Persistently()
        .WithTtl(TimeSpan.FromMinutes(30));
    
    // Configure incoming message handlers  
    cfg.On<UserCreatedEvent>("user.created")
        .ReactWith(new UserCreatedHandler())
        .RequiresAccept();
});

// Bus is automatically started with CreateRmqBusAsync
```

#### Advanced Configuration

```csharp
var bus = await BusFactory.CreateRmqBusAsync(cfg =>
{
    // Endpoint configuration
    cfg.WithEndpointName("payment-service");
    cfg.WithConnectionString("amqp://user:pass@localhost:5672/vhost");
    
    // Connection settings
    cfg.WithReuseConnection(true);
    cfg.WithFailoverAttempts(5);
    cfg.WithParallelismLevel(4);
    
    // Message processing
    cfg.WithPayloadConverter(new JsonPayloadConverter());
    cfg.WithBusLifecycleHandler(new CustomLifecycleHandler());
    
    // Quality of Service
    cfg.WithQoS(prefetchCount: 10);
    
    // Fault tolerance
    cfg.WithFaultQueueTtl(TimeSpan.FromDays(7));
    cfg.WithExcludedHeaders(new[] { "x-custom-header" });
    
    // Routes with request/response pattern
    cfg.Route("payment.process")
        .WithRequestTimeout(TimeSpan.FromSeconds(30))
        .WithDefaultCallbackEndpoint();
    
    // Multiple message handlers
    cfg.On<PaymentRequested>("payment.requested")
        .ReactWith(new PaymentHandler())
        .WithParallelismLevel(2)
        .RequiresAccept();
        
    cfg.On<PaymentCompleted>("payment.completed")
        .ReactWith(new NotificationHandler());
});
```

#### Fault Tolerance and High Availability

```csharp
var bus = await BusFactory.CreateRmqBusAsync(cfg =>
{
    cfg.WithEndpointName("resilient-service");
    
    // Multiple broker URLs for failover
    cfg.WithConnectionString("amqp://broker1:5672,amqp://broker2:5672,amqp://broker3:5672");
    
    // Connection pooling and reuse
    cfg.WithReuseConnection(true);
    cfg.WithFailoverAttempts(10);
    
    // Custom connection string provider for service discovery
    cfg.WithConnectionStringProvider(new ConsulConnectionStringProvider());
});
```

#### Dynamic Message Routing

```csharp
var bus = await BusFactory.CreateRmqBusAsync(cfg =>
{
    cfg.WithEndpointName("dynamic-sender");
    cfg.WithConnectionString("amqp://localhost:5672/");
    
    // Enable dynamic routing for unknown message types
    cfg.EnableDynamicRouting();
});

// Send to any label at runtime
await bus.EmitAsync("runtime.determined.label", new { Data = "value" });
```

#### Dependency Injection Integration

```csharp
// In your DI container setup (e.g., ASP.NET Core)
services.AddSingleton<IBus>(provider =>
{
    return BusFactory.CreateRmqBusAsync(cfg =>
    {
        cfg.WithEndpointName("web-api");
        cfg.WithConnectionString(configuration.GetConnectionString("RabbitMQ"));
        
        // Use DI container for handler resolution
        cfg.WithHandlerResolver(type => provider.GetService(type));
        
        cfg.On<OrderCreated>("order.created")
            .ReactWith<OrderCreatedHandler>() // Resolved from DI
            .RequiresAccept();
    }).Result;
});
```

### Message Handler Examples

```csharp
public class OrderCreatedHandler : IConsumerOf<OrderCreated>
{
    public async Task Consume(IConsumedMessage<OrderCreated> message)
    {
        var order = message.Payload;
        
        // Process the order
        await ProcessOrder(order);
        
        // Acknowledge message processing
        message.Accept();
        
        // Send follow-up messages
        await message.Bus.EmitAsync("order.processed", new OrderProcessed 
        { 
            OrderId = order.Id 
        });
    }
}
```

### Configuration via EndpointConfiguration

For scenarios where you need programmatic configuration without hardcoded values, Contour provides `EndpointConfiguration` class that can be used with `RabbitBusConfigurationExtensions`:

#### Basic Usage with EndpointConfiguration

```csharp
using Contour.Configuration;
using Contour.Transport.RabbitMQ.Configuration;

// Create configuration object
var endpointConfig = new EndpointConfiguration
{
    Name = "my-service",
    ConnectionString = "amqp://localhost:5672/",
    ParallelismLevel = 4,
    ReuseConnection = true,
    FaultQueueTtl = TimeSpan.FromDays(7),
    FaultQueueLimit = 1000,
    ExcludedHeaders = new[] { "x-internal-header" }
};

// Apply configuration using extension method
var bus = await BusFactory.CreateRmqBusAsync(cfg =>
{
    cfg.Configure(endpointConfig, (name, type) => serviceProvider.GetService(type));
    
    // Add additional configuration if needed
    cfg.Route("additional.route")
        .Persistently();
});
```

#### Loading Configuration from External Sources

```csharp
// From JSON configuration file
var configuration = builder.Configuration;
var endpointConfig = configuration.GetSection("MessageBus").Get<EndpointConfiguration>();

// From environment variables
var endpointConfig = new EndpointConfiguration
{
    Name = Environment.GetEnvironmentVariable("SERVICE_NAME") ?? "default-service",
    ConnectionString = Environment.GetEnvironmentVariable("RABBITMQ_CONNECTION") ?? "amqp://localhost:5672/",
    ParallelismLevel = uint.Parse(Environment.GetEnvironmentVariable("PARALLELISM_LEVEL") ?? "1")
};

var bus = await BusFactory.CreateRmqBusAsync(cfg =>
{
    cfg.Configure(endpointConfig, dependencyResolver);
});
```

#### ASP.NET Core Integration with appsettings.json

```json
{
  "MessageBus": {
    "Name": "web-api",
    "ConnectionString": "amqp://user:password@rabbitmq-server:5672/",
    "ParallelismLevel": 4,
    "ReuseConnection": true,
    "FaultQueueTtl": "7.00:00:00",
    "FaultQueueLimit": 1000,
    "ExcludedHeaders": ["x-trace-id", "x-internal"]
  }
}
```

```csharp
// In Program.cs
builder.Services.Configure<EndpointConfiguration>(builder.Configuration.GetSection("MessageBus"));

builder.Services.AddSingleton<IBus>(provider =>
{
    var endpointConfig = provider.GetRequiredService<IOptions<EndpointConfiguration>>().Value;
    var dependencyResolver = (name, type) => provider.GetService(type);
    
    return BusFactory.CreateRmqBusAsync(cfg =>
    {
        cfg.Configure(endpointConfig, dependencyResolver);
        
        // Additional runtime configuration
        cfg.On<OrderCreated>("order.created")
            .ReactWith<OrderCreatedHandler>()
            .RequiresAccept();
    }).GetAwaiter().GetResult();
});
```

## Kafka transport

The Kafka transport implements the same `IBus` interface as RabbitMQ: it consumes from topics,
publishes through `EmitAsync`, and shares Contour's infrastructure (`AbstractBus`, `BusState`,
`IPayloadConverter`, `IBusComponent`, `IDependencyResolver`, the `x-*` header conventions,
`LogManager`). It lives in `Contour.Transport.Kafka` and there is no Kafka-specific bus interface —
`BusFactory.CreateKafkaBus` returns a plain `IBus`.

Request/response works too, over a reply topic and correlation ids — see below for how it is built and
what that costs.

Configuration follows the same shape as RabbitMQ: `BusFactory.CreateKafkaBus` hands you a
`KafkaBusConfigurator`, where `On(topic)` declares a receiver and `Route(topic)` declares a
publishing route.

### Basic usage

```csharp
using Contour;
using Contour.Transport.Kafka;

// Optional: route library logs into your ILoggerFactory (NullLogger is used otherwise)
BusFactory.UseLogger(loggerFactory);

IBus bus = await BusFactory.CreateKafkaBus(cfg =>
{
    cfg.WithEndpointName("my-service")
        .WithBootstrapServers("kafka-1:9092,kafka-2:9092")
        .WithGroupId("my-service")
        .WithParallelismLevel(4);           // 4 workers per topic, each with its own consumer

    // Publishing
    cfg.Route("user.updated");

    // Consuming
    cfg.On("user.created")
        .ReactWith(new UserCreatedHandler());
});

await bus.EmitAsync("user.updated", new UserUpdatedEvent { Id = 42 });
```

The bus is started automatically; set `cfg.AutoStart = false` to start it yourself later.
`WithPayloadConverter` is optional — `JsonPayloadConverter` is used by default. The endpoint name
lands in the `x-breadcrumbs` header of every emitted message.

A handler implements `IConsumerOf<T>` — the same interface as for RabbitMQ — and `ReactWith` infers
the payload type from it:

```csharp
public class UserCreatedHandler : IConsumerOf<UserCreatedEvent>
{
    public Task HandleAsync(IConsumingContext<UserCreatedEvent> context)
    {
        // context.Message.Label (the topic), context.Message.Payload, context.Message.Headers
        return Task.CompletedTask;
    }
}
```

### Publishing

`Route(topic)` returns a `KafkaSenderConfigurator`. The route's label is the topic name, so
`EmitAsync("user.updated", payload)` publishes to `user.updated`:

```csharp
cfg.Route("user.updated")
    .WithAcks(Acks.All)                              // wait for all in-sync replicas
    .WithIdempotence()                               // broker drops duplicates on retry
    .WithCompression(CompressionType.Lz4)
    .WithLinger(TimeSpan.FromMilliseconds(20))       // batch before sending
    .WithConfirmation(timeout: TimeSpan.FromSeconds(10))
    .WithFailoverAttempts(3);
```

Setting | Default | Description
------- | ------- | -----------
`WithConfirmation(required, timeout)` | required, no timeout | Await the broker's delivery report. With `required: false` the call returns as soon as the message is buffered.
`WithFailoverAttempts(n)` | 3 | How many times to retry a message whose delivery failed.
`WithFlushTimeout(t)` | 10 s | How long the producer gets to drain its buffer when the bus stops.
`WithAcks` / `WithIdempotence` / `WithCompression` / `WithLinger` / `WithMessageTimeout` | librdkafka defaults | Standard producer settings.
`WithProducerProperty(key, value)` | — | Escape hatch for any other librdkafka setting.

There is deliberately no `Persistently()`, `WithTtl()` or `Delayed()` on a Kafka route: those set the
`x-persist` / `x-ttl` / `x-delay` headers, which Kafka does not interpret — persistence and retention
are topic settings, and there is no delayed delivery at all. `PublishingOptions` still carries the
corresponding per-call fields, and they still only produce headers.

Contour's headers are applied on every emit exactly as for RabbitMQ — `x-correlation-id`,
`x-breadcrumbs`, `x-original-message-id`, `x-sent-timestamp`, plus anything in
`PublishingOptions.AdditionalHeaders`. Kafka headers are untyped, so values are encoded as
invariant-culture UTF-8 strings and arrive as strings in `Message<T>.Headers`.

`PublishingOptions.Key` becomes the Kafka message key, so messages with the same key go to one
partition and are consumed in the order they were sent. Without it the message has no key and
librdkafka picks the partition:

```csharp
await bus.EmitAsync("cp.dialogs.messages", payload, new PublishingOptions { Key = chatId });
```

An option the transport does not support is logged as a warning once per route: `Ttl` and
`Persistently` on Kafka, `Key` on RabbitMQ, `Delay` on any route not declared as delayed.

Headers of a consumed message are stored in the `IIncomingMessageHeaderStorage`, so a handler that
emits while processing propagates `x-correlation-id` and `x-breadcrumbs` automatically — the same
behaviour as the RabbitMQ transport.

### Request/response

Kafka has no reply queue, so Contour builds the pattern the way Kafka clients normally do: the request
carries an `x-reply-route` header naming a **reply topic** plus an `x-correlation-id`, the responder
publishes its answer there with the same correlation id, and the requester matches answers to pending
requests by that id.

Enable it on the route with `WithDefaultCallbackEndpoint()`, and answer with a responder consumer:

```csharp
IBus bus = await BusFactory.CreateKafkaBus(cfg =>
{
    cfg.WithEndpointName("orders")
        .WithBootstrapServers("kafka:9092")
        .WithGroupId("orders")
        .WithRequestTimeout(TimeSpan.FromSeconds(30));

    cfg.Route("price.requested").WithDefaultCallbackEndpoint();

    cfg.On("price.asked").ReactWith(new PriceHandler());
});

var result = await bus.RequestAsync<PriceRequest, PriceResponse>("price.requested", new PriceRequest { Sku = "X" });
Console.WriteLine(result.Response.Amount);
```

```csharp
public class PriceHandler : IConsumerOf<PriceRequest>
{
    public Task HandleAsync(IConsumingContext<PriceRequest> context)
    {
        return context.ReplyAsync(new PriceResponse { Amount = 42 });
    }
}
```

A responder is an ordinary handler that replies through the context, exactly as for RabbitMQ — there
is no separate responder interface. `context.CanReply` tells whether the message arrived as a request
at all; replying to a message that came from a plain `EmitAsync` sends nothing, as it does for
RabbitMQ, so check `CanReply` when a topic receives both requests and one-way messages.

Setting | Default | Description
------- | ------- | -----------
`WithRequestTimeout(t)` (bus or route) | none | How long `RequestAsync` waits before throwing `TimeoutException`. `RequestOptions.Timeout` overrides it per call.
`WithDefaultCallbackEndpoint()` (route) | off | Allows `RequestAsync` on the route, using `{endpoint}.{label}..reply`.
`WithCallbackTopic(topic)` (route) | — | Same, with a dedicated reply topic.

Each request/response route gets its own reply topic, `{endpoint}.{label}..reply`, so answers to
different labels never land in the same topic and no instance reads replies it cannot use. A route on
`MessageLabel.Any` has no label of its own and falls back to `{endpoint}.reply`. `WithCallbackTopic`
on the route is the only way to override the derived name; there is no bus-wide setting.

**What you need to know before using it.**

The reply topic must exist (or auto-creation must be enabled) — Contour does not create topics.

Each bus instance consumes every reply topic in its **own, generated consumer group**
(`{endpoint}.reply.{guid}`) starting from the latest offset. Kafka has no per-client addressable
queue, so every instance receives every reply on that topic and keeps only the correlation ids it is
waiting for; the rest are skipped with a trace message. That is correct but not free: the per-label
default keeps routes from reading each other's traffic, but instances of the same service still
share a topic, so pointing several routes at one `WithCallbackTopic` costs more the more routes and
instances you run. Offsets in these throwaway groups are never committed.

A reply is delivered at most once, and only while the requester is alive: `RequestAsync` throws
`OperationCanceledException` if the bus stops while the request is pending, and `TimeoutException` when
no answer arrives in time. Responses are not persisted for a restarted requester — a new instance has a
new group and starts at the latest offset. For work that must survive a restart, use one-way messaging
with an explicit response topic and a durable consumer instead.

`RequestAsync` on a route without a callback endpoint throws `NotSupportedException`.

### Handling messages

Kafka uses the same handler interface as RabbitMQ: `IConsumerOf<T>` receiving an
`IConsumingContext<T>`. There is no Kafka-specific message or handler type — the message is
`Message<T>`, and the topic it came from is its label. The context gives control over the offset and
lets a handler forward or reply without a separate bus call:

```csharp
public class UserCreatedHandler : IConsumerOf<UserCreatedEvent>
{
    public async Task HandleAsync(IConsumingContext<UserCreatedEvent> context)
    {
        if (!IsInteresting(context.Message.Payload))
        {
            await context.ForwardAsync("user.created.ignored", context.Message.Payload);
            return;
        }

        if (context.CanReply)
        {
            await context.ReplyAsync(new Ack());
        }

        await context.AcceptAsync();
    }
}
```

Handlers are registered with `ReactWith`. A handler that forwards or replies needs `WithReplies()` on
its receiver: the bus then keeps a producer for the cluster that receiver reads from, and replies and
forwards go back to that cluster, where the requester waits for the answer. Receivers on one cluster
share the producer. A receiver with a failed delivery strategy gets one as well, since the strategy
forwards to the fault topic. Without it `ReplyAsync` and `ForwardAsync` throw
`BusConfigurationException`.

```csharp
cfg.On("user.get").WithReplies().ReactWith(handler);
```

The context is Contour's own `DefaultConsumingContext<T>`; what differs per transport lives one layer
down, in the `IDelivery` the transport supplies. Two of its members carry RabbitMQ-only arguments, and
Kafka gives them a meaning of its own rather than ignoring them: `RejectAsync(requeue)` — see below —
and `ReplyAsync(response, expires)`, which throws when `expires` is set, because a Kafka message has
no expiration of its own; how long it stays available is the retention policy of the topic.

#### Offsets, Accept and Reject

The offset a worker commits is the **lowest offset it has received but not acknowledged**, not "last
handled + 1". A message that was not acknowledged therefore holds the position, so nothing after it is
committed past it and nothing is silently lost. Gaps in offset numbering — normal in compacted and
transactional topics — are safe, because only offsets actually received are considered.

By default a message is acknowledged when the handler returns without throwing. Two things change
that:

* `context.RejectAsync(requeue: true)` — the message is not acknowledged. It will be read again after
  a restart or a rebalance, and so will everything after it in that partition: Kafka has no
  per-message requeue, which is why `requeue: false` instead *skips* the message and lets the offset
  advance, as a successful handler would.
* `RequiresAccept()` on the receiver — nothing is acknowledged unless the handler calls
  `context.AcceptAsync()`, even on a clean return. The counterpart of RabbitMQ's `RequiresAccept`.

```csharp
cfg.On("orders.created").RequiresAccept().ReactWith(handler);
```

A worker hands messages to the handler one at a time: the next message is not read until
`HandleAsync` completes, so the order within a partition is kept. Parallelism comes from
`ParallelismLevel` — more workers in the consumer group, each reading its own partitions — not
from overlapping handlers.

Under `RequiresAccept()` the acknowledgement does not have to come before the handler returns. A
handler that accumulates messages and acknowledges a whole batch later — as the batching consumers of
`Gems.ServiceBus.Emitting` do — keeps working: the worker does not wait for that decision, it goes on
reading, and the offset advances once the acknowledgement arrives. Decisions that never arrive simply
hold their offset, and a bus stopping does not wait for them: those messages are read again after a
restart.

#### Handler lifetime

In the section-configured path, `IIncoming.Lifestyle` controls when the handler is created,
exactly as `IIncoming.Lifestyle` does for RabbitMQ: `Normal` resolves it once at configuration time,
`Lazy` on the first message, `Delegated` on every message. `Delegated` is what a handler depending on
scoped services needs; Contour's own `LazyConsumerOf<T>` and `FactoryConsumerOf<T>` are the wrappers
behind it and can be used directly in code-based configuration.

#### Fetch tuning

`WithFetch` is the nearest thing to RabbitMQ's `WithQoS`: it sets how much a consumer keeps buffered
locally per partition and how much it fetches at a time.

```csharp
cfg.WithFetch(queuedMinMessages: 100, maxPartitionFetchBytes: 1_048_576);
```

These map onto librdkafka's `queued.min.messages` and `max.partition.fetch.bytes`. There is no
equivalent of the Java client's `max.poll.records`: librdkafka does not know that property and
rejects the whole configuration if it is set, and `Consume` hands over one message per call anyway.

#### Naming topics from message types

`On<TModel>()` and `Route<T>()` take the topic name from the `[Message("...")]` attribute on the
payload type, like their RabbitMQ counterparts. A type without the attribute is rejected — the name is
never guessed from the type name.

```csharp
[Message("user.created")]
public class UserCreatedEvent { }

cfg.On<UserCreatedEvent>().ReactWith(handler);
cfg.Route<UserUpdatedEvent>();
```

#### Broker addresses resolved at runtime

`WithBootstrapServersProvider` accepts Contour's `IConnectionStringProvider`, so addresses can be
computed per label instead of hard-coded — the counterpart of RabbitMQ's
`WithConnectionStringProvider`. A provider wins over a value set with `WithBootstrapServers`.

### Receiver settings

`KafkaBusConfigurator` exposes the Confluent consumer settings (`WithBootstrapServers`, `WithGroupId`,
`WithAutoOffsetReset`, `WithAutoCommit`, `WithPartitionAssignmentStrategy`, `WithSecurityProtocol`,
`WithSslCaPem`, `WithSslCertificatePem`, `WithSslKeyPem`, `WithSslCertificateVerification`, `WithDebug`)
plus the library-specific ones:

Setting | Default | Description
------- | ------- | -----------
`WithParallelismLevel(n)` | 1 | Number of workers per topic; every worker owns a dedicated consumer and a dedicated thread.
`WithCommitOffset(n)` | 1000 | Manual commit checkpoint: commit after every `n` **handled** messages (offset multiples are unreliable in compacted/transactional topics).
`WithCommitTimeout(t)` | 5 s | Commit the pending tail after `t` of inactivity, so an idle partition does not stay uncommitted.
`WithPayloadConverter(c)` | `JsonPayloadConverter` | `IPayloadConverter` used to serialize and deserialize the message body.
`WithFetch(messages, bytes)` | librdkafka defaults | Local buffer per partition (`queued.min.messages`) and fetch size (`max.partition.fetch.bytes`) — the nearest analogue of RabbitMQ's QoS prefetch.
`RequiresAccept()` (receiver) | off | Demand an explicit `context.AcceptAsync()` before the offset advances.
`WithReplies()` (receiver) | off | Let the handler reply and forward: keeps a producer for the receiver cluster.

Every one of these also exists on `KafkaReceiverConfigurator`, where it overrides the bus-level value
for that topic only:

```csharp
cfg.WithGroupId("shared-group");

cfg.On("topic-a").ReactWith(handlerA);                       // shared-group

cfg.On("topic-b").WithGroupId("own-group")                   // own-group
    .WithParallelismLevel(8)
    .ReactWith(handlerB);
```

A receiver or route takes a snapshot of the bus defaults at the moment `On(topic)` / `Route(topic)` is
called, so `With*` calls made on the bus configurator afterwards do not affect what was already
declared — same as RabbitMQ. The payload converter is the exception: it is a bus-level setting applied
to every component that has none of its own, regardless of ordering.

Out-of-range values (`WithParallelismLevel(0)`, a non-positive commit offset or timeout) throw
`ArgumentOutOfRangeException` at configuration time. Completeness is checked by
`KafkaBusConfigurator.Validate`, which `CreateKafkaBus` calls before building the bus; it throws
`KafkaBusConfigurationException` (a `BusConfigurationException`) when a component has no servers,
no group, no consumer, or no payload converter.

### Lifecycle and graceful shutdown

```csharp
await bus.StopReceiving();               // stop polling, keep handling what was already received
await bus.WaitForRequestsCompletion();   // wait for in-flight handlers
await bus.StopProducing();               // flush producer buffers
await bus.Stop();                        // commit offsets, leave the consumer group, release resources
```

`Stop` performs all of these itself. On shutdown every worker commits the position after the last
successfully handled message **per partition**. The same commit happens when partitions are revoked
during a rebalance. Producers flush their buffers, so messages already accepted by `EmitAsync` are
not lost.

### Unhandled messages

A subscription always has a handler — `Validate` rejects a receiver without `ReactWith` — so
unhandled messages can only appear in a reply topic: a message there without `x-correlation-id` is
not a response. `OnUnhandled` on the bus sets what to do with it; without a strategy it is skipped
with a warning. Responses awaited by another instance of the service are not unhandled:
every instance reads the whole reply topic and silently skips the responses it is not waiting for.

### Failed messages and the fault topic

A handler that throws never stops the worker. What happens to the message depends on whether a
fault strategy is configured.

`UseDefaultFaultStrategy()` forwards the message to a **fault topic** — the counterpart of the
RabbitMQ transport's fault queue:

```csharp
cfg.WithEndpointName("orders")
    .UseDefaultFaultStrategy();         // forwards to "{endpoint}.fault"
```

The forwarded message is a `KafkaFaultMessage`: the original payload as text, the content type, the
receive timestamp and a serializable description of the exception. The payload is kept as raw text
rather than a deserialized object, because the handler may have failed on deserialization itself —
the same choice the RabbitMQ transport makes. Contour's `x-*` headers of the original travel with it,
so the entry in the fault topic can be tied back to its chain by `x-correlation-id`.

The strategy also covers a message that could not be deserialized at all: a payload the converter
chokes on is a failure of handling like any other, and letting it past the strategy would leave it
unacknowledged and stall its partition for good.

Once the message has been forwarded, its offset advances, so a restarted bus does not read it again.
**If forwarding itself fails, the offset is not advanced** and the message is read again on the next
poll — unlike RabbitMQ, Kafka has no requeue, so swallowing a failed forward would lose the message
for good. A permanently unavailable fault topic therefore stalls that partition, loudly, instead of
discarding messages quietly.

`OnFailed` takes over completely if you want something else — a topic per source, retry topics, a
metric:

```csharp
cfg.OnFailed(context => context.ForwardAsync("dead-letter", context.BuildFaultMessage()));

cfg.On("orders.created")
    .OnFailed(context => context.ForwardAsync("orders.created.fault", context.BuildFaultMessage()))
    .ReactWith(handler);
```

**With no strategy configured**, the failure is logged and the message is *skipped*: its offset
advances as if the handler had returned cleanly. Holding the offset instead would stall the partition
on a single poison message forever, so a message nobody was told what to do with is dropped rather
than allowed to block everything behind it. Configure a fault strategy for anything you cannot afford
to lose.

Configuring the bus from a section calls `UseDefaultFaultStrategy()` for you, exactly as the RabbitMQ
`Configure` does.

There is no `OnUnhandled` counterpart: a Kafka receiver subscribes to one topic and always has a
consumer registered for it (configuration validation requires one), so the "no handler for this
message" case cannot arise.

**Topic names must be lower case.** Contour normalises message labels to lower case, and a topic name
is used as its label, while Kafka topic names are case-sensitive — a mixed-case name would be
published to its lower-case form instead. `On`, `Route` and `WithCallbackTopic` reject anything else
at configuration time rather than misrouting silently.

As with `RmqBus`, a stopped bus cannot be restarted — create a new instance. `State` stays `Working`
until both receiving and producing have stopped; use `IBus.IsHealthy` for liveness probes, which
reports `false` when the bus is not working, a consume loop has died on a fatal transport error, a
producer has been broken by a fatal broker error and not yet rebuilt, or receiving has been stopped. It is available on both transports. `bus.ActiveRequestsCount` shows how many messages are being processed
right now. `IBus.Transport` names the transport behind the bus — `"rmq"` or `"kafka"` (`BusTransports.Rmq`
and `BusTransports.Kafka`) — so callers such as metrics can tell the two apart without knowing the
concrete bus type.

### Configuration from a config section

`KafkaBusConfigurator.Configure` builds the bus from an `IKafkaEndpoint`, the exact counterpart of
`RabbitBusConfigurator.Configure`: one call takes the whole section — endpoint-level settings,
receivers (`IIncoming`) and send routes (`IOutgoing`) — and resolves handlers, the lifecycle handler
and the payload converter through Contour's `IDependencyResolver`.

```csharp
var section = new KafkaEndpoint
{
    Name = "orders",
    ConnectionString = "kafka-1:9092,kafka-2:9092",
    GroupId = "orders",
    ParallelismLevel = 4,
    AutoOffsetReset = AutoOffsetReset.Earliest,
    SecurityProtocol = SecurityProtocol.SaslSsl,
    LifecycleHandler = "busLifecycle",
    ExcludedHeaders = ["x-internal"],

    Incoming = [new IncomingConfiguration { Label = "user.created", React = "userCreatedHandler", Type = "UserCreatedEvent" }],
    Outgoing = [new OutgoingConfiguration { Label = "user.updated" }]
};

var bus = await BusFactory.CreateKafkaBus(cfg =>
    cfg.Configure(section, (name, type) => container.Resolve(type, name)));
```

`KafkaEndpoint`, `IncomingConfiguration` and `OutgoingConfiguration` are ready-made implementations you can bind configuration
to. A receiver is skipped when its `ParallelismLevel` is `0` — declaring a reader with no workers
is the way to turn one off.

Section-level values act as defaults for every receiver and route: `Name`, `BootstrapServers`,
`GroupId`, `ParallelismLevel`, `CommitOffset`, `CommitTimeout`, `AutoOffsetReset`, `EnableAutoCommit`,
`PartitionAssignmentStrategy`, `Confirm`/`ConfirmTimeout`, `FailoverAttempts`, `RequestTimeout`,
`ExcludedHeaders`, `LifecycleHandler`, `Debug`, and the security/SSL block. An
element overrides them only where it sets something of its own; anything left unset on both the
element and the section keeps whatever was configured in code, so a section can top up a
code-built configuration instead of replacing it.

`Dynamic = new … { Outgoing = true }` declares a catch-all route (`MessageLabel.Any`), which lets
`EmitAsync` publish to a topic the section never named — the same role it plays for RabbitMQ.

A sender entry covers everything `Route(...)` does: `Confirm`/`ConfirmTimeout`, `FailoverAttempts`,
`FlushTimeout`, `Alias`, the producer settings (`Acks`, `CompressionType`,
`EnableIdempotence`, `Linger`, `MessageTimeout`, security and SSL), and request/response via
`RequiresCallback` or `CallbackTopic` — setting a callback topic enables request/response on its own.

When `IIncoming.Type` is not set, the payload is deserialized into an `ExpandoObject`.

Two knobs from RabbitMQ's endpoint have no Kafka counterpart and are absent by design: `Qos` (Kafka
prefetch is not per-consumer configurable this way) and the fault/queue limits (`FaultQueueTtl`,
`QueueLimit`, …), which describe AMQP topology that Kafka does not have. `Persistently()`, `WithTtl()`
and `Delayed()` are absent for the same reason: Kafka interprets none of `x-persist`, `x-ttl` and
`x-delay`, so a route could not honour them.

## Contour message headers
List of used headers is represented in table below.

For message tracking in chain of  application components interaction through service bus several incoming messages headers are copied in the outgoing messages.

All incoming headers not mentioned in the table below are copied to outgoing messages, except for the headers listed in `ExcludedHeaders` property of `EndpointConfiguration`. A single `*` entry in that list excludes every header, so nothing from an incoming message is carried over.

Header field name | Description | Copying
----------------- | ----------- |--------
x-correlation-id | The correlation identifier is used to track a set of messages as a single group. It is used to match reply message with the request. New value for header is generated for every request (if not supplied in headers parameter).| Only for one-way messages.
x-expires | Header, which contains the rules of data deterioration. For example: x-expires: at 2016-04-01T22:00:33Z or x-expires: in 100 | No
x-message-type | Message label with which it was sent. Using this header is not recommended | No
x-persist | Marks message persistance (saved on disk) or not | No
x-reply-route | Reply message address for the request. For example, x-reply-route: direct:///amq.gen-n9DsUj1qm4vgCq0MHHPoBQ | No
x-timeout | Response timeout for the request | No
x-ttl | Message TTL | No
x-breadcrumbs | List of all endpoints, through which message has passed, separated by semicolon (;) | Yes (appending new value)
x-original-message-id | Message identifier, that started the messages exchange | Yes

## Library license

The library is available under MIT. For more information see the [License file][1] in the GitHub repository.

 [1]: https://github.com/SDVentures/Contour/blob/master/LICENSE.md
