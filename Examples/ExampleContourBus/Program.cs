using Contour;
using ExampleContourBus;

Console.WriteLine("Start!");

var bus = await BusFactory.CreateRmqBus(cfg =>
{
    cfg.WithEndpointName("TestEndpoint")
        .WithConnectionString("amqp://guest:guest@docker.15.stage/cp");

    cfg.On("test.incoming.message")
        .ReactWith(new TestConsumer());
});

var bus2 = await BusFactory.CreateRmqBus(cfg =>
{
    cfg.WithEndpointName("TestEndpoint2")
        .WithConnectionString("amqp://guest:guest@docker.15.stage/cp");

    cfg.Route("test.incoming.message");
});

await bus2.EmitAsync("test.incoming.message", new TestModel());

Console.WriteLine("Press any key to exit...");
Console.ReadLine();

await bus.Stop();
await bus2.Stop();
