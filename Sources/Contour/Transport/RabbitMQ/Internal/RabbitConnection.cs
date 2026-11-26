using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.Extensions.Logging;
using Contour.Helpers;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;

using INativeConnection = RabbitMQ.Client.IConnection;

namespace Contour.Transport.RabbitMQ.Internal;

internal class RabbitConnection : IRabbitConnection
{
    private const int ConnectionTimeout = 3000;
    private const int OperationTimeout = 500;
    private readonly SemaphoreSlim semaphore = new(1, 1);
    private readonly ILogger logger = LogManager.GetLogger<RabbitConnection>();
    private readonly IEndpoint endpoint;
    private readonly RabbitBus bus;
    private readonly ConnectionFactory connectionFactory;
    private INativeConnection connection;

    private readonly Dictionary<string, RabbitChannel> sharedChannels = new();

    public RabbitConnection(IEndpoint endpoint, string connectionString, RabbitBus bus)
    {
        Id = Guid.NewGuid();
        this.endpoint = endpoint;
        ConnectionString = connectionString;
        this.bus = bus;

        ConnectionKey = GetConnectionKey(connectionString);

        var clientProperties = new Dictionary<string, object>
        {
            { "Endpoint", this.endpoint.Address },
            { "Machine", Environment.MachineName },
            { "Location", Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location) },
            { "ConnectionId", Id.ToString() }
        };

        this.connectionFactory = new ConnectionFactory
        {
            Uri = new Uri(ConnectionString),
            AutomaticRecoveryEnabled = false,
            ClientProperties = clientProperties,
            RequestedConnectionTimeout = TimeSpan.FromMilliseconds(ConnectionTimeout),
            ConsumerDispatchConcurrency = 1
        };
    }

    public Guid Id { get; }

    public bool IsActive { get; private set; }

    public bool IsOpen => this.connection?.IsOpen ?? false;

    public string ConnectionString { get; }

    public string ConnectionKey { get; }

    public bool IsBlocked { get; private set; }

    public async Task Open(CancellationToken token)
    {
        await this.semaphore.ExecuteWithCondition(async () =>
        {
            await OpenInternal(token);
            IsActive = true;
            return 0;
        }, logger: this.logger, cancellationToken: token);
    }

    private async Task OpenInternal(CancellationToken token)
    {
        if (IsOpen)
        {
            this.logger.LogTrace("Connection [{ConnectionId}] is already open", Id);
            return;
        }

        this.logger.LogInformation("Connecting to RabbitMQ using [{ConnectionString}]", ConnectionString);

        var retryCount = 0;
        while (true)
        {
            token.ThrowIfCancellationRequested();

            INativeConnection con = null;
            try
            {
                con = await this.connectionFactory.CreateConnectionAsync(token);
                con.ConnectionShutdownAsync += OnConnectionShutdown;

                con.ConnectionBlockedAsync += OnConnectionBlocked;
                con.ConnectionUnblockedAsync += OnConnectionUnblocked;
                this.connection = con;
                this.logger.LogInformation("Connection [{ConnectionId}] opened at [{Endpoint}]", Id, this.connection.Endpoint);
                return;
            }
            catch (Exception ex)
            {
                if (con != null)
                {
                    con.ConnectionShutdownAsync -= OnConnectionShutdown;
                    await con.AbortAsync(TimeSpan.FromMilliseconds(OperationTimeout));
                }

                var secondsToRetry = Math.Min(10, retryCount);

                this.logger.LogWarning(ex, "Unable to connect to RabbitMQ on connection string: [{ConnectionString}]. Retrying in {SecondsToRetry} seconds...", ConnectionString, secondsToRetry);

                await Task.Delay(TimeSpan.FromSeconds(secondsToRetry), cancellationToken: token);
                retryCount++;
            }
        }
    }

    private Task OnConnectionBlocked(object sender, ConnectionBlockedEventArgs e)
    {
        this.logger.LogInformation("OnConnectionBlocked {Sender}. Reason: {Reason}", sender, e.Reason);
        IsBlocked = true;
        return Task.CompletedTask;
    }

    private Task OnConnectionUnblocked(object sender, AsyncEventArgs asyncEventArgs)
    {
        this.logger.LogInformation("OnConnectionUnblocked {Sender}", sender);
        IsBlocked = false;
        return Task.CompletedTask;
    }

    public Task<RabbitChannel> OpenChannel(CreateChannelOptions options = null, CancellationToken cancellationToken = default)
    {
        return this.semaphore.ExecuteWithCondition(async () => await OpenChannelInternal(options, cancellationToken), logger: this.logger, cancellationToken: cancellationToken).AsTask();
    }

    public async Task<RabbitChannel> OpenSharedChannel(string key, CreateChannelOptions options = null, CancellationToken cancellationToken = default)
    {
        if (this.sharedChannels.TryGetValue(key, out var channel) && channel.IsOpen)
        {
            return channel;
        }

        return await this.semaphore.ExecuteWithCondition(async () =>
        {
            if (this.sharedChannels.TryGetValue(key, out channel) && channel.IsOpen)
            {
                return channel;
            }

            if (channel != null)
            {
                this.logger.LogTrace("An attempt to open a new shared channel with key '{Key}' while the old one is not removed from shared cache", key);
                this.sharedChannels.Remove(key);
            }

            var newChannel = await OpenChannelInternal(options, cancellationToken);

            this.sharedChannels.Add(key, newChannel);

            return newChannel;
        }, logger: this.logger, cancellationToken: cancellationToken);
    }

    public async Task Close()
    {
        IsActive = false;
        await this.semaphore.ExecuteWithCondition(async () =>
        {
            await Task.WhenAll(this.sharedChannels.Select(c => c.Value.DisposeAsync().AsTask()));
            this.sharedChannels.Clear();

            if (this.connection != null)
            {
                if (this.connection.CloseReason == null)
                {
                    this.logger.LogTrace("[{Endpoint}]: closing connection", this.endpoint);
                    try
                    {
                        await this.connection.CloseAsync(TimeSpan.FromMilliseconds(OperationTimeout));
                        if (this.connection.CloseReason != null)
                        {
                            await this.connection.AbortAsync(TimeSpan.FromMilliseconds(OperationTimeout));
                        }
                    }
                    catch (AlreadyClosedException ex)
                    {
                        this.logger.LogWarning("[{Endpoint}]: connection is already closed: {Message}", this.endpoint, ex.Message);
                    }
                }
            }

            return 0;
        }, logger: this.logger);
    }

    public async Task Abort(CancellationToken token)
    {
        IsActive = false;
        await this.semaphore.ExecuteWithCondition(async () =>
        {
            try
            {
                if (this.connection != null)
                {
                    await this.connection.AbortAsync();
                }
            }
            catch (Exception ex)
            {
                this.logger.LogError(ex, "[{Endpoint}]: failed to abort the underlying connection due to: {Message}", this.endpoint, ex.Message);
                throw;
            }

            return 0;
        }, cancellationToken: token);
    }

    public async ValueTask DisposeAsync()
    {
        IsActive = false;
        try
        {
            await Close();

            if (this.connection != null)
            {
                this.logger.LogTrace("[{Endpoint}]: disposing connection [{ConnectionId}] at [{Endpoint2}]", this.endpoint, Id, this.connection.Endpoint);
                this.connection?.Dispose();
                this.connection = null;
            }
            this.semaphore.Dispose();
        }
        catch (Exception ex)
        {
            this.logger.LogTrace("An error {Message} during connection cleanup has been suppressed", ex.Message);
        }
    }

    public override string ToString()
    {
        return $"{Id} : {ConnectionString} : {ConnectionKey}";
    }

    private Task OnConnectionShutdown(object sender, ShutdownEventArgs shutdownEventArgs)
    {
        IsActive = false;
        this.logger.LogTrace("Connection [{ConnectionId}] has been closed due to {ReplyText} ({ReplyCode})", Id, shutdownEventArgs.ReplyText, shutdownEventArgs.ReplyCode);
        ((INativeConnection)sender).ConnectionShutdownAsync -= OnConnectionShutdown;
        return Task.CompletedTask;
    }

    // TODO вынести знание о DNS и прочем за пределы контура, вычислять данные вне контура и передавать на старте
    private static string GetConnectionKey(string connectionString)
    {
        var uri = new Uri(connectionString, UriKind.Absolute);

        var ip = string.Join(",", Dns.GetHostAddresses(uri.Host).OrderBy(x => x.ToString()));

        return $"{ip}:{uri.Port}:{uri.Segments.Last()}";
    }

    private async Task<RabbitChannel> OpenChannelInternal(CreateChannelOptions options, CancellationToken token)
    {
        while (true)
        {
            token.ThrowIfCancellationRequested();

            if (this.connection == null || !this.connection.IsOpen)
            {
                await OpenInternal(token);
            }

            try
            {
                var model = await this.connection.CreateChannelAsync(options, cancellationToken: token);
                return new RabbitChannel(this, model, this.bus);
            }
            catch (Exception ex)
            {
                this.logger.LogError(ex, "Failed to open a new channel on connection string: [{ConnectionString}] due to: {Message}; retrying...", ConnectionString, ex.Message);
            }
        }
    }
}