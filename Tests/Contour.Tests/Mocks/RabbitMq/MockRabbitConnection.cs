using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Contour.Transport.RabbitMQ.Internal;
using RabbitMQ.Client;
using Moq;

namespace Contour.Tests.Mocks.RabbitMq;

internal class MockRabbitConnection : IRabbitConnection
{
    private bool isDisposed;
    private bool isActive;
    private bool isBlocked;
    private readonly RabbitBus busContext;

    public List<Mock<IChannel>> Channels { get; } = [];

    public MockRabbitConnection(string connectionString, string connectionKey, RabbitBus bus)
    {
        Id = Guid.NewGuid();
        ConnectionString = connectionString ?? throw new ArgumentNullException(nameof(connectionString));
        ConnectionKey = connectionKey ?? connectionString;
        this.busContext = bus ?? throw new ArgumentNullException(nameof(bus));
        this.isActive = false;
        this.isBlocked = false;
    }

    public Guid Id { get; }

    public bool IsActive => this.isActive && !this.isDisposed;

    public string ConnectionString { get; }

    public string ConnectionKey { get; }

    public bool IsBlocked => this.isBlocked && !this.isDisposed;

    public bool IsOpen => !this.isDisposed;

    public Task Open(CancellationToken token)
    {
        ThrowIfDisposed();
        token.ThrowIfCancellationRequested();

        this.isActive = true;
        return Task.CompletedTask;
    }

    public Task Close()
    {
        if (!this.isDisposed)
        {
            this.isActive = false;
        }
        return Task.CompletedTask;
    }

    public Task Abort(CancellationToken token)
    {
        token.ThrowIfCancellationRequested();

        if (!this.isDisposed)
        {
            this.isActive = false;
        }
        return Task.CompletedTask;
    }

    public Task<RabbitChannel> OpenChannel(CreateChannelOptions options = null, CancellationToken token = default)
    {
        ThrowIfDisposed();
        token.ThrowIfCancellationRequested();

        if (!IsActive)
        {
            throw new InvalidOperationException("Connection is not active. Call Open() first.");
        }

        // Create a mock IChannel
        var mockChannel = CreateMockChannel();
        Channels.Add(mockChannel);

        // Create real RabbitChannel with mocked IChannel and provided IBusContext
        var rabbitChannel = new RabbitChannel(this, mockChannel.Object, this.busContext);

        return Task.FromResult(rabbitChannel);
    }

    public Task<RabbitChannel> OpenSharedChannel(string key, CreateChannelOptions options = null, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        cancellationToken.ThrowIfCancellationRequested();

        if (!IsActive)
        {
            throw new InvalidOperationException("Connection is not active. Call Open() first.");
        }

        if (string.IsNullOrEmpty(key))
        {
            throw new ArgumentException("Shared channel key cannot be null or empty.", nameof(key));
        }

        // Create a mock IChannel
        var mockChannel = CreateMockChannel();
        Channels.Add(mockChannel);
        // Create real RabbitChannel with mocked IChannel and provided IBusContext
        var rabbitChannel = new RabbitChannel(this, mockChannel.Object, this.busContext);

        return Task.FromResult(rabbitChannel);
    }

    public ValueTask DisposeAsync()
    {
        if (!this.isDisposed)
        {
            this.isDisposed = true;
            this.isActive = false;
        }
        return ValueTask.CompletedTask;
    }

    /// <summary>
    /// Sets the blocked state for testing purposes
    /// </summary>
    public void SetBlocked(bool blocked)
    {
        this.isBlocked = blocked;
    }

    private void ThrowIfDisposed()
    {
        if (this.isDisposed)
        {
            throw new ObjectDisposedException(nameof(MockRabbitConnection));
        }
    }

    /// <summary>
    /// Creates a mock IChannel with basic setup
    /// </summary>
    private static Mock<IChannel> CreateMockChannel()
    {
        var mockChannel = new Mock<IChannel>();

        // Setup basic properties and methods
        mockChannel.SetupGet(c => c.IsOpen).Returns(true);
        // ChannelNumber setup can be added later if needed

        // Setup async methods - for old Moq version, we'll setup as returning Task
        mockChannel.Setup(c => c.BasicAckAsync(It.IsAny<ulong>(), It.IsAny<bool>(), It.IsAny<CancellationToken>()))
            .Returns(() => ValueTask.CompletedTask);

        mockChannel.Setup(c => c.BasicNackAsync(It.IsAny<ulong>(), It.IsAny<bool>(), It.IsAny<bool>(), It.IsAny<CancellationToken>()))
            .Returns(() => ValueTask.CompletedTask);

        mockChannel.Setup(c => c.BasicRejectAsync(It.IsAny<ulong>(), It.IsAny<bool>(), It.IsAny<CancellationToken>()))
            .Returns(() => ValueTask.CompletedTask);

        mockChannel.Setup(c => c.CloseAsync(It.IsAny<ushort>(), It.IsAny<string>(), It.IsAny<bool>(), It.IsAny<CancellationToken>()))
            .Returns(() => Task.CompletedTask);

        return mockChannel;
    }
}