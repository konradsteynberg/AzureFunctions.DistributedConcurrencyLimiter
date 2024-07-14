using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using CCBA.Integrations.Base.Abstracts;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using ExecutionContext = Microsoft.Azure.WebJobs.ExecutionContext;

namespace CCBA.Testing.ConcurrencyLimiterTest.Services;

public class ConcurrencyLimiterService : BaseLogger
{
    private readonly ILogger<ConcurrencyLimiterService> _logger;
    private readonly IConfiguration _configuration;
    private readonly IMemoryCache _memoryCache;
    private readonly BlobContainerClient _blobContainerClient;

    private static readonly object _cacheLock = new();

    public ConcurrencyLimiterService(ILogger<ConcurrencyLimiterService> logger, IConfiguration configuration, IMemoryCache memoryCache) : base(logger, configuration)
    {
        _logger = logger;
        _configuration = configuration;
        _memoryCache = memoryCache;

        var blobServiceClient = new BlobServiceClient(Environment.GetEnvironmentVariable("AzureWebJobsStorage"));
        _blobContainerClient = blobServiceClient.GetBlobContainerClient("leases");
        _blobContainerClient.CreateIfNotExists();
    }

    /// <summary>
    /// Waits for a concurrent lease to be acquired.
    /// </summary>
    /// <param name="leaseName">The name of the lease.</param>
    /// <param name="maxConcurrency">The maximum allowed concurrency.</param>
    /// <param name="timeout">The maximum time to wait for the lease to be acquired.</param>
    /// <param name="executionContext">The execution context.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation that returns a <see cref="ConcurrencyLimiterSession"/> object.</returns>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="leaseName"/> is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when <paramref name="maxConcurrency"/> is less than or equal to 0.</exception>
    /// <exception cref="TimeoutException">Thrown when the lease acquisition times out.</exception>
    /// <exception cref="Exception">Thrown when an unknown error occurs while waiting for the concurrent lease.</exception>
    public async Task<ConcurrencyLimiterSession> WaitForConcurrentLeaseAsync(string leaseName, int maxConcurrency, TimeSpan timeout,
        ExecutionContext? executionContext = null, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(leaseName)) throw new ArgumentNullException(nameof(leaseName));
        if (maxConcurrency <= 0) throw new ArgumentOutOfRangeException(nameof(maxConcurrency));

        var stopwatch = Stopwatch.StartNew();

        lock (_cacheLock)
        {
            // ensure blobs exist for leasing according to max concurrency limit
            for (var i = 0; i < maxConcurrency; i++)
            {
                var blobClient = _blobContainerClient.GetBlobClient($"lease-{leaseName}-{i.ToString().PadLeft(2, '0')}");

                var key = $"LeaseBlobExists.{blobClient.Name}";
                if (_memoryCache.Get<bool>(key)) continue;
                if (!blobClient.Exists(cancellationToken))
                    blobClient.Upload(BinaryData.FromString(string.Empty), true, cancellationToken);

                _memoryCache.Set(key, true);
            }
        }

        string? leaseId = null;
        while (leaseId == null)
        {
            for (var i = 0; i < maxConcurrency; i++)
            {
                try
                {
                    var blobName = $"lease-{leaseName}-{i.ToString().PadLeft(2, '0')}";
                    var key = $"BlobLease.{blobName}";

                    lock (_cacheLock)
                    {
                        var lastChecked = _memoryCache.Get(key);

                        if (lastChecked == null)
                        {
                            var blobClient = _blobContainerClient.GetBlobClient(blobName);
                            var blobLeaseClient = blobClient.GetBlobLeaseClient();

                            var leaseResponse = blobLeaseClient.Acquire(TimeSpan.FromSeconds(15), cancellationToken: cancellationToken);
                            leaseId = leaseResponse.Value.LeaseId;

                            _memoryCache.Set(key, leaseId, new MemoryCacheEntryOptions { SlidingExpiration = TimeSpan.FromSeconds(5) });

                            LogInformation($"Acquired blob lease on blob '{blobClient.Name}'", LogLevel.Trace, properties: new()
                            {
                                { "BlobName", blobClient.Name },
                                { "LeaseId", leaseId },
                                { "Duration", stopwatch.Elapsed.ToString() },
                                { nameof(executionContext.InvocationId), executionContext?.InvocationId.ToString() }
                            });

                            return new ConcurrencyLimiterSession(_logger, _configuration, blobLeaseClient, blobName, _memoryCache, executionContext, cancellationToken);
                        }
                    }
                }
                catch (Azure.RequestFailedException ex) when (ex.Status == 409)
                {
                    // blob is already leased, need to wait for it to become available
                }

                if (timeout != default && timeout != TimeSpan.Zero && stopwatch.Elapsed > timeout)
                {
                    throw new TimeoutException($"Timed out waiting for concurrent lease.")
                    {
                        Data =
                        {
                            { "LeaseName", leaseName },
                            { "MaxConcurrency", $"{maxConcurrency}" },
                            { "Timeout", $"{timeout}" },
                            { "Elapsed", $"{stopwatch.Elapsed}" },
                            { nameof(executionContext.InvocationId), executionContext?.InvocationId.ToString() }
                        }
                    };
                }
            }

            await Task.Delay(10, cancellationToken);
        }

        throw new Exception("Unknown error occurred while waiting for concurrent lease.");
    }

    public class ConcurrencyLimiterSession : BaseLogger, IAsyncDisposable
    {
        private readonly BlobLeaseClient _blobLeaseClient;
        private readonly string _blobName;
        private readonly IMemoryCache _memoryCache;
        private readonly ExecutionContext? _executionContext;

        private readonly Timer? _timerRenewLease = null;

        public ConcurrencyLimiterSession(ILogger<ConcurrencyLimiterService> logger, IConfiguration configuration,
            BlobLeaseClient blobLeaseClient, string blobName, IMemoryCache memoryCache, ExecutionContext? executionContext, CancellationToken cancellationToken) : base(logger, configuration)
        {
            _executionContext = executionContext;
            _blobLeaseClient = blobLeaseClient;
            _blobName = blobName;
            _memoryCache = memoryCache;

            _timerRenewLease = new Timer(_ =>
            {
                try
                {
                    Response<BlobLease>? response = blobLeaseClient.Renew(cancellationToken: cancellationToken);
                    LogInformation($"Lease renewed.", LogLevel.Trace, properties: new()
                    {
                        { nameof(response.Value.LeaseId), response.Value.LeaseId },
                        { nameof(_executionContext.InvocationId), _executionContext?.InvocationId.ToString() }
                    });
                }
                catch (Exception e)
                {
                    LogException($"{e.Message}", e, LogLevel.Warning, properties: new()
                    {
                        { nameof(_blobLeaseClient.LeaseId), _blobLeaseClient.LeaseId },
                        { nameof(_executionContext.InvocationId), _executionContext?.InvocationId.ToString() }
                    });
                }
            }, null, TimeSpan.FromSeconds(5), TimeSpan.FromSeconds(5));
        }

        public async ValueTask DisposeAsync()
        {
            try
            {
                if (_timerRenewLease != null) await _timerRenewLease.DisposeAsync();
                await _blobLeaseClient.ReleaseAsync();

                var key = $"BlobLease.{_blobName}";

                _memoryCache.Remove(key);

                LogInformation($"Lease released.", LogLevel.Trace, properties: new()
                {
                    { nameof(_blobLeaseClient.LeaseId), _blobLeaseClient.LeaseId },
                    { nameof(_executionContext.InvocationId), _executionContext?.InvocationId.ToString() }
                });
            }
            catch (Exception e)
            {
                LogException(e, LogLevel.Warning, properties: new()
                {
                    { nameof(_blobLeaseClient.LeaseId), _blobLeaseClient.LeaseId },
                    { nameof(_executionContext.InvocationId), _executionContext?.InvocationId.ToString() }
                });
            }
        }
    }
}