using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using ExecutionContext = Microsoft.Azure.WebJobs.ExecutionContext;

namespace FunctionApp.Services;

public class ConcurrencyLimiterService
{
    private readonly ILogger<ConcurrencyLimiterService> _logger;
    private readonly IConfiguration _configuration;
    private readonly IMemoryCache _memoryCache;
    private readonly BlobContainerClient _blobContainerClient;

    private static readonly object _cacheLock = new();

    public ConcurrencyLimiterService(ILogger<ConcurrencyLimiterService> logger, IConfiguration configuration, IMemoryCache memoryCache)
    {
        _logger = logger;
        _configuration = configuration;
        _memoryCache = memoryCache;

        var blobServiceClient = new BlobServiceClient(Environment.GetEnvironmentVariable("AzureWebJobsStorage"));
        _blobContainerClient = blobServiceClient.GetBlobContainerClient("leases");
        _blobContainerClient.CreateIfNotExists();
    }
    
    public async Task<ConcurrencyLimiterSession> WaitForConcurrentLeaseAsync(string leaseName, int maxConcurrency, TimeSpan timeout,
        ExecutionContext? executionContext = null, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(leaseName, nameof(leaseName));
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maxConcurrency, nameof(maxConcurrency));

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

                            _logger.LogDebug($"Acquired blob lease on blob '{blobClient.Name}'");

                            return new ConcurrencyLimiterSession(_logger, _configuration, blobLeaseClient, blobName, _memoryCache, executionContext, cancellationToken);
                        }
                    }
                }
                catch (RequestFailedException ex) when (ex.Status == 409)
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

    public class ConcurrencyLimiterSession : IAsyncDisposable
    {
        private readonly ILogger<ConcurrencyLimiterService> _logger;
        private readonly BlobLeaseClient _blobLeaseClient;
        private readonly string _blobName;
        private readonly IMemoryCache _memoryCache;
        private readonly ExecutionContext? _executionContext;

        private readonly Timer? _timerRenewLease = null;

        public ConcurrencyLimiterSession(ILogger<ConcurrencyLimiterService> logger, IConfiguration configuration,
            BlobLeaseClient blobLeaseClient, string blobName, IMemoryCache memoryCache, ExecutionContext? executionContext, CancellationToken cancellationToken)
        {
            _logger = logger;
            _executionContext = executionContext;
            _blobLeaseClient = blobLeaseClient;
            _blobName = blobName;
            _memoryCache = memoryCache;

            _timerRenewLease = new Timer(_ =>
            {
                try
                {
                    Response<BlobLease>? response = blobLeaseClient.Renew(cancellationToken: cancellationToken);
                    _logger.LogDebug($"Lease renewed. LeaseId={response.Value.LeaseId}");
                }
                catch (Exception e)
                {
                    _logger.LogError(e, e.Message);
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

                _logger.LogDebug($"Lease released. LeaseId={_blobLeaseClient.LeaseId}");
            }
            catch (Exception e)
            {
                _logger.LogError(e, e.Message);
            }
        }
    }
}