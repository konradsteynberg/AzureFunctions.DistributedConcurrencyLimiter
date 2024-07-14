using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using CCBA.Integrations.Base.Abstracts;
using CCBA.Integrations.Base.Enums;
using CCBA.Integrations.Base.Helpers;
using CCBA.Integrations.Base.Models;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace CCBA.Testing.ConcurrencyLimiterService.Services;

/// <summary>
/// Developer: Johan Nieuwenhuis, Konrad Steynberg
/// </summary>
public abstract class BaseLogger : BaseConfiguration
{
    private readonly ILogger<BaseLogger> _logger;

    protected BaseLogger(ILogger<BaseLogger> logger, IConfiguration configuration) : base(configuration)
    {
        _logger = logger;
        var strings = GetType().FullName?.Split('.');
        Tracking.Add("Namespace", GetType().FullName);
        BaseDescription = strings?.Last();
        S = Assembly.GetExecutingAssembly()?.GetName()?.Version?.ToString();
    }

    public string BaseDescription { get; set; }
    public string InterfaceName { get; set; }
    public string Source { get; set; }
    public string Target { get; set; }
    protected Tracking Tracking { get; set; } = new();
    private string JobRunId { get; set; }
    private string LegalEntity { get; set; }
    private string ProgramId { get; set; }
    private string S { get; }

    public Dictionary<string, string> TrackerToProperties(Dictionary<string, string> dictionary)
    {
        var properties = new Dictionary<string, string>();
        foreach (var keyValuePair in Tracking)
        {
            if (!properties.ContainsKey(keyValuePair.Key))
            {
                var type = keyValuePair.Value.GetType();

                var value = IsSimpleType(type) ? keyValuePair.Value.ToString() : keyValuePair.Value.ToJson();
                properties.TryAdd(keyValuePair.Key, value);
            }
        }

        if (dictionary != null)
            foreach (var property in dictionary)
            {
                if (!properties.ContainsKey(property.Key)) properties.TryAdd(property.Key, property.Value);
            }

        return properties;
    }

    protected void AppExceptionLogger(string message, EErrorCode errorCode, LogLevel logLevel, Exception exception, string source = null, string target = null, List<JobData> jobData = null, [CallerMemberName] string method = null, Dictionary<string, string> properties = null)
    {
        var logger = new Logger(JobRunId, ProgramId, LegalEntity, InterfaceName, Source, Target, method, message, logLevel, jobData, errorCode, exception, Tracking);

        properties ??= new Dictionary<string, string>();
        properties["ErrorCode"] = errorCode.ToString();
        properties["MessageType"] = logger.MessageType;
        properties["Job"] = logger.Job.ToJson();

        LogException(message, exception, logLevel, source, target, jobData, method, properties);
    }

    protected void AppFailureLogger(string source = null, string target = null, [CallerMemberName] string method = null)
    {
        var logger = new AppFailureLogger(JobRunId, ProgramId, LegalEntity, InterfaceName, Source, Target, method);

        var properties = new Dictionary<string, string>
        {
            { "Job", logger.Job.ToJson() },
            { "JobStatus", logger.JobStatus },
            { "MessageType", logger.MessageType },
        };
        LogInformation("App Failure", LogLevel.Critical, source, target, new List<JobData>(), method, TrackerToProperties(properties));
    }

    protected void AppStartLogger(ExecutionContext context, string legalEntity, string source = null, string target = null, [CallerMemberName] string method = null)
    {
        Source = source;
        Target = target;
        JobRunId = context.InvocationId.ToString();
        ProgramId = context.FunctionName;

        LegalEntity = legalEntity;

        var logger = new AppStartLogger(JobRunId, ProgramId, LegalEntity, InterfaceName, source, target, method);

        var properties = new Dictionary<string, string>
        {
            { "Job", logger.Job.ToJson() },
            { "JobStatus", logger.JobStatus },
            { "MessageType", logger.MessageType },
        };
        LogInformation("App Start", LogLevel.Information, source, target, new List<JobData>(), method, TrackerToProperties(properties));
    }

    protected void AppStartLogger(string jobRunId, string programId, string legalEntity, string source = null, string target = null, [CallerMemberName] string method = null)
    {
        Source = source;
        Target = target;
        JobRunId = jobRunId;
        ProgramId = programId;
        LegalEntity = legalEntity;

        var logger = new AppStartLogger(JobRunId, ProgramId, LegalEntity, InterfaceName, source, target, method);

        var properties = new Dictionary<string, string>
        {
            { "Job", logger.Job.ToJson() },
            { "JobStatus", logger.JobStatus },
            { "MessageType", logger.MessageType },
        };
        LogInformation("App Start", LogLevel.Information, source, target, new List<JobData>(), method, TrackerToProperties(properties));
    }

    protected void AppSuccessLogger(string source = null, string target = null, [CallerMemberName] string method = null)
    {
        var logger = new AppSuccessLogger(JobRunId, ProgramId, LegalEntity, InterfaceName, Source, Target, method);

        var properties = new Dictionary<string, string>
        {
            { "Job", logger.Job.ToJson() },
            { "JobStatus", logger.JobStatus },
            { "MessageType", logger.MessageType },
        };
        LogInformation("App Success", LogLevel.Information, source, target, new List<JobData>(), method, TrackerToProperties(properties));
    }

    protected void LogException(Exception exception, LogLevel logLevel = LogLevel.Critical, string source = null, string target = null, List<JobData> jobData = null, [CallerMemberName] string method = null, Dictionary<string, string> properties = null)
    {
        LogException(exception.Message, exception, logLevel, source, target, jobData, method, TrackerToProperties(properties));
    }

    protected void LogException(string message, Exception exception, LogLevel logLevel = LogLevel.Critical, string source = null, string target = null, List<JobData> jobData = null, [CallerMemberName] string method = null, Dictionary<string, string> properties = null)
    {
        properties ??= new Dictionary<string, string>();
        properties["Exception"] = exception.ToJson();

        var formattedMessage = BuildFormattedMessage($"{message}\n{exception.GetAllMessages()}", source, target, jobData, method, properties);

        using (_logger.BeginScope(formattedMessage))
        {
            _logger.Log(logLevel, message);
        }
    }

    protected void LogInformation(string message = null, LogLevel logLevel = LogLevel.Information, string source = null, string target = null, List<JobData> jobData = null, [CallerMemberName] string method = null, Dictionary<string, string> properties = null)
    {
        LogTrace(method, message, logLevel, source, target, jobData, TrackerToProperties(properties));
    }

    protected void LogTrace(string method, string message, LogLevel logLevel, string source = null, string target = null, List<JobData> jobData = null, Dictionary<string, string> properties = null)
    {
        properties ??= new Dictionary<string, string>();

        var formattedMessage = BuildFormattedMessage(message, source, target, jobData, method, properties);

        var dictionary = formattedMessage.ToDictionary(s => s.Key, s => (object)s.Value);
        using (_logger.BeginScope(dictionary))
        {
            _logger.Log(logLevel, message);
        }
    }

    /// <summary>
    /// Replaces content with blob url if content is too big to fit in application insights property field
    /// </summary>
    /// <param name="content"></param>
    /// <returns></returns>
    private static string GetApplicationInsightsSafeValue(string content)
    {
        if (content == null) return null;
        return content.Length > 8192 ? BlobExtensions.UploadBlobWithSasUri("payloads", $"{DateTime.UtcNow:yyyyMMdd}/{Guid.NewGuid()}", new BinaryData(content)) : content;
    }

    private static bool IsSimpleType(Type type)
    {
        return
            type.IsValueType ||
            type.IsPrimitive ||
            new Type[] {
                typeof(string),
                typeof(decimal),
                typeof(DateTime),
                typeof(DateTimeOffset),
                typeof(TimeSpan),
                typeof(Guid)
            }.Contains(type) ||
            Convert.GetTypeCode(type) != TypeCode.Object;
    }

    private Dictionary<string, string> BuildFormattedMessage(string message, string source, string target, List<JobData> jobData, string method, Dictionary<string, string> properties)
    {
        if (!string.IsNullOrEmpty(JobRunId)) properties["JobRunId"] = JobRunId;
        if (!string.IsNullOrEmpty(ProgramId)) properties["ProgramId"] = ProgramId;
        if (!string.IsNullOrEmpty(InterfaceName)) properties["InterfaceName"] = InterfaceName;
        if (!string.IsNullOrEmpty(LegalEntity)) properties["LegalEntity"] = LegalEntity;
        if (!string.IsNullOrEmpty(source)) properties["Source"] = source;
        if (!string.IsNullOrEmpty(target)) properties["Target"] = target;
        if (!string.IsNullOrEmpty(method)) properties["Method"] = method;
        if (jobData != null) properties["JobData"] = jobData.ToJson();
        properties["Internal"] = $@"{S}";
        foreach (var (key, value) in properties.ToList()) properties[key] = GetApplicationInsightsSafeValue(value);
        return properties;
    }
}