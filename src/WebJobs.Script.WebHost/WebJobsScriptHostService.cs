// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;
using System.Collections.ObjectModel;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs.Hosting;
using Microsoft.Azure.WebJobs.Logging;
using Microsoft.Azure.WebJobs.Script.Scale;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Microsoft.Azure.WebJobs.Script.WebHost
{
    public class WebJobsScriptHostService : IHostedService, IScriptHostManager, IDisposable
    {
        private readonly IServiceProvider _rootServiceProvider;
        private readonly IServiceScopeFactory _rootScopeFactory;
        private readonly IOptionsMonitor<ScriptApplicationHostOptions> _applicationHostOptions;
        private readonly ILogger _logger;
        private readonly IEnvironment _environment;
        private readonly HostPerformanceManager _performanceManager;
        private readonly IOptions<HostHealthMonitorOptions> _healthMonitorOptions;
        private readonly SlidingWindow<bool> _healthCheckWindow;
        private readonly Timer _hostHealthCheckTimer;

        private IHost _host;
        private CancellationTokenSource _startupLoopTokenSource;
        private int _hostStartCount;
        private bool _disposed = false;

        public WebJobsScriptHostService(IOptionsMonitor<ScriptApplicationHostOptions> applicationHostOptions, IServiceProvider rootServiceProvider,
            IServiceScopeFactory rootScopeFactory, ILoggerFactory loggerFactory, IEnvironment environment,
            HostPerformanceManager hostPerformanceManager, IOptions<HostHealthMonitorOptions> healthMonitorOptions)
        {
            if (loggerFactory == null)
            {
                throw new ArgumentNullException(nameof(loggerFactory));
            }

            _rootServiceProvider = rootServiceProvider ?? throw new ArgumentNullException(nameof(rootServiceProvider));
            _rootScopeFactory = rootScopeFactory ?? throw new ArgumentNullException(nameof(rootScopeFactory));
            _applicationHostOptions = applicationHostOptions ?? throw new ArgumentNullException(nameof(applicationHostOptions));
            _environment = environment ?? throw new ArgumentNullException(nameof(environment));
            _performanceManager = hostPerformanceManager ?? throw new ArgumentNullException(nameof(hostPerformanceManager));
            _healthMonitorOptions = healthMonitorOptions ?? throw new ArgumentNullException(nameof(healthMonitorOptions));

            _logger = loggerFactory.CreateLogger(ScriptConstants.LogCategoryHostGeneral);

            State = ScriptHostState.Default;

            if (ShouldMonitorHostHealth)
            {
                _healthCheckWindow = new SlidingWindow<bool>(_healthMonitorOptions.Value.HealthCheckWindow);
                _hostHealthCheckTimer = new Timer(OnHostHealthCheckTimer, null, TimeSpan.Zero, _healthMonitorOptions.Value.HealthCheckInterval);
            }
        }

        public IServiceProvider Services => _host?.Services;

        public ScriptHostState State { get; private set; }

        public Exception LastError { get; private set; }

        /// <summary>
        /// Gets a value indicating whether the host health monitor should be active.
        /// </summary>
        internal bool ShouldMonitorHostHealth
        {
            get
            {
                return _healthMonitorOptions.Value.Enabled && _environment.IsAppServiceEnvironment();
            }
        }

        public async Task StartAsync(CancellationToken cancellationToken)
        {
            _startupLoopTokenSource = new CancellationTokenSource();
            var startupLoopToken = _startupLoopTokenSource.Token;
            var tokenSource = CancellationTokenSource.CreateLinkedTokenSource(startupLoopToken, cancellationToken);

            try
            {
                await StartHostAsync(tokenSource.Token);
            }
            catch (OperationCanceledException)
            {
                if (cancellationToken.IsCancellationRequested)
                {
                    _logger.LogInformation("Initialization cancellation requested by runtime.");
                    throw;
                }

                // If the exception was triggered by our loop cancellation token, just ignore as
                // it doesn't indicate an issue.
            }
        }

        private async Task StartHostAsync(CancellationToken cancellationToken, int attemptCount = 0, bool skipHostJsonConfiguration = false)
        {
            cancellationToken.ThrowIfCancellationRequested();

            ScriptHost scriptHost = null;
            try
            {
                // if we were in an error state retain that,
                // otherwise move to default
                if (State != ScriptHostState.Error)
                {
                    State = ScriptHostState.Default;
                }

                bool isOffline = CheckAppOffline();

                _host = BuildHost(isOffline, skipHostJsonConfiguration);

                scriptHost = (ScriptHost)_host.Services.GetService<ScriptHost>();
                scriptHost.HostInitializing += OnHostInitializing;

                LogInitialization(_host, isOffline, attemptCount, _hostStartCount++);

                await _host.StartAsync(cancellationToken);

                // This means we had an error on a previous load, so we want to keep the LastError around
                if (!skipHostJsonConfiguration)
                {
                    LastError = null;
                }

                if (!isOffline && !skipHostJsonConfiguration)
                {
                    State = ScriptHostState.Running;
                }
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (Exception exc)
            {
                LastError = exc;
                State = ScriptHostState.Error;
                attemptCount++;

                ILogger logger = GetHostLogger(_host);
                logger.LogError(exc, "A host error has occurred");

                if (ShutdownHostIfUnhealthy())
                {
                    return;
                }

                var orphanTask = Orphan(_host, logger)
                    .ContinueWith(t =>
                        {
                            if (t.IsFaulted)
                            {
                                t.Exception.Handle(e => true);
                            }
                        }, TaskContinuationOptions.ExecuteSynchronously);

                cancellationToken.ThrowIfCancellationRequested();

                if (exc is HostConfigurationException)
                {
                    // Try starting the host without parsing host.json. This will start up a
                    // minimal host and allow the portal to see the error. Any modification will restart again.
                    Task ignore = StartHostAsync(cancellationToken, attemptCount, skipHostJsonConfiguration: true);
                }
                else
                {
                    await Utility.DelayWithBackoffAsync(attemptCount, cancellationToken, min: TimeSpan.FromSeconds(1), max: TimeSpan.FromMinutes(2))
                        .ContinueWith(t =>
                        {
                            cancellationToken.ThrowIfCancellationRequested();
                            return StartHostAsync(cancellationToken, attemptCount);
                        });
                }
            }
        }

        public async Task StopAsync(CancellationToken cancellationToken)
        {
            _startupLoopTokenSource?.Cancel();

            State = ScriptHostState.Stopping;
            _logger.LogInformation("Stopping host...");

            var currentHost = _host;
            Task stopTask = Orphan(currentHost, _logger, cancellationToken);
            Task result = await Task.WhenAny(stopTask, Task.Delay(TimeSpan.FromSeconds(10)));

            if (result != stopTask)
            {
                _logger.LogWarning("Host did not shutdown within its allotted time.");
            }
            else
            {
                _logger.LogInformation("Host shutdown completed.");
            }

            State = ScriptHostState.Stopped;
        }

        public async Task RestartHostAsync(CancellationToken cancellationToken)
        {
            if (State == ScriptHostState.Stopping || State == ScriptHostState.Stopped)
            {
                return;
            }

            _startupLoopTokenSource?.Cancel();

            State = ScriptHostState.Default;

            _logger.LogInformation("Restarting host.");

            var previousHost = _host;
            Task startTask = StartAsync(cancellationToken);
            Task stopTask = Orphan(previousHost, _logger, cancellationToken);

            await startTask;

            _logger.LogInformation("Host restarted.");
        }

        private void OnHostInitializing(object sender, EventArgs e)
        {
            // we check host health before starting to avoid starting
            // the host when connection or other issues exist
            IsHostHealthy(throwWhenUnhealthy: true);
        }

        private IHost BuildHost(bool isOffline = false, bool skipHostJsonConfiguration = false)
        {
            var builder = new HostBuilder();

            if (skipHostJsonConfiguration)
            {
                builder.ConfigureAppConfiguration((context, _) =>
                {
                    context.Properties[ScriptConstants.SkipHostJsonConfigurationKey] = true;
                });
            }

            builder.SetAzureFunctionsEnvironment()
                .AddWebScriptHost(_rootServiceProvider, _rootScopeFactory, _applicationHostOptions.CurrentValue);

            if (isOffline)
            {
                builder.ConfigureServices(services =>
                {
                    // When offline, we need most general services registered so admin
                    // APIs can function. However, we want to prevent the ScriptHost from
                    // actually starting up. To accomplish this, we remove the host service
                    // responsible for starting the job hst.
                    var jobHostService = services.FirstOrDefault(p => p.ImplementationType == typeof(JobHostService));
                    services.Remove(jobHostService);
                });
            }

            return builder.Build();
        }

        private ILogger GetHostLogger(IHost host)
        {
            var hostLoggerFactory = _host?.Services.GetService<ILoggerFactory>();

            // Attempt to get the host logger with JobHost configuration applied
            // using the default logger as a fallback
            return hostLoggerFactory?.CreateLogger(LogCategories.Startup) ?? _logger;
        }

        private void LogInitialization(IHost host, bool isOffline, int attemptCount, int v)
        {
            var logger = GetHostLogger(host);

            var log = isOffline ? "Host is offline." : "Initializing Host.";
            logger.LogInformation(log);
            logger.LogInformation($"Host initialization: ConsecutiveErrors={attemptCount}, StartupCount={_hostStartCount++}");
        }

        private bool CheckAppOffline()
        {
            // check if we should be in an offline state
            string offlineFilePath = Path.Combine(_applicationHostOptions.CurrentValue.ScriptPath, ScriptConstants.AppOfflineFileName);
            if (File.Exists(offlineFilePath))
            {
                State = ScriptHostState.Offline;
                return true;
            }

            return false;
        }

        private void OnHostHealthCheckTimer(object state)
        {
            bool isHealthy = IsHostHealthy();
            _healthCheckWindow.AddEvent(isHealthy);

            if (!isHealthy && State == ScriptHostState.Running)
            {
                // This periodic check allows us to break out of the host run
                // loop. The health check performed in OnHostInitializing will then
                // fail and we'll enter a restart loop (exponentially backing off)
                // until the host is healthy again and we can resume host processing.
                var message = "Host is unhealthy. Initiating a restart.";
                _logger.LogError(0, message);
                var tIgnore = RestartHostAsync(CancellationToken.None);
            }
        }

        internal bool IsHostHealthy(bool throwWhenUnhealthy = false)
        {
            if (!ShouldMonitorHostHealth)
            {
                return true;
            }

            var exceededCounters = new Collection<string>();
            if (_performanceManager.IsUnderHighLoad(exceededCounters))
            {
                string formattedCounters = string.Join(", ", exceededCounters);
                if (throwWhenUnhealthy)
                {
                    throw new InvalidOperationException($"Host thresholds exceeded: [{formattedCounters}]. For more information, see https://aka.ms/functions-thresholds.");
                }
                return false;
            }

            return true;
        }

        private bool ShutdownHostIfUnhealthy()
        {
            if (ShouldMonitorHostHealth && _healthCheckWindow.GetEvents().Where(isHealthy => !isHealthy).Count() > _healthMonitorOptions.Value.HealthCheckThreshold)
            {
                // if the number of times the host has been unhealthy in
                // the current time window exceeds the threshold, recover by
                // initiating shutdown
                var message = $"Host unhealthy count exceeds the threshold of {_healthMonitorOptions.Value.HealthCheckThreshold} for time window {_healthMonitorOptions.Value.HealthCheckWindow}. Initiating shutdown.";
                _logger.LogError(0, message);
                var environment = _rootServiceProvider.GetService<IScriptJobHostEnvironment>();
                environment.Shutdown();
                return true;
            }

            return false;
        }

        /// <summary>
        /// Remove the <see cref="IHost"/> instance from the live instances collection,
        /// allowing it to finish currently executing functions before stopping and disposing of it.
        /// </summary>
        /// <param name="instance">The <see cref="IHost"/> instance to remove</param>
        private async Task Orphan(IHost instance, ILogger logger, CancellationToken cancellationToken = default)
        {
            //instance.HostInitializing -= OnHostInitializing;

            try
            {
                await (instance?.StopAsync(cancellationToken) ?? Task.CompletedTask);
            }
            catch (Exception ex)
            {
                _logger.LogTrace(ex, "Error stopping and disposing of host");
            }
            finally
            {
                instance?.Dispose();
            }
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!_disposed)
            {
                if (disposing)
                {
                    _startupLoopTokenSource?.Dispose();
                }
                _disposed = true;
            }
        }

        public void Dispose() => Dispose(true);
    }
}
