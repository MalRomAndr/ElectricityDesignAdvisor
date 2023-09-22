using NLog;
using NLog.Config;
using NLog.Targets;
using NLog.Targets.Gelf;
using NLog.Targets.Wrappers;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;

namespace CatalogWindow
{
    public static class Logger
    {
        private static Lazy<LogFactory> _instance = null;
        private static LoggingConfiguration _logConfig = null;

        private static Lazy<LogFactory> Instance
        {
            get
            {
                return _instance ?? (_instance = new Lazy<LogFactory>(BuildLogFactory));
            }
        }

        public static NLog.Logger GetLogger()
        {
            return Instance.Value.GetCurrentClassLogger();
        }

        private static LogFactory BuildLogFactory()
        {
            LoggingConfiguration config = _logConfig ?? new LoggingConfiguration();

            // Возьмем из настроек - собирать статистику или нет
            bool collectStats = true;

            if (collectStats)
            {
                Target gelfTarget = GelfTarget().Bufferize().MakeAsync();
                config.AddTarget(gelfTarget);
                config.AddRuleForAllLevels(gelfTarget);
            }

            LogFactory logFactory = new LogFactory
            {
                Configuration = config
            };

            try
            {
                // Не изящно, но расставляем минимальный и максимальный уровень логирования во все правила логирования
                config.LoggingRules.ToList().ForEach(
                    r => r.SetLoggingLevels(LogLevel.AllLevels.Min(), LogLevel.AllLevels.Max()));

                _logConfig = config;
            }
            catch (Exception ex)
            {
                // Логгера ещё нет, поэтому пока только сюда и можно вывести сообщения
                Debug.Write(ex);
            }

            return logFactory;
        }

        private static GelfTarget GelfTarget()
        {
            GelfTarget gelfTarget = new GelfTarget
            {
                Facility = "SmartLine",
                Endpoint = "udp://185.232.169.239:12204",
                Layout = "${message}",
                Name = "GelfUdp"
            };

            IList<GelfParameterInfo> gelfParameterInfos =
                new List<GelfParameterInfo>()
                {
                    new GelfParameterInfo()
                    {
                        Name = "installation_id",
                        Layout = "{3D8EF1C1-B8C5-4343-AFEF-DDA6B7573D69}"
                    }
                };

            foreach (GelfParameterInfo gelfParameterInfo in gelfParameterInfos)
            {
                gelfTarget.Parameters.Add(gelfParameterInfo);
            }

            return gelfTarget;
        }

        // Используем обётку https://github.com/nlog/NLog/wiki/AsyncWrapper-target для буферов и асинхронности
        private static Target MakeAsync(this Target targ)
        {
            return new AsyncTargetWrapper
            {
                BatchSize = 100,
                ForceLockingQueue = true,
                FullBatchSizeWriteLimit = 5,
                Name = targ.Name,
                OverflowAction = AsyncTargetWrapperOverflowAction.Grow,
                QueueLimit = 10000,
                TimeToSleepBetweenBatches = 1,
                WrappedTarget = targ
            };
        }

        private static Target Bufferize(this Target targ)
        {
            return new BufferingTargetWrapper
            {
                Name = targ.Name,
                OverflowAction = BufferingTargetWrapperOverflowAction.Flush,
                FlushTimeout = -1,
                BufferSize = 100,
                WrappedTarget = targ
            };
        }
    }
}