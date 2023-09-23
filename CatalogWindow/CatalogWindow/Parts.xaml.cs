using NLog;
using System.Collections.Generic;
using System.Diagnostics;
using System.Windows;

namespace CatalogWindow
{
    /// <summary>
    /// Interaction logic for Parts.xaml
    /// </summary>
    public partial class Parts : Window
    {
        private static readonly NLog.Logger logger = Logger.GetLogger();
        private readonly Stopwatch stopwatch = new Stopwatch();

        private readonly List<string> parts = new List<string>()
        {
            "SO250.01R",
            "СВ95-3",
            "COT36R",
            "COT37R",
            "SOT29.10R",
        };

        public Parts()
        {
            InitializeComponent();
            stopwatch.Start();
        }

        private void OnButtonClickRecommend(object sender, RoutedEventArgs e)
        {
            stopwatch.Stop();
            LogEventInfo eventInfo = MakeLogEventInfo(
                "CA 2000", true, stopwatch.Elapsed.TotalSeconds, parts, "А10-2", "support", "1.0.0.0");
#if DEBUG
            logger.Debug(eventInfo);
#else
            logger.Info(eventInfo);
#endif
            Close();
        }

        private void OnButtonClickSearch(object sender, RoutedEventArgs e)
        {
            stopwatch.Stop();
            LogEventInfo eventInfo = MakeLogEventInfo(
                "CA 2000", false, stopwatch.Elapsed.TotalSeconds, parts, "А10-2", "support", "1.0.0.0");
#if DEBUG
            logger.Debug(eventInfo);
#else
            logger.Info(eventInfo);
#endif
            Close();
        }

        private LogEventInfo MakeLogEventInfo(
            string partId,
            bool recommended,
            double elapsedSeconds,
            List<string> parts,
            string structureName,
            string structureType,
            string appVersion)
        {
            LogEventInfo eventInfo = new LogEventInfo { Message = "Part selected" };
            eventInfo.Properties.Add("part_id", partId);
            eventInfo.Properties.Add("recommended", recommended.ToString());
            eventInfo.Properties.Add("elapsed_seconds", elapsedSeconds);
            eventInfo.Properties.Add("parts", parts);
            eventInfo.Properties.Add("structure_name", structureName);
            eventInfo.Properties.Add("structure_type", structureType);
            eventInfo.Properties.Add("app_verison", appVersion);
            return eventInfo;
        }
    }
}
