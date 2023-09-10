﻿using NLog;
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

        public Parts()
        {
            InitializeComponent();            
            stopwatch.Start();            
        }

        private void OnButtonClickRecommend(object sender, RoutedEventArgs e)
        {
            stopwatch.Stop();
            logger.Info(GetLogEventInfo("CA 2000", true, stopwatch.Elapsed.TotalSeconds));
            Close();
        }

        private void OnButtonClickSearch(object sender, RoutedEventArgs e)
        {
            stopwatch.Stop();
            logger.Info(GetLogEventInfo("SLIP22.1", false, stopwatch.Elapsed.TotalSeconds));
            Close();
        }

        private LogEventInfo GetLogEventInfo(string partId, bool recommended, double elapsedSeconds)
        {
            LogEventInfo eventInfo = new LogEventInfo { Message = "Part selected" };
            eventInfo.Properties.Add("part_id", partId);
            eventInfo.Properties.Add("recommended", recommended.ToString());
            eventInfo.Properties.Add("elapsed_seconds", elapsedSeconds);
            return eventInfo;
        }
    }
}