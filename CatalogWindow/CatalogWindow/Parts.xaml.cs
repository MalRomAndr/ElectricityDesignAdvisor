using System.Windows;

namespace CatalogWindow
{
    /// <summary>
    /// Interaction logic for Parts.xaml
    /// </summary>
    public partial class Parts : Window
    {
        private static readonly NLog.Logger logger = Logger.GetLogger();

        public Parts()
        {
            InitializeComponent();
        }

        private void OnButtonClickRecommend(object sender, RoutedEventArgs e)
        {
            logger.Info("Button_Recommend clicked");
            Close();
        }

        private void OnButtonClickSearch(object sender, RoutedEventArgs e)
        {
            logger.Warn("Button_Search clicked");
            Close();
        }
    }
}
