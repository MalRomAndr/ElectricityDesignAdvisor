using System.Windows;
using NLog;


namespace CatalogWindow
{
    /// <summary>
    /// Interaction logic for MainWindow.xaml
    /// </summary>
    public partial class MainWindow : Window
    {
        public MainWindow()
        {
            InitializeComponent();

            LogManager.Setup().LoadConfiguration(builder => {
                builder.ForLogger().FilterMinLevel(LogLevel.Info).WriteToFile(fileName: "file.txt");
            });
        }

        private void Button_Click(object sender, RoutedEventArgs e)
        {
            var win = new Parts();
            win.ShowDialog();
        }
    }
}
