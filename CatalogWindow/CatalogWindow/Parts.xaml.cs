using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Data;
using System.Windows.Documents;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Imaging;
using System.Windows.Shapes;

namespace CatalogWindow
{
    /// <summary>
    /// Interaction logic for Parts.xaml
    /// </summary>
    public partial class Parts : Window
    {
        public Parts()
        {
            InitializeComponent();
        }

        private void OnButtonClickRecommend(object sender, RoutedEventArgs e)
        {
            Close();
        }

        private void OnButtonClickSearch(object sender, RoutedEventArgs e)
        {
            Close();
        }
    }
}
