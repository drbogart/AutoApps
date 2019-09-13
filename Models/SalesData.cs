using System;
using System.IO;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Data;
using System.Globalization;
using Microsoft.AspNetCore.Mvc.Rendering;
using System.Data.SqlClient;
using bogart_wireless.Libraries;


namespace bogart_wireless.Models
{


    public class SalesData
    {

        // private objects we can object throughout the class
        private SqlConnectionStringBuilder builder = new SqlConnectionStringBuilder();
        private SqlConnection connection;

        // properties
        private DateTime firstPOSUploadDate;
        public DateTime lastPOSUploadDate;
        public DateTime lastTrafficUploadDate;
        public DateTime calcStartDate;
        public DateTime calcEndDate { get; set; }

        private readonly List<string> _months = new List<string>();
        public List<string> activeReps = new List<string>();
        public List<Promo> customerPromos = new List<Promo>();
        public List<Promo> businessPromos = new List<Promo>();
        public List<Promo> employeePromos = new List<Promo>();
        public List<Promo> expiredPromos = new List<Promo>();

        // declare array of struct Projections to store company projections
        public Projection[] Company = new Projection[Enum.GetNames(typeof(ProjectionsDataType)).Length];

        // declare array defining stores
        public StoreDef[] StoreList =
            new StoreDef[]{
                new StoreDef()  {wzNum = "All Stores", storeName = "Company", dbName = "%"},
                new StoreDef()  {wzNum = "WZ-192", storeName = "Lockport", dbName = "Wireless Zone Lockport WZ192"},
                new StoreDef()  {wzNum = "WZ-298", storeName = "East Aurora", dbName = "Wireless Zone East Aurora WZ298"},
                new StoreDef()  {wzNum = "WZ-299", storeName = "Springville", dbName = "Wireless Zone Springville WZ299"}
        };



        // declare array to maintain store projections
        public Projection[,] StoreProjection = new Projection[Enum.GetNames(typeof(Stores)).Length, Enum.GetNames(typeof(ProjectionsDataType)).Length];

        //declare array to store quality data
        public Quality[] QualityData = new Quality[6];
        public Quality[] RepQualityData = new Quality[20];

        private int _selectedMonth;
        public string SelectedMonth { get; set; }
        public string DateRangeType { get; set; }


        // static configuration data that applies to all objects of this type
        public static IEmailConfiguration emailConfiguration; // for email
        public static DatabaseConnectionSettings dbSettings; // for database connection


        // data that can be accessed outside 
        public IEnumerable<SelectListItem> MonthList
        {
            get { return new SelectList(_months); }
        }


        public SalesData()
        {



            /* establish connection parameters for the specified environment */
            builder.DataSource = dbSettings.DataSource;
            builder.UserID = dbSettings.UserID;
            builder.Password = dbSettings.Password;
            builder.InitialCatalog = dbSettings.InitialCatalog;
            builder.MultipleActiveResultSets = true;

            // open the connection
            connection = new SqlConnection(builder.ConnectionString);
            connection.Open();

            // get first and last POS upload date
            getDataLoadDates();
            //calcEndDate = lastPOSUploadDate;

            // get first day of current month
            DateTime firstOfMonth = lastPOSUploadDate.AddDays(-lastPOSUploadDate.Day + 1);

            // 
            //getCompanyGP(firstOfMonth.ToString("d"), lastPOSUploadDate.ToString("G"));

            // last year numbers
            DateTime lyFromDate = new DateTime();
            DateTime lyToDate = new DateTime();
            lyFromDate = firstOfMonth.AddYears(-1);
            lyToDate = lastPOSUploadDate.AddYears(-1);

            // load promos
            loadPromos();
        }

        public int getSelectedMonth()
        {
            return _selectedMonth;

        }

        public DateTime getLastPOSUploadDate()
        {
            return lastPOSUploadDate;

        }

        public DateTime getFirstPOSUploadDate()
        {
            return firstPOSUploadDate;

        }

        /* check the date for which we last calculated commissions */
        public DateTime getLastCommissionCalcDate()
        {
            DateTime lastCalcDate = new DateTime();

            String queryString = "Select endDate from bogart_2.commissionQualifiers where Category = 'LAST_CALC_DATE'";

            // Execute the query
            SqlCommand command = new SqlCommand(queryString, connection);
            command.CommandTimeout = 600;
            SqlDataReader reader = command.ExecuteReader();

            // get the return data
            while (reader.Read())
            {
                lastCalcDate = reader.GetDateTime(0);
            }

            // close reader 
            reader.Close();

            return lastCalcDate;

        }

        public void getActiveReps(DateTime startDate, DateTime endDate)
        {
            // delete anything currently in the list
            activeReps.Clear();

            String queryString = "SELECT  pd.SoldBy,SUM(pd.Qty) FROM bogart_2.productdetails pd, bogart_2.productskus ps WHERE" +
                " pd.ProductSKU = ps.ProductSKU " +
                " AND ps.VoiceLine = 1 " +
                " AND SoldOn BETWEEN '" + startDate.ToString("d") + "' AND '" + endDate.ToString("G") + "' " +
                " AND pd.SoldBy IN(SELECT UserName FROM bogart_2.users WHERE Active = 1 and SalesRepEffectiveDate < '" + endDate.ToString("d") + "')" +
                " group BY SoldBy HAVING SUM(pd.qty) > 0";

            // Execute the query
            SqlCommand command = new SqlCommand(queryString, connection);
            command.CommandTimeout = 600;
            SqlDataReader reader = command.ExecuteReader();

            while (reader.Read())
            {
                activeReps.Add(reader.GetString(0));
            }

            // close reader 
            reader.Close();

        }

        // get date of last POS upload
        private void queryLastPOSUploadDate()
        {
            string queryString;

            // define the query string
            queryString = "Select max(SoldOn) from bogart_2.productdetails";

            // Execute the query
            SqlCommand command = new SqlCommand(queryString, connection);
            SqlDataReader reader = command.ExecuteReader();
            Console.WriteLine(queryString);

            reader.Read();

            // will only be one row.  MAX can't return more
            lastPOSUploadDate = reader.GetDateTime(0);

            // close reader 
            reader.Close();

        }

        // get date of first POS upload
        private void queryFirstPOSUploadDate()
        {
            string queryString;

            // define the query string
            queryString = "Select min(SoldOn) from bogart_2.productdetails";

            // Execute the query
            SqlCommand command = new SqlCommand(queryString, connection);
            SqlDataReader reader = command.ExecuteReader();
            Console.WriteLine(queryString);

            reader.Read();

            // will only be one row.  MAX can't return more
            firstPOSUploadDate = reader.GetDateTime(0);


            // close reader 
            reader.Close();

        }

        public void getDataLoadDates()
        {
            DateTime convertDate;

            // get load dates
            queryFirstPOSUploadDate();
            queryLastPOSUploadDate();
            queryLastTrafficUploadDate();

            // generate list of months containing data
            convertDate = new DateTime(lastPOSUploadDate.Year, lastPOSUploadDate.Month, 1);
            do
            {
                _months.Add(convertDate.ToString("Y"));
                convertDate = convertDate.AddMonths(-1);
            } while (convertDate >= firstPOSUploadDate);

            SelectedMonth = _months[0];
        }

        // get date of last traffic upload
        private void queryLastTrafficUploadDate()
        {
            string queryString;

            // define the query string
            queryString = "Select max(Date) from bogart_2.trafficCountsDailyByStore";

            // Execute the query
            SqlCommand command = new SqlCommand(queryString, connection);
            SqlDataReader reader = command.ExecuteReader();
            Console.WriteLine(queryString);

            reader.Read();

            // will only be one row.  MAX can't return more
            lastTrafficUploadDate = reader.GetDateTime(0);

            // close reader 
            reader.Close();

        }

        /* get the total company-wide GP for a specified date period */
        public decimal getCompanyGP(string startDate, string endDate)
        {
            string queryString;
            decimal result;

            // define the query string
            queryString = "Select sum(GrossProfit) from bogart_2.productdetails where SoldOn Between '" + startDate + "' AND '" + endDate + "'";
            queryString = "Select sum(GrossProfit) from #ProductDetails where SoldOn Between '" + startDate + "' AND '" + endDate + "'";

            // Execute the query
            SqlCommand command = new SqlCommand(queryString, connection);
            SqlDataReader reader = command.ExecuteReader();
            Console.WriteLine(queryString);

            reader.Read();

            // will only be one row.  MAX can't return more
            result = reader.GetDecimal(0);

            // close reader 
            reader.Close();
            return result;
        }

        /* get the number of company new lines for a specified period */
        public decimal getCompanyNewLines(string startDate, string endDate)
        {
            string queryString;
            decimal result;

            // define the query string
            queryString = "Select sum(Qty) from bogart_2.productdetails  where SoldOn Between '" + startDate + "' AND '" + endDate + "'" +
                           "AND ProductSKU IN(Select ProductSKU from bogart_2.productSKUS where newLine = 1) " +
                           "AND CONVERT(date, SoldOn) <> SoldOn";
            queryString = "Select sum(Qty) from #ProductDetails where " +
               "ProductSKU IN(Select ProductSKU from bogart_2.productSKUS where newLine = 1) " +
               "AND CONVERT(date, SoldOn) <> SoldOn AND SoldOn Between '" + startDate + "' AND '" + endDate + "'";

            // Execute the query  
            SqlCommand command = new SqlCommand(queryString, connection);
            SqlDataReader reader = command.ExecuteReader();
            Console.WriteLine(queryString);

            reader.Read();

            // will only be one row.  MAX can't return more
            result = reader.GetDecimal(0);

            // close reader 
            reader.Close();
            return result;
        }

        /* get the number of company new lines for a specified period */
        public decimal getCompanyUpgrades(string startDate, string endDate)
        {
            string queryString;
            decimal result;

            // define the query string
            queryString = "Select sum(Qty) from bogart_2.productdetails  where SoldOn Between '" + startDate + "' AND '" + endDate + "' " +
                           "AND ProductSKU IN(Select ProductSKU from bogart_2.productSKUS where Upgrade = 1) " +
                           "AND CONVERT(date, SoldOn) <> SoldOn";
            queryString = "Select sum(Qty) from #ProductDetails where SoldOn Between '" + startDate + "' AND '" + endDate + "' " +
               "AND ProductSKU IN(Select ProductSKU from bogart_2.productSKUS where Upgrade = 1) " +
               "AND CONVERT(date, SoldOn) <> SoldOn";

            // Execute the query  
            SqlCommand command = new SqlCommand(queryString, connection);
            SqlDataReader reader = command.ExecuteReader();
            Console.WriteLine(queryString);

            reader.Read();

            // will only be one row.  MAX can't return more
            result = reader.GetDecimal(0);

            // close reader 
            reader.Close();
            return result;
        }

        /* get the number of company new lines for a specified period */
        public int getCompanyVoiceLines(string startDate, string endDate)
        {
            string queryString;
            int result;

            // define the query string
            queryString = "Select sum(Qty) from #ProductDetails where SoldOn Between '" + startDate + "' AND '" + endDate + "' " +
               "AND ProductSKU IN(Select ProductSKU from bogart_2.productSKUS where VoiceLine = 1) " +
               "AND CONVERT(date, SoldOn) <> SoldOn";

            // Execute the query  
            SqlCommand command = new SqlCommand(queryString, connection);
            SqlDataReader reader = command.ExecuteReader();
            Console.WriteLine(queryString);

            reader.Read();

            // will only be one row.  MAX can't return more
            result = (int)reader.GetDecimal(0);

            // close reader 
            reader.Close();
            return result;
        }
        /* get the number of company new lines for a specified period */
        public decimal getCompanyNewStrategicGrowth(string startDate, string endDate)
        {
            string queryString;
            decimal result;

            // define the query string
            queryString = "Select sum(Qty) from #ProductDetails where SoldOn Between '" + startDate + "' AND '" + endDate + "' " +
               "AND ProductSKU IN(Select ProductSKU from bogart_2.productSKUS where NewStrategicGrowth = 1) " +
               "AND CONVERT(date, SoldOn) <> SoldOn";

            // Execute the query  
            SqlCommand command = new SqlCommand(queryString, connection);
            SqlDataReader reader = command.ExecuteReader();
            Console.WriteLine(queryString);

            reader.Read();

            // will only be one row.  MAX can't return more
            result = reader.GetDecimal(0);

            // close reader 
            reader.Close();
            return result;
        }

        public decimal getStoreGP(string startDate, string endDate, string store)
        {
            string queryString;
            decimal result;

            // define the query string
            queryString = "Select sum(GrossProfit) from bogart_2.productdetails where InvoicedAt = '" + store + "' AND SoldOn Between '" + startDate + "' AND '" + endDate + "'";

            // Execute the query
            SqlCommand command = new SqlCommand(queryString, connection);
            SqlDataReader reader = command.ExecuteReader();
            Console.WriteLine(queryString);

            reader.Read();

            // will only be one row.  MAX can't return more
            result = reader.GetDecimal(0);

            // close reader 
            reader.Close();
            return result;
        }

        public decimal getStoreNewLines(string startDate, string endDate, string store)
        {
            string queryString;
            decimal result;

            // define the query string
            queryString = "Select sum(Qty) from bogart_2.productdetails  where InvoicedAt = '" + store + "' AND SoldOn Between '" + startDate + "' AND '" + endDate + "' " +
                           "AND ProductSKU IN(Select ProductSKU from bogart_2.productSKUS where newLine = 1) " +
                           "AND CONVERT(date, SoldOn) <> SoldOn";

            // Execute the query  
            SqlCommand command = new SqlCommand(queryString, connection);
            SqlDataReader reader = command.ExecuteReader();
            Console.WriteLine(queryString);

            reader.Read();

            // will only be one row.  MAX can't return more
            if (!reader.IsDBNull(0))
            {
                result = reader.GetDecimal(0);
            }
            else
            {
                result = 0;
            }

            // close reader 
            reader.Close();
            return result;
        }

        /* get upgrades for specified store for specified date range */
        public decimal getStoreUpgrades(string startDate, string endDate, string store)
        {
            string queryString;
            decimal result;

            // define the query string
            queryString = "Select sum(Qty) from #ProductDetails where InvoicedAt = '" + store + "' AND SoldOn Between '" + startDate + "' AND '" + endDate + "' " +
                           "AND ProductSKU IN(Select ProductSKU from bogart_2.productSKUS where Upgrade = 1) " +
                           "AND CONVERT(date, SoldOn) <> SoldOn";

            // Execute the query  
            SqlCommand command = new SqlCommand(queryString, connection);
            SqlDataReader reader = command.ExecuteReader();
            Console.WriteLine(queryString);

            reader.Read();

            // will only be one row.  MAX can't return more
            if (!reader.IsDBNull(0))
            {
                result = reader.GetDecimal(0);
            }
            else
            {
                result = 0;
            }

            // close reader 
            reader.Close();
            return result;
        }

        /* get voice lines for specified store for specified date range */
        public int getStoreVoiceLines(string startDate, string endDate, string store)
        {
            string queryString;
            int result;

            // define the query string
            queryString = "Select sum(Qty) from #ProductDetails where InvoicedAt = '" + store + "' AND SoldOn Between '" + startDate + "' AND '" + endDate + "' " +
                           "AND ProductSKU IN(Select ProductSKU from bogart_2.productSKUS where VoiceLine = 1) " +
                           "AND CONVERT(date, SoldOn) <> SoldOn";

            // Execute the query  
            SqlCommand command = new SqlCommand(queryString, connection);
            SqlDataReader reader = command.ExecuteReader();
            Console.WriteLine(queryString);

            reader.Read();

            // will only be one row.  MAX can't return more
            if (!reader.IsDBNull(0))
            {
                result = (int)reader.GetDecimal(0);
            }
            else
            {
                result = 0;
            }

            // close reader 
            reader.Close();
            return result;
        }


        /* get new strategic growth for specified store for specified date range */
        public decimal getStoreNewStrategicGrowth(string startDate, string endDate, string store)
        {
            string queryString;
            decimal result;

            // define the query string
            queryString = "Select sum(Qty) from #ProductDetails where InvoicedAt = '" + store + "' AND SoldOn Between '" + startDate + "' AND '" + endDate + "' " +
                           "AND ProductSKU IN(Select ProductSKU from bogart_2.productSKUS where NewStrategicGrowth = 1) " +
                           "AND CONVERT(date, SoldOn) <> SoldOn";

            // Execute the query  
            SqlCommand command = new SqlCommand(queryString, connection);
            SqlDataReader reader = command.ExecuteReader();
            Console.WriteLine(queryString);

            reader.Read();

            // will only be one row.  MAX can't return more
            if (!reader.IsDBNull(0))
            {
                result = reader.GetDecimal(0);
            }
            else
            {
                result = 0;
            }

            // close reader 
            reader.Close();
            return result;
        }


        /* perform a query */
        private string[] query(string queryString)
        {
            // Execute the query
            SqlCommand command = new SqlCommand(queryString, connection);
            SqlDataReader reader = command.ExecuteReader();
            Console.WriteLine(queryString);

            // parse returned data into array
            List<string> list = (from IDataRecord r in reader
                                 select (string)r["FieldName"]
                    ).ToList();
            Console.WriteLine($"Returned {list.Count()} rows");
            // return number of rows
            return list.ToArray();

        }

        /* execute a query on productDetails that will return a single long value */
        public String stringValueQuery(string queryString)
        {

            String result = "";


            // Execute the query  
            SqlCommand command = new SqlCommand(queryString, connection);
            command.CommandTimeout = 600;
            SqlDataReader reader = command.ExecuteReader();
            Console.WriteLine(queryString);

            reader.Read();

            // will only be one row.  MAX can't return more
            if (reader.HasRows && !reader.IsDBNull(0))
            {
                result = reader.GetString(0);
            }


            // close reader 
            reader.Close();
            return result;
        }
        /* execute a query on productDetails that will return a single long value */
        public long longValueProductDetailsQuery(string queryString)
        {

            long result;


            // Execute the query  
            SqlCommand command = new SqlCommand(queryString, connection);
            command.CommandTimeout = 600;
            SqlDataReader reader = command.ExecuteReader();
            Console.WriteLine(queryString);

            reader.Read();

            // will only be one row.  MAX can't return more
            if (reader.HasRows && !reader.IsDBNull(0))
            {
                result = reader.GetInt32(0);
            }
            else
            {
                result = 0;
            }

            // close reader 
            reader.Close();
            return result;
        }
        /* get a single value from product details that will be returned as a decimal */
        public decimal decimalValueProductDetailsQuery(string queryString)
        {

            decimal result;


            // Execute the query  
            SqlCommand command = new SqlCommand(queryString, connection);
            command.CommandTimeout = 600;
            SqlDataReader reader = command.ExecuteReader();
            Console.WriteLine(queryString);

            reader.Read();

            // will only be one row.  MAX can't return more
            if (!reader.IsDBNull(0))
            {
                result = reader.GetDecimal(0);
            }
            else
            {
                result = 0;
            }

            // close reader 
            reader.Close();
            return result;
        }

        public void executeNonQuery(String queryString)
        {
            // Execute the query  
            SqlCommand command = new SqlCommand(queryString, connection);
            command.CommandTimeout = 600;
            command.ExecuteNonQuery();

        }
        public void calculateCompanyProjections(DateTime startDate, string startString, DateTime endDate, string endString, string lyStartString, string lyEndString, int daysInMonth)
        {



            // calculate and populate company GP data
            StoreProjection[0, (int)ProjectionsDataType.GP].value = getCompanyGP(startString, endString);
            StoreProjection[0, (int)ProjectionsDataType.GP].lastYear = getCompanyGP(lyStartString, lyEndString);
            StoreProjection[0, (int)ProjectionsDataType.GP].YOY = 100 * ((StoreProjection[0, (int)ProjectionsDataType.GP].value / StoreProjection[0, (int)ProjectionsDataType.GP].lastYear) - 1);
            StoreProjection[0, (int)ProjectionsDataType.GP].projection = (StoreProjection[0, (int)ProjectionsDataType.GP].value / endDate.Day) * daysInMonth;


            // calculate and populate company new line data
            StoreProjection[0, (int)ProjectionsDataType.NewLines].value = getCompanyNewLines(startString, endString);
            StoreProjection[0, (int)ProjectionsDataType.NewLines].lastYear = getCompanyNewLines(lyStartString, lyEndString);
            StoreProjection[0, (int)ProjectionsDataType.NewLines].YOY = 100 * (StoreProjection[0, (int)ProjectionsDataType.NewLines].value / StoreProjection[0, (int)ProjectionsDataType.NewLines].lastYear - 1);
            StoreProjection[0, (int)ProjectionsDataType.NewLines].projection = (StoreProjection[0, (int)ProjectionsDataType.NewLines].value / endDate.Day) * daysInMonth;

            // calculate and populate company upgrade data
            StoreProjection[0, (int)ProjectionsDataType.Upgrades].value = getCompanyUpgrades(startString, endString);
            StoreProjection[0, (int)ProjectionsDataType.Upgrades].lastYear = getCompanyUpgrades(lyStartString, lyEndString);
            StoreProjection[0, (int)ProjectionsDataType.Upgrades].YOY = 100 * (StoreProjection[0, (int)ProjectionsDataType.Upgrades].value / StoreProjection[0, (int)ProjectionsDataType.Upgrades].lastYear - 1);
            StoreProjection[0, (int)ProjectionsDataType.Upgrades].projection = (StoreProjection[0, (int)ProjectionsDataType.Upgrades].value / endDate.Day) * daysInMonth;

            // calculate and populate company voice line data
            StoreProjection[0, (int)ProjectionsDataType.VoiceLines].value = getCompanyVoiceLines(startString, endString);
            StoreProjection[0, (int)ProjectionsDataType.VoiceLines].lastYear = getCompanyVoiceLines(lyStartString, lyEndString);
            StoreProjection[0, (int)ProjectionsDataType.VoiceLines].YOY = 100 * (StoreProjection[0, (int)ProjectionsDataType.VoiceLines].value / StoreProjection[0, (int)ProjectionsDataType.VoiceLines].lastYear - 1);
            StoreProjection[0, (int)ProjectionsDataType.VoiceLines].projection = (StoreProjection[0, (int)ProjectionsDataType.VoiceLines].value / endDate.Day) * daysInMonth;

            // calculate and populate company new strategic data
            StoreProjection[0, (int)ProjectionsDataType.PullThru].value = getCompanyNewStrategicGrowth(startString, endString);
            StoreProjection[0, (int)ProjectionsDataType.PullThru].lastYear = getCompanyNewStrategicGrowth(lyStartString, lyEndString);
            StoreProjection[0, (int)ProjectionsDataType.PullThru].YOY = 100 * (StoreProjection[0, (int)ProjectionsDataType.PullThru].value / StoreProjection[0, (int)ProjectionsDataType.PullThru].lastYear - 1);
            StoreProjection[0, (int)ProjectionsDataType.PullThru].projection = (StoreProjection[0, (int)ProjectionsDataType.PullThru].value / endDate.Day) * daysInMonth;

            // total phones
            StoreProjection[0, (int)ProjectionsDataType.TotalPhones].value = StoreProjection[0, (int)ProjectionsDataType.NewLines].value + StoreProjection[0, (int)ProjectionsDataType.Upgrades].value;
            StoreProjection[0, (int)ProjectionsDataType.TotalPhones].lastYear = StoreProjection[0, (int)ProjectionsDataType.NewLines].lastYear + StoreProjection[0, (int)ProjectionsDataType.Upgrades].lastYear;
            StoreProjection[0, (int)ProjectionsDataType.TotalPhones].YOY = 100 * (StoreProjection[0, (int)ProjectionsDataType.TotalPhones].value / StoreProjection[0, (int)ProjectionsDataType.TotalPhones].lastYear - 1);
            StoreProjection[0, (int)ProjectionsDataType.TotalPhones].projection = (StoreProjection[0, (int)ProjectionsDataType.TotalPhones].value / endDate.Day) * daysInMonth;

        }

        public void calculateStoreProjections(DateTime startDate, string startString, DateTime endDate, string endString, string lyStartString, string lyEndString, int daysInMonth)
        {
            // loop through stores
            for (int storeNum = 1; storeNum < Enum.GetNames(typeof(Stores)).Length; storeNum++)
            {


                // calculate and populate company GP data
                StoreProjection[storeNum, (int)ProjectionsDataType.GP].value = getStoreGP(startString, endString, StoreList[storeNum].dbName);
                StoreProjection[storeNum, (int)ProjectionsDataType.GP].lastYear = getStoreGP(lyStartString, lyEndString, StoreList[storeNum].dbName);
                StoreProjection[storeNum, (int)ProjectionsDataType.GP].YOY = 100 * (StoreProjection[storeNum, (int)ProjectionsDataType.GP].value / StoreProjection[storeNum, (int)ProjectionsDataType.GP].lastYear - 1);
                StoreProjection[storeNum, (int)ProjectionsDataType.GP].projection = (StoreProjection[storeNum, (int)ProjectionsDataType.GP].value / endDate.Day) * daysInMonth;


                // calculate and populate company new line data
                StoreProjection[storeNum, (int)ProjectionsDataType.NewLines].value = getStoreNewLines(startString, endString, StoreList[storeNum].dbName);
                StoreProjection[storeNum, (int)ProjectionsDataType.NewLines].lastYear = getStoreNewLines(lyStartString, lyEndString, StoreList[storeNum].dbName);
                if (StoreProjection[storeNum, (int)ProjectionsDataType.NewLines].lastYear > 0)
                {
                    StoreProjection[storeNum, (int)ProjectionsDataType.NewLines].YOY = 100 * (StoreProjection[storeNum, (int)ProjectionsDataType.NewLines].value / StoreProjection[storeNum, (int)ProjectionsDataType.NewLines].lastYear - 1);
                }
                else
                {
                    StoreProjection[storeNum, (int)ProjectionsDataType.NewLines].YOY = 0;
                }
                StoreProjection[storeNum, (int)ProjectionsDataType.NewLines].projection = (StoreProjection[storeNum, (int)ProjectionsDataType.NewLines].value / endDate.Day) * daysInMonth;

                // calculate and populate company upgrade data
                StoreProjection[storeNum, (int)ProjectionsDataType.Upgrades].value = getStoreUpgrades(startString, endString, StoreList[storeNum].dbName);
                StoreProjection[storeNum, (int)ProjectionsDataType.Upgrades].lastYear = getStoreUpgrades(lyStartString, lyEndString, StoreList[storeNum].dbName);
                if (StoreProjection[storeNum, (int)ProjectionsDataType.Upgrades].lastYear > 0)
                {
                    StoreProjection[storeNum, (int)ProjectionsDataType.Upgrades].YOY = 100 * (StoreProjection[storeNum, (int)ProjectionsDataType.Upgrades].value / StoreProjection[storeNum, (int)ProjectionsDataType.Upgrades].lastYear - 1);
                }
                else
                {
                    StoreProjection[storeNum, (int)ProjectionsDataType.Upgrades].YOY = 0;
                }
                StoreProjection[storeNum, (int)ProjectionsDataType.Upgrades].projection = (StoreProjection[storeNum, (int)ProjectionsDataType.Upgrades].value / endDate.Day) * daysInMonth;

                // calculate and populate company voice lines data
                StoreProjection[storeNum, (int)ProjectionsDataType.VoiceLines].value = getStoreVoiceLines(startString, endString, StoreList[storeNum].dbName);
                StoreProjection[storeNum, (int)ProjectionsDataType.VoiceLines].lastYear = getStoreVoiceLines(lyStartString, lyEndString, StoreList[storeNum].dbName);
                if (StoreProjection[storeNum, (int)ProjectionsDataType.VoiceLines].lastYear > 0)
                {
                    StoreProjection[storeNum, (int)ProjectionsDataType.VoiceLines].YOY = 100 * (StoreProjection[storeNum, (int)ProjectionsDataType.VoiceLines].value / StoreProjection[storeNum, (int)ProjectionsDataType.VoiceLines].lastYear - 1);
                }
                else
                {
                    StoreProjection[storeNum, (int)ProjectionsDataType.VoiceLines].YOY = 0;
                }
                StoreProjection[storeNum, (int)ProjectionsDataType.VoiceLines].projection = (StoreProjection[storeNum, (int)ProjectionsDataType.VoiceLines].value / endDate.Day) * daysInMonth;

                // calculate and populate company new growth data
                StoreProjection[storeNum, (int)ProjectionsDataType.PullThru].value = getStoreNewStrategicGrowth(startString, endString, StoreList[storeNum].dbName);
                StoreProjection[storeNum, (int)ProjectionsDataType.PullThru].lastYear = getStoreNewStrategicGrowth(lyStartString, lyEndString, StoreList[storeNum].dbName);
                if (StoreProjection[storeNum, (int)ProjectionsDataType.PullThru].lastYear > 0)
                {
                    StoreProjection[storeNum, (int)ProjectionsDataType.PullThru].YOY = 100 * (StoreProjection[storeNum, (int)ProjectionsDataType.PullThru].value / StoreProjection[storeNum, (int)ProjectionsDataType.PullThru].lastYear - 1);
                }
                else
                {
                    StoreProjection[storeNum, (int)ProjectionsDataType.PullThru].YOY = 0;
                }
                StoreProjection[storeNum, (int)ProjectionsDataType.PullThru].projection = (StoreProjection[storeNum, (int)ProjectionsDataType.PullThru].value / endDate.Day) * daysInMonth;

                StoreProjection[storeNum, (int)ProjectionsDataType.TotalPhones].value = StoreProjection[storeNum, (int)ProjectionsDataType.NewLines].value + StoreProjection[storeNum, (int)ProjectionsDataType.Upgrades].value;
                StoreProjection[storeNum, (int)ProjectionsDataType.TotalPhones].lastYear = StoreProjection[storeNum, (int)ProjectionsDataType.NewLines].lastYear + StoreProjection[storeNum, (int)ProjectionsDataType.Upgrades].lastYear;
                StoreProjection[storeNum, (int)ProjectionsDataType.TotalPhones].YOY = 100 * (StoreProjection[storeNum, (int)ProjectionsDataType.TotalPhones].value / StoreProjection[storeNum, (int)ProjectionsDataType.TotalPhones].lastYear - 1);
                StoreProjection[storeNum, (int)ProjectionsDataType.TotalPhones].projection = (StoreProjection[storeNum, (int)ProjectionsDataType.TotalPhones].value / endDate.Day) * daysInMonth;

            }

        }

        public void calculateProjections(DateTime startDate, DateTime endDate)
        {
            // convert start and end dates to strings
            string startString = startDate.ToString("MM/dd/yyyy HH:mm:ss");
            string endString = endDate.ToString("MM/dd/yyyy HH:mm:ss");

            // get dates for YOY
            string lyStartString = startDate.AddYears(-1).ToString("MM/dd/yyyy HH:mm:ss");
            DateTime toEndOfDay = endDate.AddYears(-1);
            toEndOfDay = toEndOfDay.Date;
            toEndOfDay = toEndOfDay.AddDays(1);
            toEndOfDay = toEndOfDay.AddTicks(-1);
            string lyEndString = toEndOfDay.ToString("MM/dd/yyyy HH:mm:ss");

            // calcluate days in month for projections
            int daysInMonth = DateTime.DaysInMonth(endDate.Year, endDate.Month);

            // pull data into Temp Table
            string queryString = "Select * Into #ProductDetails from bogart_2.productdetails where SoldOn BETWEEN '" + startDate + "' AND '" + endDate + "'";
            queryString = "Select * Into #ProductDetails from bogart_2.productdetails where SoldOn BETWEEN '" + startDate + "' AND '" + endDate + "' or SoldOn BETWEEN '" + lyStartString + "' AND '" + lyEndString + "'";
            executeNonQuery(queryString);


            calculateCompanyProjections(startDate, startString, endDate, endString, lyStartString, lyEndString, daysInMonth);
            calculateStoreProjections(startDate, startString, endDate, endString, lyStartString, lyEndString, daysInMonth);

            // drop temp table
            queryString = "Drop Table #ProductDetails";
            executeNonQuery(queryString);
        }

        public void calculateCompanyQuality(DateTime startDate, string startString, DateTime endDate, string endString, string lyStartString, string lyEndString, int daysInMonth)
        {
            string queryString;

            // calculate and populate company GP data
            QualityData[0].name = "Company";
            QualityData[0].CommissionRate = 0;

            // get voice lines
            QualityData[0].VoiceLines = getCompanyVoiceLines(startString, endString);
            QualityData[0].NewLines = Math.Round(getCompanyNewLines(startString, endString), 0);

            // get HUM count
            queryString = "Select sum(Qty) from #ProductDetails  where SoldOn Between '" + startDate + "' AND '" + endDate + "'" +
           "AND ProductSKU IN(Select ProductSKU from bogart_2.productSKUS where HUM = 1) " +
           "AND CONVERT(date, SoldOn) <> SoldOn";
            QualityData[0].HUMCount = (int)decimalValueProductDetailsQuery(queryString);

            // get total GP
            QualityData[0].TotalGP = getCompanyGP(startString, endString);

            // get device GP
            queryString = "Select sum(GrossProfit) from #ProductDetails where GPCategory in ('SMART','IPHONE','BASIC','RECON' ,'DEVICES','PLANS')";
            QualityData[0].DeviceGP = Math.Round(decimalValueProductDetailsQuery(queryString) / QualityData[0].VoiceLines, 2);

            // get ACC GP
            queryString = "Select sum(GrossProfit) from #ProductDetails where GPCategory = 'ACC'";
            QualityData[0].AccGP = Math.Round(decimalValueProductDetailsQuery(queryString) / QualityData[0].VoiceLines, 2);

            // get ins GP
            queryString = "Select sum(GrossProfit) from #ProductDetails where GPCategory = 'PROT' and GrossProfit > 0";
            queryString = "Select sum(GrossProfit) from #ProductDetails where GPCategory = 'PROT'";
            QualityData[0].InsGP = Math.Round(decimalValueProductDetailsQuery(queryString) / QualityData[0].VoiceLines, 2);

            // get ins. count
            queryString = "Select sum(Qty) from #ProductDetails where GPCategory = 'PROT' and GrossProfit > 0";
            decimal insCount = decimalValueProductDetailsQuery(queryString);
            QualityData[0].InsRate = Math.Round(insCount * 100 / QualityData[0].VoiceLines, 2);

            // get upgrade count
            decimal upgrades = getCompanyUpgrades(startString, endString);

            // get new line count
            decimal newLines = Math.Round(getCompanyNewLines(startString, endString), 0);

            // get strategic growth count
            decimal pullThru = getCompanyNewStrategicGrowth(startString, endString);

            // calculate quality ratios
            QualityData[0].UpToNew = Math.Round(upgrades / newLines, 2);
            QualityData[0].GPPerSale = Math.Round(QualityData[0].TotalGP / QualityData[0].VoiceLines, 2);
            QualityData[0].PullThru = Math.Round(pullThru * 100 / QualityData[0].VoiceLines, 2);
            QualityData[0].AddOnGP = QualityData[0].GPPerSale - QualityData[0].DeviceGP - QualityData[0].AccGP - QualityData[0].InsGP;

            // calculate closing rate based on last traffic upload date
            DateTime endTrafficDate = lastTrafficUploadDate.AddSeconds(86399);
            queryString = "Select sum(Qty) from #ProductDetails  where SoldOn Between '" + startDate + "' AND '" + endTrafficDate + "' " +
               "AND Description != 'Hardware Only Rate Plan' and Category = '>> Activations >> Verizon Wireless >> Rate Plans' " +
               "AND CONVERT(date, SoldOn) <> SoldOn";
            decimal totalPhones = decimalValueProductDetailsQuery(queryString);
            queryString = "SELECT SUM(TrafficCount) as Traffic FROM bogart_2.trafficcountsdailybystore WHERE Date >= '" + startDate + "' AND Date <= '" + endTrafficDate + "'";
            decimal totalTraffic = longValueProductDetailsQuery(queryString);
            if (totalTraffic > 0)
            {
                QualityData[0].ClosingRate = Math.Round((totalPhones / (decimal)totalTraffic) * 100, 2);
            }
            else
            {
                QualityData[0].ClosingRate = 0;
            }

            // calculate trade-in rate

            queryString = "SELECT Sum(Qty) as TradeInCount FROM #ProductDetails p, bogart_2.productskus s " +
                    "WHERE p.ProductSKU = s.ProductSKU " +
                    "AND s.TradeIn = 1";
            decimal totalTradeIns = decimalValueProductDetailsQuery(queryString);
            QualityData[0].TradeInRate = Math.Round(totalTradeIns * 100 / QualityData[0].VoiceLines, 2);


            // calculate Ready/Go rate

            queryString = "SELECT SUM(Qty) as ReadyGoCount FROM #ProductDetails p, bogart_2.productskus s " +
                    "WHERE p.ProductSKU = s.ProductSKU " +
                    "AND s.ReadyGO = 1";
            decimal totalReadyGO = decimalValueProductDetailsQuery(queryString);
            QualityData[0].ReadyGoTakeRate = Math.Round(totalReadyGO * 100/ QualityData[0].VoiceLines, 2);
            

            // calculate Ready/Go GP per sale

            queryString = "SELECT SUM(Grossprofit) as ReadyGoGP FROM #ProductDetails p, bogart_2.productskus s " +
                    "WHERE p.ProductSKU = s.ProductSKU " +
                    "AND s.ReadyGO = 1";
            decimal totalReadyGOGP = decimalValueProductDetailsQuery(queryString);
            QualityData[0].ReadyGoGP = Math.Round(totalReadyGOGP / QualityData[0].VoiceLines, 2);
        }

        public void calculateStoreQuality(DateTime startDate, string startString, DateTime endDate, string endString, string lyStartString, string lyEndString, int daysInMonth)
        {
            string queryString;

            // loop through stores
            for (int storeNum = 1; storeNum < Enum.GetNames(typeof(Stores)).Length; storeNum++)
            {

                // calculate and populate store GP data
                QualityData[storeNum].CommissionRate = 0;

                // get voice lines
                QualityData[storeNum].VoiceLines = getStoreVoiceLines(startString, endString, StoreList[storeNum].dbName);
                QualityData[storeNum].NewLines = Math.Round(getStoreNewLines(startString, endString, StoreList[storeNum].dbName), 0);

                // get HUM count
                queryString = "Select sum(Qty) from #ProductDetails  where InvoicedAt = '" + StoreList[storeNum].dbName + "' AND SoldOn Between '" + startDate + "' AND '" + endDate + "'" +
               "AND ProductSKU IN(Select ProductSKU from bogart_2.productSKUS where HUM = 1) " +
               "AND CONVERT(date, SoldOn) <> SoldOn";
                QualityData[storeNum].HUMCount = (int)decimalValueProductDetailsQuery(queryString);

                // get total GP
                QualityData[storeNum].TotalGP = getStoreGP(startString, endString, StoreList[storeNum].dbName);

                // get device GP
                queryString = "Select sum(GrossProfit) from #ProductDetails where InvoicedAt = '" + StoreList[storeNum].dbName + "' and GPCategory in ('SMART','IPHONE','BASIC','RECON' ,'DEVICES','PLANS')";
                QualityData[storeNum].DeviceGP = Math.Round(decimalValueProductDetailsQuery(queryString) / QualityData[storeNum].VoiceLines, 2);

                // get ACC GP
                queryString = "Select sum(GrossProfit) from #ProductDetails where InvoicedAt = '" + StoreList[storeNum].dbName + "' and GPCategory = 'ACC'";
                QualityData[storeNum].AccGP = Math.Round(decimalValueProductDetailsQuery(queryString) / QualityData[storeNum].VoiceLines, 2);

                // get ins GP
                queryString = "Select sum(GrossProfit) from #ProductDetails where InvoicedAt = '" + StoreList[storeNum].dbName + "' and GPCategory = 'PROT' and GrossProfit > 0";
                QualityData[storeNum].InsGP = Math.Round(decimalValueProductDetailsQuery(queryString) / QualityData[storeNum].VoiceLines, 2);

                // get ins. count
                queryString = "Select sum(Qty) from #ProductDetails where InvoicedAt = '" + StoreList[storeNum].dbName + "' and GPCategory = 'PROT' and GrossProfit > 0";
                queryString = "Select sum(Qty) from #ProductDetails where InvoicedAt = '" + StoreList[storeNum].dbName + "' and GPCategory = 'PROT'";
                decimal insCount = decimalValueProductDetailsQuery(queryString);
                QualityData[storeNum].InsRate = Math.Round(insCount * 100 / QualityData[storeNum].VoiceLines, 2);

                // get upgrade count
                decimal upgrades = getStoreUpgrades(startString, endString, StoreList[storeNum].dbName);

                // get new line count
                decimal newLines = Math.Round(getStoreNewLines(startString, endString, StoreList[storeNum].dbName), 0);

                // get strategic growth count
                decimal pullThru = getStoreNewStrategicGrowth(startString, endString, StoreList[storeNum].dbName);

                // calculate quality ratios
                QualityData[storeNum].UpToNew = Math.Round(upgrades / newLines, 2);
                QualityData[storeNum].GPPerSale = Math.Round(QualityData[storeNum].TotalGP / QualityData[storeNum].VoiceLines, 2);
                QualityData[storeNum].PullThru = Math.Round(pullThru * 100 / QualityData[storeNum].VoiceLines, 2);
                QualityData[storeNum].AddOnGP = QualityData[storeNum].GPPerSale - QualityData[storeNum].DeviceGP - QualityData[storeNum].AccGP - QualityData[storeNum].InsGP;

                // calculate closing rate based on last traffic upload date
                DateTime endTrafficDate = lastTrafficUploadDate.AddSeconds(86399);
                queryString = "Select sum(Qty) from #ProductDetails  where SoldOn Between '" + startDate + "' AND '" + endTrafficDate + "' " +
                   "AND Description != 'Hardware Only Rate Plan' and Category = '>> Activations >> Verizon Wireless >> Rate Plans' " +
                   "AND CONVERT(date, SoldOn) <> SoldOn " +
                   "AND InvoicedAt = '" + StoreList[storeNum].dbName + "'";
                decimal totalPhones = decimalValueProductDetailsQuery(queryString);
                queryString = "SELECT SUM(TrafficCount) as Traffic FROM bogart_2.trafficcountsdailybystore WHERE Date >= '" + startDate + "' AND Date <= '" + endTrafficDate + "' " +
                    "AND Location = '" + StoreList[storeNum].storeName + "'";
                decimal totalTraffic = longValueProductDetailsQuery(queryString);
                if (totalTraffic > 0)
                {
                    QualityData[storeNum].ClosingRate = Math.Round((totalPhones / (decimal)totalTraffic) * 100, 2);
                }
                else
                {
                    QualityData[storeNum].ClosingRate = 0;
                }


                // calculate trade-in rate
                queryString = "SELECT Sum(Qty) as TradeInCount FROM #ProductDetails p, bogart_2.productskus s " +
                        "WHERE p.ProductSKU = s.ProductSKU " +
                        "AND s.TradeIn = 1 AND InvoicedAt = '" + StoreList[storeNum].dbName + "'";
                decimal totalTradeIns = decimalValueProductDetailsQuery(queryString);
                QualityData[storeNum].TradeInRate = Math.Round(totalTradeIns * 100 / QualityData[storeNum].VoiceLines, 2);


                // calculate Ready/Go rate

                queryString = "SELECT SUM(Qty) as ReadyGoCount FROM #ProductDetails p, bogart_2.productskus s " +
                        "WHERE p.ProductSKU = s.ProductSKU " +
                        "AND s.ReadyGO = 1 AND InvoicedAt = '" + StoreList[storeNum].dbName + "'";
                decimal totalReadyGO = decimalValueProductDetailsQuery(queryString);
                QualityData[storeNum].ReadyGoTakeRate = Math.Round(totalReadyGO * 100 / QualityData[storeNum].VoiceLines, 2);

                // calculate Ready/Go GP per sale
                queryString = "SELECT SUM(Grossprofit) as ReadyGoGP FROM #ProductDetails p, bogart_2.productskus s " +
                        "WHERE p.ProductSKU = s.ProductSKU  " +
                         "AND s.ReadyGO = 1 AND InvoicedAt = '" + StoreList[storeNum].dbName + "'";
                decimal totalReadyGOGP = decimalValueProductDetailsQuery(queryString);
                QualityData[storeNum].ReadyGoGP = Math.Round(totalReadyGOGP / QualityData[storeNum].VoiceLines, 2);
            }
        }

        public void calculateRepQuality(DateTime startDate, string startString, DateTime endDate, string endString)
        {
            string queryString;

            // get list of active reps
            getActiveReps(startDate, endDate);

            // loop through stores
            for (int repNum = 0; repNum < activeReps.Count; repNum++)
            {


                // calculate and populate company GP data

                RepQualityData[repNum].name = activeReps[repNum];
                RepQualityData[repNum].CommissionRate = 0;

                // get voice lines
                queryString = "Select sum(Qty) from #ProductDetails  where SoldBy = '" + activeReps[repNum] + "' " +
                    "AND ProductSKU IN(Select ProductSKU from bogart_2.productSKUS where VoiceLine = 1) " +
                    "AND CONVERT(date, SoldOn) <> SoldOn";
                RepQualityData[repNum].VoiceLines = (int)decimalValueProductDetailsQuery(queryString);

                // get HUM count
                queryString = "Select sum(Qty) from bogart_2.productdetails  where  SoldBy = '" + activeReps[repNum] + "' AND SoldOn Between '" + startDate + "' AND '" + endDate + "'" +
               "AND ProductSKU IN(Select ProductSKU from bogart_2.productSKUS where HUM = 1) " +
               "AND CONVERT(date, SoldOn) <> SoldOn";
                RepQualityData[repNum].HUMCount = (int)decimalValueProductDetailsQuery(queryString);

                // get total GP
                queryString = "Select sum(GrossProfit) from #ProductDetails where SoldBy = '" + activeReps[repNum] + "'";
                RepQualityData[repNum].TotalGP = decimalValueProductDetailsQuery(queryString);

                // get device GP
                queryString = "Select sum(GrossProfit) from #ProductDetails where SoldBy = '" + activeReps[repNum] + "' and GPCategory in ('SMART','IPHONE','BASIC','RECON' ,'DEVICES','PLANS')";
                if (RepQualityData[repNum].VoiceLines > 0)
                {
                    RepQualityData[repNum].DeviceGP = Math.Round(decimalValueProductDetailsQuery(queryString) / RepQualityData[repNum].VoiceLines, 2);
                }
                else
                {
                    RepQualityData[repNum].DeviceGP = 0;
                }
                // get ACC GP
                queryString = "Select sum(GrossProfit) from #ProductDetails where SoldBy = '" + activeReps[repNum] + "' AND GPCategory = 'ACC'";
                if (RepQualityData[repNum].VoiceLines > 0)
                {
                    RepQualityData[repNum].AccGP = Math.Round(decimalValueProductDetailsQuery(queryString) / RepQualityData[repNum].VoiceLines, 2);
                }
                else
                {
                    RepQualityData[repNum].AccGP = 0;
                }

                // get ins GP
                queryString = "Select sum(GrossProfit) from #ProductDetails where SoldBy = '" + activeReps[repNum] + "' and GPCategory = 'PROT' and GrossProfit > 0";
                queryString = "Select sum(GrossProfit) from #ProductDetails where SoldBy = '" + activeReps[repNum] + "' and GPCategory = 'PROT'";
                if (RepQualityData[repNum].VoiceLines > 0)
                {
                    RepQualityData[repNum].InsGP = Math.Round(decimalValueProductDetailsQuery(queryString) / RepQualityData[repNum].VoiceLines, 2);
                }
                else
                {
                    RepQualityData[repNum].InsGP = 0;
                }

                // get ins. count
                queryString = "Select sum(Qty) from #ProductDetails where SoldBy = '" + activeReps[repNum] + "' and GPCategory = 'PROT' and GrossProfit > 0";
                decimal insCount = decimalValueProductDetailsQuery(queryString);
                if (RepQualityData[repNum].VoiceLines > 0)
                {
                    RepQualityData[repNum].InsRate = Math.Round(insCount * 100 / RepQualityData[repNum].VoiceLines, 2);
                }
                else
                {
                    RepQualityData[repNum].InsRate = 0;
                }

                // get upgrade count
                queryString = "Select sum(Qty) from #ProductDetails where SoldBy = '" + activeReps[repNum] + "' " +
                             "AND ProductSKU IN(Select ProductSKU from bogart_2.productSKUS where Upgrade = 1) " +
                             "AND CONVERT(date, SoldOn) <> SoldOn";
                decimal upgrades = decimalValueProductDetailsQuery(queryString);

                // get new line count
                queryString = "Select sum(Qty) from #ProductDetails  where SoldBy = '" + activeReps[repNum] + "' " +
                            "AND ProductSKU IN(Select ProductSKU from bogart_2.productSKUS where NewLine = 1) " +
                            "AND CONVERT(date, SoldOn) <> SoldOn";
                decimal newLines = decimalValueProductDetailsQuery(queryString);
                RepQualityData[repNum].NewLines = Math.Round(newLines, 0);

                // get strategic growth count
                queryString = "Select sum(Qty) from #ProductDetails  where SoldBy = '" + activeReps[repNum] + "' " +
                            "AND ProductSKU IN(Select ProductSKU from bogart_2.productSKUS where NewStrategicGrowth = 1) " +
                         "AND CONVERT(date, SoldOn) <> SoldOn";
                decimal pullThru = decimalValueProductDetailsQuery(queryString);

                // calculate quality ratios
                if (newLines > 0)
                {
                    RepQualityData[repNum].UpToNew = Math.Round(upgrades / newLines, 2);
                }
                else
                {
                    RepQualityData[repNum].UpToNew = 0;
                }
                if (RepQualityData[repNum].VoiceLines > 0)
                {
                    RepQualityData[repNum].GPPerSale = Math.Round(RepQualityData[repNum].TotalGP / RepQualityData[repNum].VoiceLines, 2);
                    RepQualityData[repNum].PullThru = Math.Round(pullThru * 100 / RepQualityData[repNum].VoiceLines, 2);
                    RepQualityData[repNum].AddOnGP = RepQualityData[repNum].GPPerSale - RepQualityData[repNum].DeviceGP - RepQualityData[repNum].AccGP - RepQualityData[repNum].InsGP;
                }
                else
                {
                    RepQualityData[repNum].GPPerSale = 0;
                    RepQualityData[repNum].PullThru = 0;
                    RepQualityData[repNum].AddOnGP = 0;

                }

                // calculate closing rate based on last traffic upload date
                DateTime endTrafficDate = lastTrafficUploadDate.AddSeconds(86399);
                queryString = "Select sum(Qty) from #ProductDetails  where SoldOn Between '" + startDate + "' AND '" + endTrafficDate + "' " +
                           "AND Description != 'Hardware Only Rate Plan' and Category = '>> Activations >> Verizon Wireless >> Rate Plans' " +
                           "AND CONVERT(date, SoldOn) <> SoldOn " +
                           "AND SoldBy = '" + activeReps[repNum] + "'";
                decimal totalPhones = decimalValueProductDetailsQuery(queryString);
                queryString = "SELECT SUM(TrafficCount) as Traffic FROM bogart_2.trafficcountsbyemployee WHERE Date >= '" + startDate + "' AND Date <= '" + endTrafficDate + "' " +
                    "AND Employee = '" + activeReps[repNum] + "'";
                decimal totalTraffic = longValueProductDetailsQuery(queryString);
                if (totalTraffic > 0)
                {
                    RepQualityData[repNum].ClosingRate = Math.Round((totalPhones / (decimal)totalTraffic) * 100, 2);
                }
                else
                {
                    RepQualityData[repNum].ClosingRate = 0;
                }

                // calculate trade-in rate
                queryString = "SELECT Sum(Qty) as TradeInCount FROM #ProductDetails p, bogart_2.productskus s " +
                        "WHERE p.ProductSKU = s.ProductSKU " +
                        "AND s.TradeIn = 1 AND SoldBy = '" + activeReps[repNum] + "'";
                decimal totalTradeIns = decimalValueProductDetailsQuery(queryString);
                RepQualityData[repNum].TradeInRate = Math.Round(totalTradeIns * 100 / RepQualityData[repNum].VoiceLines, 2);


                // calculate Ready/Go rate
                queryString = "SELECT SUM(Qty) as ReadyGoCount FROM #ProductDetails p, bogart_2.productskus s " +
                        "WHERE p.ProductSKU = s.ProductSKU " +
                        "AND s.ReadyGO = 1 AND SoldBy = '" + activeReps[repNum] + "'";
                decimal totalReadyGO = decimalValueProductDetailsQuery(queryString);
                RepQualityData[repNum].ReadyGoTakeRate = Math.Round(totalReadyGO * 100 / RepQualityData[repNum].VoiceLines, 2);

                // calculate Ready/Go GP per sale
                queryString = "SELECT SUM(Grossprofit) as ReadyGoGP FROM #ProductDetails p, bogart_2.productskus s " +
                        "WHERE p.ProductSKU = s.ProductSKU " +
                        "AND s.ReadyGO = 1 AND SoldBy = '" + activeReps[repNum] + "'";
                decimal totalReadyGOGP = decimalValueProductDetailsQuery(queryString);
                RepQualityData[repNum].ReadyGoGP = Math.Round(totalReadyGOGP / RepQualityData[repNum].VoiceLines, 2);

                // calculate commission rate
                if (endDate.Subtract(startDate).TotalDays > 21)
                {
                    Payroll payroll = new Payroll();
                    CommissionRates rateSet = payroll.calculateCommissionRate(RepQualityData[repNum], endDate, this);
                    RepQualityData[repNum].CommissionRate = Math.Round(rateSet.effectiveRate, 2);
                }
                else
                {
                    RepQualityData[repNum].CommissionRate = -1;
                }
            }
        }
        public void calculateQuality(DateTime startDate, DateTime endDate)
        {
            calcStartDate = startDate;
            calcEndDate = endDate;

            // convert start and end dates to strings
            string startString = startDate.ToString("MM/dd/yyyy HH:mm:ss");
            string endString = endDate.ToString("MM/dd/yyyy HH:mm:ss");

            // get dates for YOY
            string lyStartString = startDate.AddYears(-1).ToString("MM/dd/yyyy HH:mm:ss");
            DateTime toEndOfDay = endDate.AddYears(-1);
            toEndOfDay = toEndOfDay.Date;
            toEndOfDay = toEndOfDay.AddDays(1);
            toEndOfDay = toEndOfDay.AddTicks(-1);
            string lyEndString = toEndOfDay.ToString("MM/dd/yyyy HH:mm:ss");

            // calcluate days in month for projections
            int daysInMonth = DateTime.DaysInMonth(endDate.Year, endDate.Month);

            // pull data into TempTable
            string queryString = "Select * Into #ProductDetails from bogart_2.productdetails where SoldOn BETWEEN '" + startDate + "' AND '" + endDate + "'";
            executeNonQuery(queryString);
            calculateCompanyQuality(startDate, startString, endDate, endString, lyStartString, lyEndString, daysInMonth);
            calculateStoreQuality(startDate, startString, endDate, endString, lyStartString, lyEndString, daysInMonth);
            calculateRepQuality(startDate, startString, endDate, endString);
            queryString = "Drop Table #ProductDetails";
            executeNonQuery(queryString);

            // sort Rep Quality array by GP per sale
            
            Array.Sort(RepQualityData, delegate (Quality user2, Quality user1) {
                return user1.GPPerSale.CompareTo(user2.GPPerSale);
            });
            
        }


        // load all promos
        public int loadPromos()
        {
            int recordCount = 0;
            int promoIndex; // used to track list index for newly added promo

            // create the query
            String queryString = "SELECT PromoID, StartDate, EndDate, PromoTitle, PromoDetails, Priority, PromoType " +
                    "FROM bogart_2.promotions " +
                    "WHERE (StartDate <= GETDATE() and(EndDate >= GETDATE()  OR EndDate IS NULL)) " +
                    "Order by Priority ASC";

            // Execute the query
            SqlCommand command = new SqlCommand(queryString, connection);
            command.CommandTimeout = 600;
            SqlDataReader reader = command.ExecuteReader();

            while (reader.Read())
            {
                recordCount++;

                // put promo data into class variable
                Promo newPromo = new Promo();
                newPromo.promoID = reader.GetInt32(0);
                newPromo.startDate = reader.GetDateTime(1);
                if (reader.IsDBNull(2))
                {
                    newPromo.effectiveDates = "starting " + newPromo.startDate.ToString("d");
                }
                else
                {
                    newPromo.endDate = reader.GetDateTime(2);
                    newPromo.effectiveDates = newPromo.startDate.ToString("d") + " thru " + newPromo.endDate.ToString("d");
                }
                newPromo.promoTitle = reader.GetString(3);
                newPromo.promoDetails = reader.GetString(4);
                newPromo.priority = reader.GetInt32(5);
                newPromo.promoType = reader.GetString(6);

                // check promo type
                switch (newPromo.promoType)
                {
                    case "Customer":
                        customerPromos.Add(newPromo);
                        break;
                    case "Business":
                        businessPromos.Add(newPromo);
                        break;
                    case "Both":
                        customerPromos.Add(newPromo);
                        businessPromos.Add(newPromo);
                        break;
                    case "Employee":
                        employeePromos.Add(newPromo);
                        break;

                }

            }
            // close reader 
            reader.Close();

            // create the query for recently expired promos
            queryString = "SELECT PromoID, StartDate, EndDate, PromoTitle, PromoDetails, Priority, PromoType " +
                    "FROM bogart_2.promotions " +
                    "WHERE (EndDate IS NOT NULL AND EndDate < GETDATE() And EndDate > DATEADD(day, -14, GETDATE())) " +
                    "Order by EndDate Desc";
            command.CommandText = queryString;
            reader = command.ExecuteReader();

            while (reader.Read())
            {
                recordCount++;

                // put promo data into class variable
                Promo newPromo = new Promo();
                newPromo.promoID = reader.GetInt32(0);
                newPromo.startDate = reader.GetDateTime(1);
                if (reader.IsDBNull(2))
                {
                    newPromo.effectiveDates = "starting " + newPromo.startDate.ToString("d");
                }
                else
                {
                    newPromo.endDate = reader.GetDateTime(2);
                    newPromo.effectiveDates = newPromo.startDate.ToString("d") + " thru " + newPromo.endDate.ToString("d");
                }
                newPromo.promoTitle = reader.GetString(3);
                newPromo.promoDetails = reader.GetString(4);
                newPromo.priority = reader.GetInt32(5);
                newPromo.promoType = reader.GetString(6);

                expiredPromos.Add(newPromo);

            }

            // close reader 
            reader.Close();

            return recordCount;
        }


        public int loadProductDetails(List<ProductDetails> pds, String minDateString, String maxDateString)
        {
            int numInserts = -1;
            long recordID;
            String soldOnString;
            DateTime minDate = new DateTime();
            String queryString;
            Funcs funcs = new Funcs();

            // convert minDateString to a DateTime object
            minDate = Convert.ToDateTime(minDateString);

            // start database transaction
            queryString = "BEGIN TRANSACTION PRODUCTDETAILS";
            //executeNonQuery(queryString);

            // get highest record ID currently in productDetails
            queryString = "SELECT max(ProductDetailLineID) as maxid FROM bogart_2.productdetails";
            recordID = longValueProductDetailsQuery(queryString);

            foreach (ProductDetails pd in pds)
            {

                // does invoice number look valid?
                if (pd.invoiceNo.Substring(0, 3) == "W12")
                {
                    // avoid issues with quotes and double quotes
                    pd.description = pd.description.Replace('"', '\"');
                    pd.description = pd.description.Replace("'", "''");
                    pd.customer = pd.customer.Replace("'", "''");

                    soldOnString = pd.soldOn.ToString("MM/dd/yyyy HH:mm");

                    // check whether this is ann adjustment record
                    if (pd.soldOn < minDate)
                    {
                        queryString = "DELETE from bogart_2.productdetails " +
                                      "Where InvoiceNo = '" + pd.invoiceNo + "' " +
                                      "AND SoldBy = '" + pd.soldBy + "' " +
                                      "AND ProductSKU = '" + pd.productSKU + "' " +
                                      "AND TrackingNo = '" + pd.TrackingNo + "' " +
                                      "AND AdjustedUnitPrice = " + pd.adjustedUnitprice +
                                      " AND TotalSales = " + pd.totalSales +
                                      " AND SoldOn < '" + minDateString + "'";
                        executeNonQuery(queryString);

                        // adjust SoldOn to be within the file date range
                        pd.soldOn = minDate;


                    }

                    // create the insert query
                    queryString = "Insert into bogart_2.productdetails " +
                        "(InvoiceNo, InvoicedBy, InvoicedAt, SoldBy, TenderedBy, SoldOn," +
                        "InvoiceComments, Customer, ProductSKU, TrackingNo, SoldAsUsed," +
                        "ContractNo, Description, Refund, Qty, UnitCost," +
                        "TotalCost, ListPrice, SoldFor, AdjustedUnitPrice," +
                        "GrossProfit, CarrierPrice, TotalSales, TotalDiscount, District, Category," +
                        "LocationType, TotalProductCoupons, OrigUnitPrice)" +
                        "Values" +
                        "('" + pd.invoiceNo + "', '" + pd.invoicedBy + "', '" + pd.invoicedAt + "', '" + pd.soldBy + "', '" + pd.tenderedBy +
                        "', '" + pd.soldOn.ToString("MM/dd/yyyy HH:mm:ss") + "', '" + funcs.AddSlashes(pd.invoiceComments) + "', '" + pd.customer + "', '" + pd.productSKU +
                        "', '" + pd.TrackingNo + "', '" + pd.soldAsUsed + "', '" + pd.contractNo + "', '" + funcs.AddSlashes(pd.description) + "', '" + pd.refund + "', " + pd.qty +
                        "," + pd.unitCost + "," + pd.totalCost + "," + pd.listPrice + "," + pd.soldFor + "," + pd.adjustedUnitprice +
                        "," + pd.grossProfit + "," + pd.carrierPrice + "," + pd.totalSales + "," + pd.totalDiscount + ",'" + pd.district + "','" + pd.category.Trim() +
                        "','" + pd.locationType + "'," + pd.totalProductCoupons + "," + pd.origUnitPrice + ")";
                    executeNonQuery(queryString);
                    numInserts++;

                }
            }

            // delete records we replaced
            queryString = "Delete from bogart_2.productdetails where ProductDetailLineID <= " + recordID + " and SoldOn between '" + minDateString + "' AND '" + maxDateString + "'";
            executeNonQuery(queryString);

            // commit transaction
            queryString = "COMMIT TRANSACTION PRODUCTDETAILS";
            //executeNonQuery(queryString);

            // assign categories to calculate KPI
            assign_gp_categories();

            // Calculate TMP counts
            calc_TMP_counts(recordID);

            // return number of records inserted
            return numInserts + 1;
        }

        public int executeFileLoader()
        {
            String minDate;
            String maxDate;
            CultureInfo provider = CultureInfo.InvariantCulture;

            List<EmailMessage> emails;
            EmailService emailSvc = new EmailService(emailConfiguration);
            emails = emailSvc.ReceiveEmail(10);
            if (emails.Count > 0)
            {
                // loop through emails
                foreach (EmailMessage email in emails)
                {

                    // get first attachment
                    if (email.Attachments.Count() > 0)
                    {
                        String fileName = email.Attachments[0];
                        String fileRoot = Path.GetFileName(fileName);

                        // check whether this is a product details file
                        if (fileRoot.Contains("Sales by Product Report From"))
                        {
                            int namePos = fileRoot.IndexOf("Sales by Product Report From");

                            // get min/max dates from file name
                            minDate = fileRoot.Substring(namePos + 29, 11);
                            DateTime myDateTime = DateTime.ParseExact(minDate, "dd-MMM-yyyy", provider);
                            minDate = myDateTime.ToString("d");
                            maxDate = fileRoot.Substring(namePos + 44, 11);
                            myDateTime = DateTime.ParseExact(maxDate, "dd-MMM-yyyy", provider);
                            myDateTime = myDateTime.AddSeconds(86399);
                            maxDate = myDateTime.ToString("MM/dd/yyyy HH:mm:ss");


                            // read data from spreadsheet
                            Excel excel = new Excel();
                            Hashtable indices = new Hashtable();
                            List<ProductDetails> rows = new List<ProductDetails>();
                            int rowCount = excel.ProductDetailsToArray(fileName, rows, indices, false, 0);

                            // insert data into productdetails table
                            int numInserts = loadProductDetails(rows, minDate, maxDate);

                            // send a "SUCCESS" email
                            EmailService emailService = new EmailService(SalesData.emailConfiguration);
                            if (email.FromAddresses.Count > 0)
                            {
                                emailService.QuickSend("Re: " + email.Subject, "Successfully Processed", email.FromAddresses[0].Address, email.FromAddresses[0].Name);
                            }


                        }
                        else if (fileRoot.Length > 36 && fileRoot.Substring(0, 37) == "Weekly Summary - Springville Wireless") // check whether this is a ReBiz file
                        {
                            Excel excel = new Excel();
                            excel.processWeeklySummary(fileName, email.Subject);
                            EmailService emailService = new EmailService(SalesData.emailConfiguration);
                            emailService.QuickSend("Re: " + email.Subject, "Successfully Processed", "noreply@bogart-wireless.net", SalesData.emailConfiguration.FromName);

                        }
                    }

                }
            }

            // return the number of emails processed
            return emails.Count();

        }

        /* ASSIGN GP CATEGORIES TO PRODUCT DETAILS RECORDS */
        private void assign_gp_categories()
        {
            String queryString;


            queryString = "UPDATE bogart_2.productdetails SET GPCategory = 'ACC' WHERE GPCategory IS NULL AND Category LIKE '>> Accessory%'";
            executeNonQuery(queryString);


            queryString = "UPDATE bogart_2.productdetails SET GPCategory = 'ACC' WHERE GPCategory IS NULL AND Category LIKE '>> Integrated Solutions%'";
            executeNonQuery(queryString);

            queryString = "UPDATE bogart_2.productdetails SET GPCategory = 'PROT' WHERE GPCategory IS NULL  AND Category LIKE('>> Activations >> Verizon Wireless >> Features%')";
            executeNonQuery(queryString);

            queryString = "UPDATE bogart_2.productdetails SET GPCategory = 'BILLS' WHERE GPCategory IS NULL AND Category LIKE('>> Bill Payment%')";
            executeNonQuery(queryString);

            queryString = "UPDATE bogart_2.productdetails SET GPCategory = 'FEES' WHERE GPCategory IS NULL AND Category LIKE('>> Fees%')";
            executeNonQuery(queryString);

            queryString = "UPDATE bogart_2.productdetails  SET GPCategory = 'GIFT' WHERE GPCategory IS NULL AND Category LIKE('>> Gift%')";
            executeNonQuery(queryString);

            queryString = "UPDATE bogart_2.productdetails SET GPCategory = 'OTHERDEV' WHERE GPCategory IS NULL AND Category = ('>> Activations >> Verizon Wireless >> Equipment >> Other Devices')";
            executeNonQuery(queryString);

            queryString = "UPDATE bogart_2.productdetails  SET GPCategory = 'DEVICES' WHERE GPCategory IS NULL AND Category Like('>> Activations >> Verizon Wireless >> Equipment%')";
            executeNonQuery(queryString);

            queryString = "UPDATE bogart_2.productdetails SET GPCategory = 'PLANS' WHERE GPCategory IS NULL AND Category Like('>> Activations >> Verizon Wireless%')";
            executeNonQuery(queryString);

            //this is meant to update all the corrections crated as a result of commmission reconciliation
            queryString = "UPDATE bogart_2.productdetails SET GPCategory = 'RECON' WHERE GPCategory IS NULL AND convert(varchar(8), SoldOn, 108) = '00:00:00' AND Category LIKE '>> Activations >> Verizon Wireless >> Phone Rebates (VZ Commission)%'";
            executeNonQuery(queryString);

            queryString = "UPDATE bogart_2.productdetails SET StoreTitle = 'Lockport' WHERE StoreTitle IS NULL  AND InvoicedBy = 'Wireless Zone Lockport WZ192'";
            executeNonQuery(queryString);

            queryString = "UPDATE bogart_2.productdetails SET StoreTitle = 'East Aurora' WHERE StoreTitle IS NULL AND InvoicedBy = 'Wireless Zone East Aurora WZ298'";
            executeNonQuery(queryString);

            queryString = "UPDATE bogart_2.productdetails SET StoreTitle = 'Springville' WHERE StoreTitle IS NULL AND InvoicedBy = 'Wireless Zone Springville WZ299'";
            executeNonQuery(queryString);

            queryString = "UPDATE bogart_2.productdetails SET CompanyTitle = 'Company' WHERE CompanyTitle IS NULL";
            executeNonQuery(queryString);


        }

        /* calcualte and assign TMP counts to single and multi-line TMP */
        private void calc_TMP_counts(long max_rec_id)
        {
            decimal tmp_count;

            // assign values to TMPCount field for single line TMP
            String queryString = "Update bogart_2.productdetails set TMPCount = Qty where TMPCount is null and ProductDetailLineID > " + max_rec_id +
                   " and ProductSKU in (Select ProductSKU from bogart_2.productskus where TMP = 1)";
            executeNonQuery(queryString);

            // assign values to TMPCount field for multi line TMP
            queryString = "Select ProductDetailLineID, InvoiceNo, ProductSKU, Qty from bogart_2.productdetails where ProductDetailLineID > " + max_rec_id + " and TMPCount is null " +
                  " and ProductSKU in (Select ProductSKU from bogart_2.productskus where TMPMulti = 1)";

            // Execute the query  
            SqlCommand command = new SqlCommand(queryString, connection);
            command.CommandTimeout = 600;
            SqlDataReader reader = command.ExecuteReader();

            // loop through the data
            if (reader.HasRows)
            {
                while (reader.Read())
                {
                    // find number of lines on this TMP invoice
                    queryString = "SELECT SUM(p.Qty) as TMPMultiCount FROM bogart_2.productdetails p, bogart_2.productskus s " +
                            " WHERE p.InvoiceNo = '" + reader.GetString(1) + "' AND p.ProductSKU = s.ProductSKU AND(s.NewLine = 1 OR s.Upgrade = 1)";
                    tmp_count = decimalValueProductDetailsQuery(queryString);

                    // update the TMP count on that multi-line record
                    queryString = "Update bogart_2.productdetails Set TMPCount = " + tmp_count + " Where ProductDetailLineID = " + reader.GetInt32(0);
                    executeNonQuery(queryString);
                }
            }

            // close reader 
            reader.Close();


        }
        public bool validateUser(String userName)
        {
            bool result;

            if (userName == "dave@bogart.com")
            {
                result = true;
            }
            else
            {


                String queryString = "Select UserName from bogart_2.users where Email = '" + userName + "' and Active = 1";

                // Execute the query  
                SqlCommand command = new SqlCommand(queryString, connection);
                command.CommandTimeout = 600;
                SqlDataReader reader = command.ExecuteReader();
                Console.WriteLine(queryString);

                reader.Read();

                // will only be one row.  MAX can't return more
                if (reader.HasRows)
                {
                    result = true;
                }
                else
                {
                    result = false;
                }

                // close reader 
                reader.Close();
            }
            return result;

        }
        public CommissionRates getCommissionData(String repName, DateTime calcDate)
        {
            CommissionRates rateSet = new CommissionRates();
            List<String> individualQualifiers = new List<String>();

            // use previous Sunday for effective date of commission qualifiers
            DateTime sunday = calcDate.AddDays(0 - calcDate.DayOfWeek).Date;
            String queryString = "Select FixedRate, BaseCommission, BaseGoal, Qualifiers from bogart_2.commissionBase " +
                "where AppliesTo = '" + repName + "' " +
                "AND startDate = (Select  MAX(StartDate) FROM bogart_2.commissionBase " +
                "WHERE AppliesTo = '" + repName + "' and startDate <= '" + sunday.ToString("d") + "' and (endDate >= '" + calcDate.ToString("d") + "' or endDate is null))";

            // Execute the query
            SqlCommand command = new SqlCommand(queryString, connection);
            SqlDataReader reader = command.ExecuteReader();

            // the first will be the one with the latest start date
            reader.Read();
            if (reader.HasRows)
            {

                // get data returned
                rateSet.fixedRate = reader.GetDecimal(0);
                rateSet.varRate = reader.GetDecimal(1);
                rateSet.varGoal = reader.GetDecimal(2);
                rateSet.qualifiersApply = reader.GetInt16(3);
                reader.Close();

                // now pull qualifiers
                if (rateSet.qualifiersApply == 1)
                {
                    queryString = "SELECT Category, Minimum, Rate, AppliesTo FROM bogart_2.commissionQualifiers cq " +
                            "WHERE  '" + calcDate.ToString("d") + "' BETWEEN StartDate AND EndDate " +
                            " AND AppliesTo = '" + repName + "' " +
                            "UNION SELECT Category, Minimum, Rate, AppliesTo FROM bogart_2.commissionQualifiers cq " +
                            "WHERE '" + calcDate.ToString("d") + "' BETWEEN cq.StartDate AND cq.EndDate " +
                            "AND AppliesTo = 'COMPANY' AND Category <> 'LAST_CALC_DATE'" +
                            "AND NOT EXISTS(SELECT * FROM bogart_2.commissionQualifiers cq1 WHERE '" + calcDate.ToString("d") + "' BETWEEN cq1.StartDate AND cq1.EndDate and cq1.AppliesTo = '" + repName + "' AND cq1.Category = cq.Category) " +
                            "Order by Category, Minimum, AppliesTo";

                    // get the company qualifiers for which there is not an overriding individual qualifier
                    queryString = "SELECT Category, Minimum, Rate, AppliesTo, RotationMinimum FROM bogart_2.commissionQualifiers cq " +
                                "WHERE AppliesTo = 'COMPANY' " +
                                "AND ( startDate <= '" + sunday.ToString("d") + "' AND ( endDate >= '" + calcDate.ToString("d") + "' or endDate is null))  AND Category <> 'LAST_CALC_DATE' " +
                                "AND NOT EXISTS(SELECT * " +
                                "FROM bogart_2.commissionQualifiers cq1 " +
                                "WHERE ( cq1.startDate <= '" + sunday.ToString("d") + "' AND ( cq1.endDate >= '" + calcDate.ToString("d") + "' or cq1.endDate is null)) " +
                                "AND cq1.AppliesTo = '" + repName + "' " +
                                "AND cq1.Category = cq.Category) Order by Category, Minimum, AppliesTo";

                    command.CommandText = queryString;
                    reader = command.ExecuteReader();

                    while (reader.Read())
                    {
                        Qualifier qualifier = new Qualifier();

                        qualifier.category = reader.GetString(0);
                        qualifier.minimum = reader.GetDecimal(1);
                        qualifier.rate = reader.GetDecimal(2);
                        qualifier.appliesTo = reader.GetString(3);
                        qualifier.rotationMinumum = reader.GetDecimal(4);

                        // add qualifiers to other commission data
                        rateSet.qualifiers.Add(qualifier);

                        qualifier = null;


                    }
                    reader.Close();

                    // get the individual qualifiers
                    queryString = "SELECT DISTINCT category FROM bogart_2.commissionQualifiers q " +
                                "WHERE AppliesTo = '" + repName + "' " +
                                "AND ( startDate <= '" + sunday.ToString("d") + "' AND( endDate >= '" + calcDate.ToString("d") + "' or endDate is null))";
                    command.CommandText = queryString;
                    reader = command.ExecuteReader();

                    // store qualifiers in a list
                    while (reader.Read())
                    {
                        individualQualifiers.Add(reader.GetString(0));
                    }
                    reader.Close();

                    // find applicable qualifier details for each category
                    foreach (String qualifierCategory in individualQualifiers)
                    {

                        queryString = "Select Category, minimum, rate, AppliesTo, RotationMinimum from bogart_2.commissionQualifiers " +
                                "where AppliesTo = '" + repName + "' " +
                                "AND Category =  '" + qualifierCategory + "' " +
                                "AND startDate = (Select  MAX(StartDate) FROM bogart_2.commissionQualifiers " +
                                "WHERE Category = '" + qualifierCategory + "' " +
                                "AND AppliesTo = '" + repName + "' and startDate <= '" + sunday.ToString("d") + "' and (endDate >= '" + calcDate.ToString("d") + "' or endDate is null))";
                        command.CommandText = queryString;
                        reader = command.ExecuteReader();
                        while (reader.Read())
                        {

                            // create new qualifier
                            Qualifier qualifier = new Qualifier();

                            qualifier.category = reader.GetString(0);
                            qualifier.minimum = reader.GetDecimal(1);
                            qualifier.rate = reader.GetDecimal(2);
                            qualifier.appliesTo = reader.GetString(3);
                            qualifier.rotationMinumum = reader.GetDecimal(4);

                            // add qualifiers to other commission data
                            rateSet.qualifiers.Add(qualifier);

                            qualifier = null;

                        }
                        reader.Close();
                    }
                }
            }


            return rateSet;

        }

    }


}








