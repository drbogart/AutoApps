using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text.RegularExpressions;

namespace bogart_wireless.Models
{
    public class Datascape
    {
        // private objects we can object throughout the class
        private SqlConnectionStringBuilder builder = new SqlConnectionStringBuilder();
        private SqlConnection connection;

        // static configuration data that applies to all objects of this type
        public static IEmailConfiguration emailConfiguration; // for email
        public static List<string> emailList = new List<string>();
        public static DatabaseConnectionSettings dbSettings; // for database connection
        public static GeneralSettings generalSettings; 

        public Datascape()
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
        }

        public void loadEmails()
        {
            List<EmailMessage> emails;
            EmailService emailSvc = new EmailService(emailConfiguration);
            emails = emailSvc.ReceiveEmail(10);
            if (emails.Count > 0)
            {
                // loop through emails
                foreach (EmailMessage email in emails)
                {
                    if (isDatascapeEmail(email.Subject, email.Content))
                    {
                        DatascapeEmail datascape = extractDatascapeData(email.Subject, email.HTMLContent);
                        datascape.dateReceived = email.MessageDate;
                        loadDatascapeEmailData(datascape);
                    }
                }
            }
        }

        private void loadDatascapeEmailData(DatascapeEmail datascape)
        {
            // check that this data hasn't already been loaded
            String queryString = "Select DRRID from bogart_2.DatascapeReconRecord where ATIStoreID = '" + datascape.storeID + "' and TransactionStartDate = '" + datascape.transStartDate.ToString("d") + "' and TransactionEndDate = '" + datascape.transEndDate.ToString("d") + "'";
            long DRRID = longValueQuery(queryString);

            if (DRRID == 0)
            {

                // create a new record in DatascapeReconRecord table and get DRRID
                string insertSQL = "Insert Into bogart_2.DatascapeReconRecord (ATIStoreID, DateATIReportReceived, TransactionStartDate, TransactionEndDate, ATIAmount) Values ('" + datascape.storeID + "', '" + datascape.dateReceived + "','" + datascape.transStartDate.ToString("d") + "', '" + datascape.transEndDate.ToString("d") + "', " + datascape.totalAmount + ")";
                executeNonQuery(insertSQL);
                queryString = "select Max(DRRID) from bogart_2.DatascapeReconRecord where ATIStoreID = '" + datascape.storeID + "' and DateATIReportReceived = '" + datascape.dateReceived.ToString("d") + "'";
                DRRID = longValueQuery(queryString);

                // load into datascape transaction table
                foreach (DatascapeTransaction trans in datascape.transactions)
                {
                    insertSQL = "Insert into bogart_2.DatascapeEmailTransactions (DRRID, DatascapeTransType, DateIn, AgentID, MobileNumber, SalesID, RateType, Platform, BatchID, ControlNumber, Invoice, PaymentAmount,TransFee, DebitAmount) Values (" + DRRID + ", '" + trans.transType + "', '" + trans.transDate.ToString("d") + "', '" + trans.agentID + "', '" + trans.mobileNumber + "', '" + trans.salesID + "', '" + trans.rateType + "', '" + trans.platform + "', '" + trans.batchID + "', '" + trans.controlNumber + "', '" + trans.invoiceNumber + "', " + trans.paymentAmount + ", " + trans.transFee + ", " + trans.debitAmount + ")";
                    executeNonQuery(insertSQL);
                }
            }

        }

        public void loadAndMatch()
        {

            loadRQData();
            transactionMatcher();
            sendDatascapeReports();

        }

        private void loadRQData()
        {
            loadRQDatascapeTransactions();

        }

        private void transactionMatcher()
        {
            matchDatascapeTransactions();
        }

        private DatascapeEmail extractDatascapeData(String subject, String message)
        {
            DatascapeEmail datascape = new DatascapeEmail();
            int transCount = 0;
            List<DateTime> transDates = new List<DateTime>();

            // get message info
            String pattern = @"\b(WZ192|WZ298|WZ299)\b";
            datascape.storeID = Regex.Match(subject, pattern, RegexOptions.IgnoreCase).Value;

            // split email into rows
            string[] rows = message.Split("<tr");

            // process each row
            foreach (string row in rows)
            {
                // split out each cell
                string[] cells = row.Split("<td");
                if (cells.Count() == 14)
                {
                    DatascapeTransaction thisTrans = new DatascapeTransaction();
                    transCount++;

                    // get trans type
                    string cellHtml = "<td" + cells[1];
                    string rawString = Regex.Replace(cellHtml, "<.*?>", string.Empty);
                    thisTrans.transType = rawString.Trim();

                    // get trans date
                    cellHtml = "<td" + cells[2];
                    rawString = Regex.Replace(cellHtml, "<.*?>", string.Empty);
                    string transDateString = rawString.Trim();
                    transDateString = transDateString.Substring(4, 2) + "/" + transDateString.Substring(6, 2) + "/" + transDateString.Substring(0, 4);
                    thisTrans.transDate = Convert.ToDateTime(transDateString);

                    // check for min and max transaction dates
                    if (thisTrans.transDate < datascape.transStartDate || datascape.transStartDate == DateTime.MinValue)
                    {
                        datascape.transStartDate = thisTrans.transDate.Date;
                    }
                    if (thisTrans.transDate > datascape.transEndDate)
                    {
                        datascape.transEndDate = thisTrans.transDate.Date;
                    }

                    // get agent ID
                    cellHtml = "<td" + cells[3];
                    rawString = Regex.Replace(cellHtml, "<.*?>", string.Empty);
                    thisTrans.agentID = Convert.ToInt16(rawString.Trim());

                    // get mobile number
                    cellHtml = "<td" + cells[4];
                    rawString = Regex.Replace(cellHtml, "<.*?>", string.Empty);
                    thisTrans.mobileNumber = rawString.Trim();

                    // get rep name
                    cellHtml = "<td" + cells[5];
                    rawString = Regex.Replace(cellHtml, "<.*?>", string.Empty);
                    thisTrans.salesID = rawString.Trim();

                    // get rate type
                    cellHtml = "<td" + cells[6];
                    rawString = Regex.Replace(cellHtml, "<.*?>", string.Empty);
                    thisTrans.rateType = rawString.Trim();

                    // get platform ID
                    cellHtml = "<td" + cells[7];
                    rawString = Regex.Replace(cellHtml, "<.*?>", string.Empty);
                    thisTrans.platform = rawString.Trim();

                    // get batch ID
                    cellHtml = "<td" + cells[8];
                    rawString = Regex.Replace(cellHtml, "<.*?>", string.Empty);
                    thisTrans.batchID = rawString.Trim();

                    // get control number
                    cellHtml = "<td" + cells[9];
                    rawString = Regex.Replace(cellHtml, "<.*?>", string.Empty);
                    thisTrans.controlNumber = rawString.Trim();

                    // get RQ invoice number entered by rep
                    cellHtml = "<td" + cells[10];
                    rawString = Regex.Replace(cellHtml, "<.*?>", string.Empty);
                    thisTrans.invoiceNumber = rawString.Trim();

                    // get payment amount
                    cellHtml = "<td" + cells[11];
                    rawString = Regex.Replace(cellHtml, "<.*?>", string.Empty);
                    thisTrans.paymentAmount = Convert.ToDecimal(rawString.Trim());

                    // transcation fee
                    cellHtml = "<td" + cells[12];
                    rawString = Regex.Replace(cellHtml, "<.*?>", string.Empty);
                    thisTrans.transFee = Convert.ToDecimal(rawString.Trim());

                    // debit amount
                    cellHtml = "<td" + cells[13];
                    rawString = Regex.Replace(cellHtml, "<.*?>", string.Empty);
                    thisTrans.debitAmount = Convert.ToDecimal(rawString.Trim());

                    datascape.transactions.Add(thisTrans);
                }
                else // not a transaction row
                {
                    // check whether this row contains total amount
                    if (row.Contains("has the following datascape transactions totaling"))
                    {
                        string amountString = row.Substring(row.IndexOf("$") + 1);
                        amountString = amountString.Substring(0, amountString.IndexOf(".") + 3);
                        amountString = Regex.Replace(amountString, "'", string.Empty);
                        datascape.totalAmount = Convert.ToDecimal(amountString);
                    }
                }
            }

            return datascape;
        }

        // check whether the subject and message indicate a Datascape email
        private bool isDatascapeEmail(String subject, string message)
        {
            if (subject.Contains("Datascape Notification")
                && message.Contains("ATI has the following datascape transactions totaling")
                && message.Contains("An ACH will be issued for this amount tomorrow and will be debited from your account number ending"))
            {
                return true;
            }
            else
            {
                return false;
            }

        }

        public void reconcileAll()
        {
            loadAndMatch();
        }


        public int loadRQDatascapeTransactions()
        {
            int numSets = 0;

            // get info on email transactions that have not been reconciled
            string queryString = "Select drr.DRRID, idr.RQStoreID, drr.TransactionStartDate, drr.TransactionEndDate from bogart_2.DatascapeReconRecord drr, bogart_2.Stores idr where drr.ATIStoreID = idr.WZNumber and drr.Reconciled=0 and drr.DRRID not in (Select DRRID from bogart_2.RQDatascapeTransactions)";

            // Execute the query
            SqlCommand command = new SqlCommand(queryString, connection);
            SqlDataReader reader = command.ExecuteReader();

            // process each record            
            if (reader.HasRows)
            {
                while (reader.Read())
                {
                    // parse data from DatascapeReconRecord
                    Int32 DRRID = reader.GetInt32(0);
                    String RQStoreID = reader.GetString(1);
                    DateTime transStartDate = reader.GetDateTime(2);
                    DateTime transEndDate = reader.GetDateTime(3);

                    // check whether we have RQ data for that date range
                    queryString = "Select SoldOn from bogart_2.productdetails where InvoicedAt = '" + RQStoreID + "' and SoldOn >= '" + transEndDate.ToString("d") + "'";
                    SqlCommand command2 = new SqlCommand(queryString, connection);
                    SqlDataReader reader2 = command2.ExecuteReader();
                    if (reader2.HasRows)
                    {
                        numSets++;

                        // pull the data from productdetails into RQDatascapeTransactions
                        reader2.Close();
                        DateTime endDate = transEndDate.AddDays(1);
                        queryString = "Insert into bogart_2.RQDatascapeTransactions (DRRID, SoldOn, InvoiceNo, SoldBy, Description, SoldFor, MobileNumber) Select " + DRRID + ", SoldOn, InvoiceNo, SoldBy, Description, AdjustedUnitPrice, TrackingNo from bogart_2.productdetails where InvoicedAt = '" + RQStoreID + "' and SoldOn >= '" + transStartDate + "' and SoldOn < '" + endDate + "' and Category Like '>> Bill Payment%' and Description in ('Datascape Postpaid Payment','Datascape Prepaid Refill', 'Datascape DPP Buy Out Postpaid Payment')";
                        command2.CommandText = queryString;
                        reader2 = command2.ExecuteReader();
                    }

                }
            }
            return numSets;

        }

        private bool transTypeMatch(String emailTransType, String RQTransType)
        {
            String queryString = "Select 1 from bogart_2.DatascapeTransTypeMatches where DatascapeTransType = '" + emailTransType + "' and RQTransType = '" + RQTransType + "'";
            if (longValueQuery(queryString) == 1)
            {
                return true;
            }
            else
            {
                return false;
            }

        }

        public int matchDatascapeTransactions()
        {
            int numBatches = 0;
            Int32 DRRID = 0;
            Int32 RQDTID = 0;
            Int32 ADTID = 0;
            String queryString;
            String insertSQL;
            String partialMsg;

            // find datascape batches that have not been reconciled 
            queryString = "Select DRRID, ATIStoreID, DateATIReportReceived, TransactionStartDate, TransactionEndDate, ATIAmount from bogart_2.DatascapeReconRecord drr where Reconciled = 0 and EXISTS (Select * from bogart_2.RQDatascapeTransactions where DRRID = drr.DRRID)";
            SqlCommand command = new SqlCommand(queryString, connection);
            SqlDataReader reader = command.ExecuteReader();

            // loop through unreconciled RQ records
            while (reader.Read())
            {
                numBatches++;

                // pull records from RQDatascapeTransactions
                DRRID = reader.GetInt32(0);
                queryString = "Select RQDTID,SoldOn, InvoiceNo, SoldBy, Description, SoldFor, MobileNumber from bogart_2.RQDatascapeTransactions where DRRID = " + DRRID;
                SqlCommand commandRQ = new SqlCommand(queryString, connection);
                SqlDataReader readerRQ = commandRQ.ExecuteReader();

                while (readerRQ.Read())
                {

                    // check for a matching email record
                    RQDTID = readerRQ.GetInt32(0);
                    String transType = readerRQ.GetString(4);
                    String mobileNumber = readerRQ.GetString(6);
                    DateTime soldOn = readerRQ.GetDateTime(1).Date;
                    Decimal paymentAmount = readerRQ.GetDecimal(5);
                    queryString = "Select ADTID from bogart_2.DatascapeEmailTransactions adt, bogart_2.DatascapeTransTypeMatches dttm where dttm.DatascapeTransType = adt.DatascapeTransType and dttm.RQTransType = '" + transType + "' and DRRID = " + DRRID + " and MobileNumber = '" + mobileNumber + "' and DateIn = '" + soldOn.ToString("d") + "' and PaymentAmount = " + paymentAmount.ToString() + " and not exists (select * from bogart_2.DatascapeTransMatch where ADTID = adt.ADTID)";
                    SqlCommand command2 = new SqlCommand(queryString, connection);
                    SqlDataReader reader2 = command2.ExecuteReader();

                    // do we have a matching record?
                    if (reader2.HasRows)
                    {
                        // add record to DatascapeTransMatch
                        reader2.Read();
                        ADTID = reader2.GetInt32(0);
                        insertSQL = "Insert into bogart_2.DatascapeTransMatch (RQDTID, ADTID, Partial) values (" + RQDTID + "," + ADTID + ", 0)";
                        executeNonQuery(insertSQL);

                    }
                    reader2.Close();
                }
                readerRQ.Close();

                // perform another loop of unmatched records to search for partial matches
                queryString = "Select RQDTID,SoldOn, InvoiceNo, SoldBy, Description, SoldFor, MobileNumber from bogart_2.RQDatascapeTransactions rdt where DRRID = " + DRRID + "and not exists(select ACTMID from bogart_2.DatascapeTransMatch where RQDTID = rdt.RQDTID)";
                commandRQ.CommandText = queryString; 
                readerRQ = commandRQ.ExecuteReader();
                while (readerRQ.Read())
                {
                    partialMsg = "";

                    RQDTID = readerRQ.GetInt32(0);
                    String transType = readerRQ.GetString(4);
                    String mobileNumber = readerRQ.GetString(6);
                    DateTime soldOn = readerRQ.GetDateTime(1).Date;
                    Decimal paymentAmount = readerRQ.GetDecimal(5);

                    // pull all unmatched email transactions to check for a partial match to this one
                    queryString = "Select ADTID, DRRID, DatascapeTransType, DateIn, AgentID, MobileNumber, SalesID, RateType, Platform, BatchID, ControlNumber, Invoice, PaymentAmount, TransFee, DebitAmount from bogart_2.DatascapeEmailTransactions adt where DRRID = " + DRRID + " and not exists (select ACTMID from bogart_2.DatascapeTransMatch where ADTID = adt.ADTID)";
                    SqlCommand command3 = new SqlCommand(queryString, connection);
                    SqlDataReader reader3 = command3.ExecuteReader();

                    // loop through each of the unmatched email records
                    while (reader3.Read())
                    {
                        // extract data from email record
                        DateTime emailTransDate = reader3.GetDateTime(3);
                        Decimal emailAmount = reader3.GetDecimal(12);
                        String emailMobileNumber = reader3.GetString(5);
                        String emailTransType = reader3.GetString(2);
                        ADTID = reader3.GetInt32(0);

                        // check whether this email record matches using only amount and date, ignore mobile number and trans type
                        if (emailAmount == paymentAmount && emailTransDate == soldOn)
                        {

                            // is it mobile numbers that don't match?
                            if (emailMobileNumber != mobileNumber && transTypeMatch(emailTransType, transType))
                            {
                                partialMsg = "Mobile numbers do not match; ";
                            }

                            // is it Trans Types that don't match?
                            if (transTypeMatch(emailTransType, transType) == false && (emailMobileNumber == mobileNumber))
                            {
                                partialMsg += "Transaction types do not match";

                            }
                        }

                        if (partialMsg != "")
                        {
                            // add a record to DatascapeTransMatch
                            insertSQL = "Insert into bogart_2.DatascapeTransMatch (RQDTID, ADTID, Partial, MatchNote) values (" + RQDTID + "," + ADTID + ", 1, '" + partialMsg + "')";
                            executeNonQuery(insertSQL);
                            break;
                        }

                        else
                        {
                            // we did not find a match on amount, search for matching mobile number  and trans type, but non-matching amount
                            if (emailMobileNumber == mobileNumber && transTypeMatch(emailTransType, transType) && emailAmount != paymentAmount)
                            {
                                partialMsg = "Amounts do not match";
                                queryString = "Insert into bogart_2.DatascapeTransMatch (RQDTID, ADTID, Partial, MatchNote) values (" + RQDTID + "," + ADTID + ", 1, '" + partialMsg + "')";
                                executeNonQuery(queryString);
                            }
                        }
                    }
           
                }

                // update the reconciled flag
                String updateSQL = "Update bogart_2.DatascapeReconRecord set reconciled = 1 where DRRID = '" + DRRID.ToString() + "'";
                executeNonQuery(updateSQL);




                Boolean issuesFound = false;


                //Partial matches
                queryString = "SELECT RQDTID, ADTID, Partial, MatchNote FROM bogart_2.DatascapeTransMatch WHERE Partial = 1 AND RQDTID IN (SELECT RQDTID FROM bogart_2.RQDatascapeTransactions WHERE DRRID = " + DRRID + ")";

                SqlCommand commandIssues = new SqlCommand(queryString, connection);
                SqlDataReader readerIssues = commandIssues.ExecuteReader();
                while (readerIssues.Read())
                {
                    issuesFound = true;
                    Int32 RQDTID2 = readerIssues.GetInt32(0);
                    Int32 ID = readerIssues.GetInt32(1);
                    Int16 partialMatch = readerIssues.GetInt16(2);
                    String matchNote = readerIssues.GetString(3);

                    insertSQL =
                        "INSERT INTO bogart_2.DatascapeReconciliationIssues (DRRID,IssueType, ADTID, ATIInvoice, ATIMobileNumber,ATITransactionDate, " +
                        "ATITransactionType, ATIRep, ATIAmount, RQDTID, RQInvoice, RQMobileNumber, RQTransactionDate, RQTransactionType, " +
                        "RQRep, RQAmount, MatchNote) Values (" + DRRID + ",'Partial Match'," + ID + "," +
                        "(SELECT Invoice FROM bogart_2.DatascapeEmailTransactions WHERE ADTID = " + ID + ")," +
                        "(SELECT MobileNumber FROM bogart_2.DatascapeEmailTransactions WHERE ADTID = " + ID + ")," +
                        "(SELECT DateIn FROM bogart_2.DatascapeEmailTransactions WHERE ADTID = " + ID + ")," +
                        "(SELECT DatascapeTransType FROM bogart_2.DatascapeEmailTransactions WHERE ADTID = " + ID + ")," +
                        "(SELECT SalesID FROM bogart_2.DatascapeEmailTransactions WHERE ADTID = " + ID + ")," +
                        "(SELECT PaymentAmount FROM bogart_2.DatascapeEmailTransactions WHERE ADTID = " + ID + ")," + RQDTID2 + "," +
                        "(SELECT InvoiceNo FROM bogart_2.RQDatascapeTransactions WHERE RQDTID = " + RQDTID2 + ")," +
                        "(SELECT MobileNumber FROM bogart_2.RQDatascapeTransactions WHERE RQDTID = " + RQDTID2 + ")," +
                        "(SELECT SoldOn FROM bogart_2.RQDatascapeTransactions WHERE RQDTID = " + RQDTID2 + ")," +
                        "(SELECT Description FROM bogart_2.RQDatascapeTransactions WHERE RQDTID = " + RQDTID2 + ")," +
                        "(SELECT SoldBy FROM bogart_2.RQDatascapeTransactions WHERE RQDTID = " + RQDTID2 + ")," +
                        "(SELECT SoldFor FROM bogart_2.RQDatascapeTransactions WHERE RQDTID = " + RQDTID2 + ")," +
                        "'" + matchNote + "')";
                    executeNonQuery(insertSQL);

                }
                readerIssues.Close();

                //Unmatched email records
                queryString = "SELECT ADTID, DRRID, DatascapeTransType, DateIn, AgentID, MobileNumber, SalesID, RateType, Platform, BatchID, ControlNumber, Invoice, PaymentAmount, TransFee, DebitAmount  FROM bogart_2.DatascapeEmailTransactions WHERE DRRID = " + DRRID + " AND ADTID NOT IN (SELECT ADTID FROM bogart_2.DatascapeTransMatch)";
                commandIssues.CommandText = queryString;
                readerIssues = commandIssues.ExecuteReader();
                while (readerIssues.Read())
                {

                    issuesFound = true;
                    Int32 ID = readerIssues.GetInt32(0);
                    insertSQL = "INSERT INTO bogart_2.DatascapeReconciliationIssues (DRRID, IssueType, ADTID, ATIInvoice, ATIMobileNumber, ATITransactionDate, " +
                                "ATITransactionType, ATIRep, ATIAmount, MatchNote) Values (" +
                                DRRID + ", 'Not in RQ', " + ID + ", " +
                        "(SELECT Invoice FROM  bogart_2.DatascapeEmailTransactions WHERE ADTID = " + ID + ")," +
                        "(SELECT MobileNumber FROM  bogart_2.DatascapeEmailTransactions WHERE ADTID = " + ID + ")," +
                        "(SELECT DateIn FROM  bogart_2.DatascapeEmailTransactions WHERE  ADTID = " + ID + ")," +
                        "(SELECT DatascapeTransType FROM  bogart_2.DatascapeEmailTransactions WHERE  ADTID = " + ID + ")," +
                        "(SELECT SalesID FROM  bogart_2.DatascapeEmailTransactions WHERE  ADTID = " + ID + ")," +
                        "(SELECT PaymentAmount FROM  bogart_2.DatascapeEmailTransactions WHERE ADTID = " + ID + ")," +
                        "'Not in RQ')";
                    executeNonQuery(insertSQL);

                }
                readerIssues.Close();

                //RQ records not in Email  Report
                queryString = "SELECT RQDTID FROM bogart_2.RQDatascapeTransactions WHERE DRRID = " + DRRID + " AND RQDTID NOT IN (SELECT RQDTID FROM bogart_2.DatascapeTransMatch)";
                commandIssues.CommandText = queryString;
                readerIssues = commandIssues.ExecuteReader();
                while (readerIssues.Read())
                {

                    issuesFound = true;
                    Int32 ID = readerIssues.GetInt32(0);

                    insertSQL = "INSERT INTO bogart_2.DatascapeReconciliationIssues (DRRID, IssueType, RQDTID, RQInvoice , RQMobileNumber, " +
                        "RQTransactionDate, RQTransactionType, RQRep, RQAmount, MatchNote) Values (" + DRRID + ", 'Not in Email Report', " + ID + "," +
                        "(SELECT InvoiceNo FROM bogart_2.RQDatascapeTransactions WHERE RQDTID = " + ID + ")," +
                        "(SELECT MobileNumber FROM bogart_2.RQDatascapeTransactions WHERE RQDTID = " + ID + ")," +
                        "(SELECT SoldOn FROM bogart_2.RQDatascapeTransactions WHERE RQDTID = " + ID + ")," +
                        "(SELECT Description FROM bogart_2.RQDatascapeTransactions WHERE RQDTID = " + ID + ")," +
                        "(SELECT SoldBy FROM bogart_2.RQDatascapeTransactions WHERE RQDTID = " + ID + ")," +
                        "(SELECT SoldFor FROM bogart_2.RQDatascapeTransactions WHERE RQDTID = " + ID + ")," +
                        "'Not in Email Report')";
                    executeNonQuery(insertSQL);

                }

            }

            return numBatches;

        }

        // Working as of 9/3 but mismatching partial.  Delete this once we have matchDatascapeTransactions working properly.
        private int oldMatchDatascapeTransactions()
        {
            int numBatches = 0;
            Int32 DRRID = 0;
            Int32 RQDTID = 0;
            Int32 ADTID = 0;
            String queryString;
            String insertSQL;
            String partialMsg;

            // find datascape batches that have not been reconciled 
            queryString = "Select DRRID, ATIStoreID, DateATIReportReceived, TransactionStartDate, TransactionEndDate, ATIAmount from bogart_2.DatascapeReconRecord drr where Reconciled = 0 and EXISTS (Select * from bogart_2.RQDatascapeTransactions where DRRID = drr.DRRID)";
            SqlCommand command = new SqlCommand(queryString, connection);
            SqlDataReader reader = command.ExecuteReader();

            // loop through unreconciled RQ records
            while (reader.Read())
            {
                numBatches++;

                // pull records from RQDatascapeTransactions
                DRRID = reader.GetInt32(0);
                queryString = "Select RQDTID,SoldOn, InvoiceNo, SoldBy, Description, SoldFor, MobileNumber from bogart_2.RQDatascapeTransactions where DRRID = " + DRRID;
                SqlCommand commandRQ = new SqlCommand(queryString, connection);
                SqlDataReader readerRQ = commandRQ.ExecuteReader();

                while (readerRQ.Read())
                {
                    partialMsg = "";

                    // check for a matching email record
                    RQDTID = readerRQ.GetInt32(0);
                    String transType = readerRQ.GetString(4);
                    String mobileNumber = readerRQ.GetString(6);
                    DateTime soldOn = readerRQ.GetDateTime(1).Date;
                    Decimal paymentAmount = readerRQ.GetDecimal(5);
                    queryString = "Select ADTID from bogart_2.DatascapeEmailTransactions adt, bogart_2.DatascapeTransTypeMatches dttm where dttm.DatascapeTransType = adt.DatascapeTransType and dttm.RQTransType = '" + transType + "' and DRRID = " + DRRID + " and MobileNumber = '" + mobileNumber + "' and DateIn = '" + soldOn.ToString("d") + "' and PaymentAmount = " + paymentAmount.ToString() + " and not exists (select * from bogart_2.DatascapeTransMatch where ADTID = adt.ADTID)";
                    SqlCommand command2 = new SqlCommand(queryString, connection);
                    SqlDataReader reader2 = command2.ExecuteReader();

                    // do we have a matching record?
                    if (reader2.HasRows)
                    {
                        // add record to DatascapeTransMatch
                        reader2.Read();
                        ADTID = reader2.GetInt32(0);
                        insertSQL = "Insert into bogart_2.DatascapeTransMatch (RQDTID, ADTID, Partial) values (" + RQDTID + "," + ADTID + ", 0)";
                        executeNonQuery(insertSQL);

                    }
                    else  // did not find an exact match in RQ email
                    {

                        // pull all unmatched email transactions to check for a partial match to this one
                        queryString = "Select ADTID, DRRID, DatascapeTransType, DateIn, AgentID, MobileNumber, SalesID, RateType, Platform, BatchID, ControlNumber, Invoice, PaymentAmount, TransFee, DebitAmount from bogart_2.DatascapeEmailTransactions adt where DRRID = " + DRRID + " and not exists (select ACTMID from bogart_2.DatascapeTransMatch where ADTID = adt.ADTID)";
                        SqlCommand command3 = new SqlCommand(queryString, connection);
                        SqlDataReader reader3 = command3.ExecuteReader();

                        // loop through each of the unmatched email records
                        while (reader3.Read())
                        {
                            // extract data from email record
                            DateTime emailTransDate = reader3.GetDateTime(3);
                            Decimal emailAmount = reader3.GetDecimal(12);
                            String emailMobileNumber = reader3.GetString(5);
                            String emailTransType = reader3.GetString(2);
                            ADTID = reader3.GetInt32(0);

                            // check whether this email record matches using only amount and date, ignore mobile number and trans type
                            if (emailAmount == paymentAmount && emailTransDate == soldOn)
                            {

                                // is it mobile numbers that don't match?
                                if (emailMobileNumber != mobileNumber && transTypeMatch(emailTransType, transType))
                                {
                                    partialMsg = "Mobile numbers do not match; ";
                                }

                                // is it Trans Types that don't match?
                                if (transTypeMatch(emailTransType, transType) == false && (emailMobileNumber == mobileNumber))
                                {
                                    partialMsg += "Transaction types do not match";

                                }
                            }

                            if (partialMsg != "")
                            {
                                // add a record to DatascapeTransMatch
                                insertSQL = "Insert into bogart_2.DatascapeTransMatch (RQDTID, ADTID, Partial, MatchNote) values (" + RQDTID + "," + ADTID + ", 1, '" + partialMsg + "')";
                                executeNonQuery(insertSQL);
                                break;
                            }

                            else
                            {
                                // we did not find a match on amount, search for matching mobile number  and trans type, but non-matching amount
                                if (emailMobileNumber == mobileNumber && transTypeMatch(emailTransType, transType) && emailAmount != paymentAmount)
                                {
                                    partialMsg = "Amounts do not match";
                                    queryString = "Insert into bogart_2.DatascapeTransMatch (RQDTID, ADTID, Partial, MatchNote) values (" + RQDTID + "," + ADTID + ", 1, '" + partialMsg + "')";
                                    executeNonQuery(queryString);
                                }
                            }
                        }


                    }
                    reader2.Close();
                }

                // update the reconciled flag
                String updateSQL = "Update bogart_2.DatascapeReconRecord set reconciled = 1 where DRRID = '" + DRRID.ToString() + "'";
                executeNonQuery(updateSQL);




                Boolean issuesFound = false;


                //Partial matches
                queryString = "SELECT RQDTID, ADTID, Partial, MatchNote FROM bogart_2.DatascapeTransMatch WHERE Partial = 1 AND RQDTID IN (SELECT RQDTID FROM bogart_2.RQDatascapeTransactions WHERE DRRID = " + DRRID + ")";

                SqlCommand commandIssues = new SqlCommand(queryString, connection);
                SqlDataReader readerIssues = commandIssues.ExecuteReader();
                while (readerIssues.Read())
                {
                    issuesFound = true;
                    Int32 RQDTID2 = readerIssues.GetInt32(0);
                    Int32 ID = readerIssues.GetInt32(1);
                    Int16 partialMatch = readerIssues.GetInt16(2);
                    String matchNote = readerIssues.GetString(3);

                    insertSQL =
                        "INSERT INTO bogart_2.DatascapeReconciliationIssues (DRRID,IssueType, ADTID, ATIInvoice, ATIMobileNumber,ATITransactionDate, " +
                        "ATITransactionType, ATIRep, ATIAmount, RQDTID, RQInvoice, RQMobileNumber, RQTransactionDate, RQTransactionType, " +
                        "RQRep, RQAmount, MatchNote) Values (" + DRRID + ",'Partial Match'," + ID + "," +
                        "(SELECT Invoice FROM bogart_2.DatascapeEmailTransactions WHERE ADTID = " + ID + ")," +
                        "(SELECT MobileNumber FROM bogart_2.DatascapeEmailTransactions WHERE ADTID = " + ID + ")," +
                        "(SELECT DateIn FROM bogart_2.DatascapeEmailTransactions WHERE ADTID = " + ID + ")," +
                        "(SELECT DatascapeTransType FROM bogart_2.DatascapeEmailTransactions WHERE ADTID = " + ID + ")," +
                        "(SELECT SalesID FROM bogart_2.DatascapeEmailTransactions WHERE ADTID = " + ID + ")," +
                        "(SELECT PaymentAmount FROM bogart_2.DatascapeEmailTransactions WHERE ADTID = " + ID + ")," + RQDTID2 + "," +
                        "(SELECT InvoiceNo FROM bogart_2.RQDatascapeTransactions WHERE RQDTID = " + RQDTID2 + ")," +
                        "(SELECT MobileNumber FROM bogart_2.RQDatascapeTransactions WHERE RQDTID = " + RQDTID2 + ")," +
                        "(SELECT SoldOn FROM bogart_2.RQDatascapeTransactions WHERE RQDTID = " + RQDTID2 + ")," +
                        "(SELECT Description FROM bogart_2.RQDatascapeTransactions WHERE RQDTID = " + RQDTID2 + ")," +
                        "(SELECT SoldBy FROM bogart_2.RQDatascapeTransactions WHERE RQDTID = " + RQDTID2 + ")," +
                        "(SELECT SoldFor FROM bogart_2.RQDatascapeTransactions WHERE RQDTID = " + RQDTID2 + ")," +
                        "'" + matchNote + "')";
                    executeNonQuery(insertSQL);

                }
                readerIssues.Close();

                //Unmatched email records
                queryString = "SELECT ADTID, DRRID, DatascapeTransType, DateIn, AgentID, MobileNumber, SalesID, RateType, Platform, BatchID, ControlNumber, Invoice, PaymentAmount, TransFee, DebitAmount  FROM bogart_2.DatascapeEmailTransactions WHERE DRRID = " + DRRID + " AND ADTID NOT IN (SELECT ADTID FROM bogart_2.DatascapeTransMatch)";
                commandIssues.CommandText = queryString;
                readerIssues = commandIssues.ExecuteReader();
                while (readerIssues.Read())
                {

                    issuesFound = true;
                    Int32 ID = readerIssues.GetInt32(0);
                    insertSQL = "INSERT INTO bogart_2.DatascapeReconciliationIssues (DRRID, IssueType, ADTID, ATIInvoice, ATIMobileNumber, ATITransactionDate, " +
                                "ATITransactionType, ATIRep, ATIAmount, MatchNote) Values (" +
                                DRRID + ", 'Not in RQ', " + ID + ", " +
                        "(SELECT Invoice FROM  bogart_2.DatascapeEmailTransactions WHERE ADTID = " + ID + ")," +
                        "(SELECT MobileNumber FROM  bogart_2.DatascapeEmailTransactions WHERE ADTID = " + ID + ")," +
                        "(SELECT DateIn FROM  bogart_2.DatascapeEmailTransactions WHERE  ADTID = " + ID + ")," +
                        "(SELECT DatascapeTransType FROM  bogart_2.DatascapeEmailTransactions WHERE  ADTID = " + ID + ")," +
                        "(SELECT SalesID FROM  bogart_2.DatascapeEmailTransactions WHERE  ADTID = " + ID + ")," +
                        "(SELECT PaymentAmount FROM  bogart_2.DatascapeEmailTransactions WHERE ADTID = " + ID + ")," +
                        "'Not in RQ')";
                    executeNonQuery(insertSQL);

                }
                readerIssues.Close();

                //RQ records not in Email  Report
                queryString = "SELECT RQDTID FROM bogart_2.RQDatascapeTransactions WHERE DRRID = " + DRRID + " AND RQDTID NOT IN (SELECT RQDTID FROM bogart_2.DatascapeTransMatch)";
                commandIssues.CommandText = queryString;
                readerIssues = commandIssues.ExecuteReader();
                while (readerIssues.Read())
                {

                    issuesFound = true;
                    Int32 ID = readerIssues.GetInt32(0);

                    insertSQL = "INSERT INTO bogart_2.DatascapeReconciliationIssues (DRRID, IssueType, RQDTID, RQInvoice , RQMobileNumber, " +
                        "RQTransactionDate, RQTransactionType, RQRep, RQAmount, MatchNote) Values (" + DRRID + ", 'Not in Email Report', " + ID + "," +
                        "(SELECT InvoiceNo FROM bogart_2.RQDatascapeTransactions WHERE RQDTID = " + ID + ")," +
                        "(SELECT MobileNumber FROM bogart_2.RQDatascapeTransactions WHERE RQDTID = " + ID + ")," +
                        "(SELECT SoldOn FROM bogart_2.RQDatascapeTransactions WHERE RQDTID = " + ID + ")," +
                        "(SELECT Description FROM bogart_2.RQDatascapeTransactions WHERE RQDTID = " + ID + ")," +
                        "(SELECT SoldBy FROM bogart_2.RQDatascapeTransactions WHERE RQDTID = " + ID + ")," +
                        "(SELECT SoldFor FROM bogart_2.RQDatascapeTransactions WHERE RQDTID = " + ID + ")," +
                        "'Not in Email Report')";
                    executeNonQuery(insertSQL);

                }

            }

            return numBatches;

        }
        public void sendDatascapeReports()
        {
            String emailInvoice;
            String RQInvoice;
            String emailMobileNumber;
            String RQMobileNumber;
            String emailDate;
            String RQDate;
            String emailTransType;
            String RQTransType;
            String emailRep;
            String RQRep;
            String MatchNote;
            String emailAmount;
            String RQAmount;

            // get list of reconciled batches not yet reported
            String queryString = "SELECT DRRID, ATIStoreID, DateATIReportReceived, TransactionStartDate, TransactionEndDate, ATIAmount, Reconciled, AmountOK FROM bogart_2.DatascapeReconRecord WHERE Reconciled = 1 and Reported = 0";
            SqlCommand command = new SqlCommand(queryString, connection);
            SqlDataReader reader = command.ExecuteReader();

            while (reader.Read())
            {

                Int32 DRRID = reader.GetInt32(0);
                String storeID = reader.GetString(1);
                DateTime startDate = reader.GetDateTime(3);
                DateTime endDate = reader.GetDateTime(4);

                String emailSubject = storeID + " Datascape Reconciliation for " + startDate.ToShortDateString();
                if (endDate > startDate)
                {
                    emailSubject += " thru " + endDate.ToShortDateString();
                }


                String message = "<html><body>";
                message += "<h1 style='font-size: 16px'>" + emailSubject + "</h1>";
                message += "<p>Please investigate the following issues:</p>";
                message += "<table>";

                // check whether any issues exist
                String issueQuery = "SELECT ATIInvoice, RQInvoice, ATIMobileNumber, RQMobileNumber, ATITransactionDate, RQTransactionDate, ATITransactionType, RQTransactionType, ATIRep, RQRep, ATIAmount, RQAmount, MatchNote FROM bogart_2.DatascapeReconciliationIssues WHERE DRRID = " + DRRID;
                SqlCommand issueCommand = new SqlCommand(issueQuery, connection);
                SqlDataReader issueReader = issueCommand.ExecuteReader();
                    int issueCount = 0;
                    while (issueReader.Read())
                    {
                        issueCount++;
                       
                        if (issueReader.IsDBNull(0)) { emailInvoice = ""; } else { emailInvoice = issueReader.GetString(0); }
                        if (issueReader.IsDBNull(1)) { RQInvoice = ""; } else { RQInvoice = issueReader.GetString(1); }
                        if (issueReader.IsDBNull(2)) { emailMobileNumber = ""; } else { emailMobileNumber = issueReader.GetString(2); }
                        if (issueReader.IsDBNull(3)) { RQMobileNumber = ""; } else { RQMobileNumber = issueReader.GetString(3); }
                        if (issueReader.IsDBNull(4)) { emailDate = ""; } else  { emailDate = issueReader.GetDateTime(4).ToShortDateString(); }
                        if (issueReader.IsDBNull(5)) { RQDate = ""; } else { RQDate = issueReader.GetDateTime(5).ToShortDateString(); }
                        if (issueReader.IsDBNull(6)) { emailTransType = ""; } else { emailTransType = issueReader.GetString(6); }
                        if (issueReader.IsDBNull(7)) { RQTransType = ""; } else { RQTransType = issueReader.GetString(7); }
                        if (issueReader.IsDBNull(8)) { emailRep = ""; } else { emailRep = issueReader.GetString(8); }
                        if (issueReader.IsDBNull(9)) { RQRep = ""; } else { RQRep = issueReader.GetString(9); }
                        if (issueReader.IsDBNull(10)) { emailAmount = ""; } else { emailAmount = issueReader.GetDecimal(10).ToString(); }
                        if (issueReader.IsDBNull(11)) { RQAmount = ""; } else { RQAmount = issueReader.GetDecimal(11).ToString(); }
                        if (issueReader.IsDBNull(12)) { MatchNote = ""; } else { MatchNote = issueReader.GetString(12); }

                        message += "<tr><th colspan='2' style='font-weight: bold; text-align: center;'>Issue No. " + issueCount + "</th></tr>";
                        message += "<tr><th>&nbsp;</th><th style='font-weight: bold; text-align: center;'>Email Report</th><th style='font-weight: bold; text-align: center;'>RQ</th></tr>";
                        message += "<tr><th style='font-weight: bold; text-align: right;'>Invoice:</th><td>" + emailInvoice + "</td><td>" + RQInvoice + "</td></tr>";
                        message += "<tr><th style='font-weight: bold; text-align: right;'>Mobile:</th><td>" + emailMobileNumber + "</td><td>" + RQMobileNumber + "</td></tr>";
                        message += "<tr><th style='font-weight: bold; text-align: right;'>Date:</th><td>" + emailDate + "</td><td>" + RQDate + "</td></tr>";
                        message += "<tr><th style='font-weight: bold; text-align: right;'>Type:</th><td>" + emailTransType + "</td><td>" + RQTransType + "</td></tr>";
                        message += "<tr><th style='font-weight: bold; text-align: right;'>Rep:</th><td>" + emailRep + "</td><td>" + RQRep + "</td></tr>";
                        message += "<tr><th style='font-weight: bold; text-align: right;'>Amount:</th><td>" + emailAmount + "</td><td>" + RQAmount + "</td></tr>";
                        message += "<tr><th style='font-weight: bold; text-align: right;'>Note:</th><td colspan='2'>" + MatchNote + "</td></tr>";
                        message += "<tr><th>&nbsp;</th></tr>";


                    }
                if (issueCount == 0) { 
                    // this is a really ugly fudge but mail() doesn't work when sending a simple "No issues found."
                    message += "<tr><th colspan='2' style='ont-weight: bold; text-align: center;'>No issues found.</th></tr>";

                }
                message += "</table></body></html>";

                // send a report email
                EmailService emailService = new EmailService(Datascape.emailConfiguration);
                EmailMessage emailMessage = new EmailMessage();
                
                string[] emailList = generalSettings.DatascapeEmailList.Split(',');
                foreach (String email in emailList)
                {
                    EmailAddress emailToAddress = new EmailAddress();
                    emailToAddress.Address = email;
                    emailMessage.ToAddresses.Add(emailToAddress);
                    //emailService.QuickSend(emailSubject, message, email, "");
                }
                emailMessage.Subject = emailSubject;
                emailMessage.Content = message;
                emailService.Send(emailMessage);

                // update DatascapeReconRecord
                String updateSQL = "Update bogart_2.datascapeReconRecord set Reported = 1 where DRRID = " + DRRID;
                executeNonQuery(updateSQL);
            }
        }

        private void executeNonQuery(String queryString)
        {
            // Execute the query  
            SqlCommand command = new SqlCommand(queryString, connection);
            command.CommandTimeout = 600;
            command.ExecuteNonQuery();

        }

        public long longValueQuery(string queryString)
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
    }
}
