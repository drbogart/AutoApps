using System;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Options;
using AutoApps.Models;
using bogart_wireless.Libraries;
using System.Security.Cryptography.X509Certificates;
using Google.Apis.Auth.OAuth2;
using System.IO;
using Google.Apis.Auth.OAuth2.Flows;
using Google.Apis.Util.Store;
using System.Threading;
using Google.Apis.Util;
using System.Threading.Tasks;


using Google.Apis.Services;
using Google.Apis.Gmail.v1;
using Google.Apis.Gmail.v1.Data;
using System.Collections.Generic;

using Google.Apis.Auth.OAuth2.Mvc;
using MailKit.Security;
using MailKit.Net.Smtp;
using MimeKit;
using MimeKit.Text;
using System.Text;


namespace bogart_wireless.Controllers
{

    public class AutoController : Controller
    {
        readonly IEmailConfiguration _configuration; // for email

        public AutoController(IOptionsSnapshot<EmailConfiguration> configuration, IOptions<DatabaseConnectionSettings> dbsettings, IOptions<GeneralSettings> generalSettings, IOptions<OAuth2Configuration> oauthSettings)
        {
            _configuration = configuration.Get("ReportFiles");
            Datascape.emailConfiguration = configuration.Get("Datascape");
            Datascape.clientEmailConfiguration = configuration.Get("ExternalClients");
            Datascape.dbSettings = dbsettings.Value;
            Datascape.generalSettings = generalSettings.Value;
            SalesData.emailConfiguration = configuration.Get("ReportFiles");
            SalesData.clientEmailConfiguration = configuration.Get("ExternalClients");
            SalesData.dbSettings = dbsettings.Value;
            SalesData.generalSettings = generalSettings.Value;
            Payroll.generalSettings = generalSettings.Value;
            Payroll.dbSettings = dbsettings.Value;
            ProductDetailsData.dbSettings = dbsettings.Value;
            SlingTimeClock.dbSettings = dbsettings.Value;
            QMinder.dbSettings = dbsettings.Value;
            StyleMappings.dbSettings = dbsettings.Value;
            StyleMappings.generalSettings = generalSettings.Value;
            EmailList.dbSettings = dbsettings.Value;
            OAuth2.authSettings = oauthSettings.Value;


        }

        public IActionResult Index()
        {
            return View();
        }
        public IActionResult Test()
        {

            return View("/Home/Quality");
        }



        public IActionResult FileLoader()
        {
            SalesData salesData = new SalesData();
            int numEmails = salesData.executeFileLoader();
            ViewBag.Message = "Processed " + numEmails + " emails";
            if (numEmails > 0)
            {
                ProductDetailsData pd = new ProductDetailsData();
                pd.loadDailyDashboard();
                ViewBag.Message += "; Loaded DailyDashboard";



            }

            return View("Done");
        }


        // for development only.  Calculate commissions for payroll
        public IActionResult Commissions(SalesData salesData)
        {
            Payroll payroll = new Payroll();
            payroll.calculateWeeklyCommission(false);
            ViewBag.Message = "Processed commissions";
            return View("Done");

        }
        public IActionResult ProcessDatascape(SalesData salesData)
        {
            Datascape datascape = new Datascape();
            datascape.loadEmails();
            datascape.reconcileAll();
            ViewBag.Message = "Processed Datascape";
            return View("Done");

        }
        // for development only.  Load Datascape emails
        public IActionResult DatascapeLoad(SalesData salesData)
        {
            Datascape datascape = new Datascape();
            datascape.loadEmails();
            ViewBag.Message = "Processed Datascape emails";
            return View("Done");

        }

        // for development only.  Reconcile Datascape 
        public IActionResult DatascapeReconciliation(SalesData salesData)
        {
            Datascape datascape = new Datascape();
            datascape.reconcileAll();
            ViewBag.Message = "Reconciled Datascape";
            return View("Done");

        }

        // for development only.  Reconcile Datascape 
        public IActionResult DailyDashboard()
        {
            ProductDetailsData pd = new ProductDetailsData();
            pd.loadDailyDashboard(true);
            ViewBag.Message = "Daily Dashboard Loaded";
            return View("Done");

        }

        public IActionResult LoadDailyNeedsData()
        {
            ProductDetailsData pd = new ProductDetailsData();
            pd.loadDailyNeedsData();
            ViewBag.Message = "Daily Needs Loaded";
            return View("Done");
        }

        public IActionResult processExternalClients()
        {
            SalesData salesData = new SalesData();
            int numClients = salesData.processExternalClientData();
            ViewBag.Message = "Processed " + numClients + " client emails";
            return View("Done");
        }

        

        public IActionResult processExternalClientsDatascapeReconcileOnly()
        {
            Client client = new Client();
            client.name = "Hershel Martin";
            client.schema = "martin";
            client.ownerID = 5;
            Datascape datascape = new Datascape();
            datascape.reconcileClient(client);
            return View("Done");
        }

        public async Task<IActionResult> testOAuthAsync()
        {

            OAuth2 oauth = new OAuth2("clients_dev@bogart-wireless.net");
            List<MimeMessage> messages = oauth.ReceiveEmail(1);
            return View("Done");
        }

    }
}