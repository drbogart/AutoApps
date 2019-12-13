using System;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Options;
using bogart_wireless.Models;
using AutoApps.Models;
using bogart_wireless.Libraries;



namespace bogart_wireless.Controllers
{

    public class AutoController : Controller
    {
        readonly IEmailConfiguration _configuration; // for email

        public AutoController(IOptionsSnapshot<EmailConfiguration> configuration, IOptions<DatabaseConnectionSettings> dbsettings, IOptions<GeneralSettings> generalSettings)
        {
            _configuration = configuration.Get("ReportFiles");
            Datascape.emailConfiguration = configuration.Get("Datascape");
            Datascape.dbSettings = dbsettings.Value;
            Datascape.generalSettings = generalSettings.Value;
            SalesData.emailConfiguration = configuration.Get("ReportFiles");
            SalesData.dbSettings = dbsettings.Value;
            Payroll.generalSettings = generalSettings.Value;
            ProductDetailsData.dbSettings = dbsettings.Value;
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
            if (numEmails > 0)
            {
                ProductDetailsData pd = new ProductDetailsData();
                pd.loadDailyDashboard();
            }
            ViewBag.Message = "Processed " + numEmails + " emails; Loaded DailyDashboard";
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
    }
   
}