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

        public AutoController(IOptionsSnapshot<EmailConfiguration> configuration, IOptions<DatabaseConnectionSettings> dbsettings, IOptions<GeneralSettings> generalSettings)
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
            Datascape datascape = new Datascape();
            datascape.reconcileClient(client);
            return View("Done");
        }

        public async Task<IActionResult> testOAuthAsync()
        {

            /*
            


            var credential = GoogleCredential.FromFile(PathToServiceAccountKeyFile)
                .CreateScoped("https://mail.google.com/").CreateWithUser("dave.bogart@bogart-wireless.net"); ;

            // Create Gmail API service.
            var service = new GmailService(new BaseClientService.Initializer()
            {
                HttpClientInitializer = credential,
                ApplicationName = "Autoapp-dev",
            });

            // Define parameters of request.
            UsersResource.LabelsResource.ListRequest request = service.Users.Labels.List("clients_dev@bogart-wireless.net");
            */

            string PathToServiceAccountKeyFile = @"C:\Users\dave\Downloads\autoapp-344816-d424feab9c5f.json";


            var scopes = new[] { "https://mail.google.com/" };

            ServiceAccountCredential credential;
            using (Stream stream = new FileStream(PathToServiceAccountKeyFile, FileMode.Open, FileAccess.Read, FileShare.Read))
            {
                credential =
                    GoogleCredential.FromFile(PathToServiceAccountKeyFile).CreateScoped("https://mail.google.com/").CreateWithUser("dave.bogart@bogart-wireless.net").UnderlyingCredential as
                        ServiceAccountCredential;
            }

            bool result = await credential.RequestAccessTokenAsync(CancellationToken.None);
            SaslMechanism oauth2 = new SaslMechanismOAuth2("dave.bogart@bogart-wireless.net", credential.Token.AccessToken);

            /*
            FileStream fs = new FileStream(PathToServiceAccountKeyFile, FileMode.Open, FileAccess.Read, FileShare.None);
            ServiceAccountCredential credential = new ServiceAccountCredential(new ServiceAccountCredential.Initializer("dave.bogart@bogart-wireless.net")
            {
                Scopes = new[] { "https://mail.google.com/" }
            }.F(fs));
       

            var secrets = new ClientSecrets
            {
                ClientId = "661323586463-7f88kadp590ejhurm8530kv5pp05j6bg.apps.googleusercontent.com",
                ClientSecret = "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"
            };

            var codeFlow = new GoogleAuthorizationCodeFlow(new GoogleAuthorizationCodeFlow.Initializer
            {
                DataStore = new FileDataStore("CredentialCacheFolder", false),
                Scopes = new[] { "https://mail.google.com/" },
                ClientSecrets = secrets
            });
            var codeReceiver = new LocalServerCodeReceiver();
            var authCode = new AuthorizationCodeInstalledApp(codeFlow, codeReceiver);

            var xcredential = await authCode.AuthorizeAsync("dave.bogart@bogart-wireless.net", CancellationToken.None);

            if (xcredential.Token.IsExpired(SystemClock.Default))
                await xcredential.RefreshTokenAsync(CancellationToken.None);

            oauth2 = new SaslMechanismOAuth2(xcredential.UserId, xcredential.Token.AccessToken);
            */

            using (var client = new SmtpClient())
            {
                await client.ConnectAsync("smtp.gmail.com", 587, SecureSocketOptions.StartTls);
                await client.AuthenticateAsync(oauth2);
                var email = new MimeMessage();
                email.From.Add(MailboxAddress.Parse("from_address@example.com"));
                email.To.Add(MailboxAddress.Parse("dave.bogart@wireless-zone.com"));
                email.Subject = "Test Email Subject";
                email.Body = new TextPart(TextFormat.Html) { Text = "<h1>Example HTML Message Body</h1>" };

                client.Send(email);
                await client.DisconnectAsync(true);
               
            }

            /*
            var googleCredentials = await GoogleWebAuthorizationBroker.AuthorizeAsync(secrets, new[] { GmailService.Scope.MailGoogleCom }, "clients_dev@bogart-wireless.net", CancellationToken.None);
            if (googleCredentials.Token.IsExpired(SystemClock.Default))
            {
                await googleCredentials.RefreshTokenAsync(CancellationToken.None);
            }

            using (var client = new SmtpClient())
            {
                client.Connect("smtp.gmail.com", 587, SecureSocketOptions.StartTls);

                var xoauth2 = new SaslMechanismOAuth2(googleCredentials.UserId, googleCredentials.Token.AccessToken);
                client.Authenticate(oauth2);
                var emailMessage = new MimeMessage();

                emailMessage.From.Add(new MailboxAddress("Dave", "Clients_dev@bogart-bogart_wireless.net"));
                emailMessage.To.Add(new MailboxAddress("Dave", "dave.bogart@wireless-zone.com"));
                emailMessage.Subject = "Test";
                emailMessage.Body = new TextPart("html") { Text = "This is a test" };
                await client.SendAsync(emailMessage);
                client.Disconnect(true);
            }

            Message msg = new Message();

            service.Users.Messages.Send(msg, "clients_dev");
            IList<Label> labels = request.Execute().Labels;
            */

            return View("Done");
        }

    }
}