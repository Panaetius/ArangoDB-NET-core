using System;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading.Tasks;

namespace Arango.Client.Protocol
{
    /// <summary>
    /// Stores data about single endpoint and processes communication between client and server.
    /// </summary>
    internal class Connection
    {
        #region Properties

        internal string Alias { get; set; }

        internal string Hostname { get; set; }

        internal int Port { get; set; }

        internal bool IsSecured { get; set; }

        internal string DatabaseName { get; set; }

        internal string Username { get; set; }

        internal string Password { get; set; }

        internal Uri BaseUri { get; set; }

        #endregion

        internal Connection(string alias, string hostname, int port, bool isSecured, string username, string password)
        {
            Alias = alias;
            Hostname = hostname;
            Port = port;
            IsSecured = isSecured;
            Username = username;
            Password = password;

            BaseUri = new Uri((isSecured ? "https" : "http") + "://" + hostname + ":" + port + "/");
        }

        internal Connection(
            string alias,
            string hostname,
            int port,
            bool isSecured,
            string databaseName,
            string userName,
            string password)
        {
            Alias = alias;
            Hostname = hostname;
            Port = port;
            IsSecured = isSecured;
            DatabaseName = databaseName;
            Username = userName;
            Password = password;

            BaseUri =
                new Uri((isSecured ? "https" : "http") + "://" + hostname + ":" + port + "/_db/" + databaseName + "/");
        }

        internal Response Send(Request request)
        {
            using (var client = new HttpClient())
            {
                // New code:
                client.BaseAddress = BaseUri;
                client.DefaultRequestHeaders.Accept.Clear();
                client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));

                foreach (var header in request.Headers)
                {
                    client.DefaultRequestHeaders.Add(header.Key, header.Value);
                }

                if (!string.IsNullOrEmpty(Username) && !string.IsNullOrEmpty(Password))
                {
                    client.DefaultRequestHeaders.Add(
                        "Authorization",
                        "Basic " + Convert.ToBase64String(Encoding.ASCII.GetBytes(Username + ":" + Password)));
                }

                var message = new HttpRequestMessage(
                                  new System.Net.Http.HttpMethod(request.HttpMethod.ToString()),
                                  request.GetRelativeUri());

                if (!string.IsNullOrEmpty(request.Body))
                {
                    message.Content = new StringContent(request.Body, Encoding.UTF8, "application/json");
                }

                var response = new Response();


                var responseTask = client.SendAsync(message);
                responseTask.Wait();

                if (responseTask.Result.IsSuccessStatusCode)
                {
                    response.StatusCode = (int)responseTask.Result.StatusCode;
                    response.Headers = responseTask.Result.Headers.ToDictionary(h => h.Key, h => h.Value);

                    var bodyTask = responseTask.Result.Content.ReadAsStringAsync();
                    bodyTask.Wait();

                    response.Body = bodyTask.Result;

                    response.GetBodyDataType();
                }
                else
                {
                    response.StatusCode = (int)responseTask.Result.StatusCode;

                    if (responseTask.Result.Headers.ToList().Count > 0)
                    {
                        response.Headers = responseTask.Result.Headers.ToDictionary(h => h.Key, h => h.Value);
                    }

                    var bodyTask = responseTask.Result.Content.ReadAsStringAsync();
                    bodyTask.Wait();

                    if (!string.IsNullOrEmpty(bodyTask.Result))
                    {
                        response.Body = bodyTask.Result;

                        response.GetBodyDataType();
                    }

                    response.Error = new AEerror();
                    response.Error.Exception = responseTask.Exception;

                    if (response.BodyType == BodyType.Document)
                    {
                        var body = response.ParseBody<Body<object>>();

                        if ((body != null) && body.Error)
                        {
                            response.Error.StatusCode = body.Code;
                            response.Error.Number = body.ErrorNum;
                            response.Error.Message = "ArangoDB error: " + body.ErrorMessage;
                        }
                    }

                    if (string.IsNullOrEmpty(response.Error.Message))
                    {
                        response.Error.StatusCode = response.StatusCode;
                        response.Error.Number = 0;
                        response.Error.Message = "Protocol error: " + responseTask.Exception;
                    }
                }

                return response;
            }
        }
    }
}
