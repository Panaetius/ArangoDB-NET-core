﻿using System;
using System.IO;
using System.Net;
using System.Text;

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

        internal Connection(string alias, string hostname, int port, bool isSecured, string databaseName, string userName, string password)
        {
            Alias = alias;
            Hostname = hostname;
            Port = port;
            IsSecured = isSecured;
            DatabaseName = databaseName;
            Username = userName;
            Password = password;

            BaseUri = new Uri((isSecured ? "https" : "http") + "://" + hostname + ":" + port + "/_db/" + databaseName + "/");
        }

        internal Response Send(Request request)
        {
            var httpRequest = HttpWebRequest.CreateHttp(BaseUri + request.GetRelativeUri());

            if (request.Headers.Count > 0)
            {
                httpRequest.Headers = request.Headers;
            }
            
            httpRequest.Proxy = null;
            httpRequest.Method = request.HttpMethod.ToString();

            if (!string.IsNullOrEmpty(Username) && !string.IsNullOrEmpty(Password))
            {
                httpRequest.Headers.Add(
                    "Authorization", 
                    "Basic " + Convert.ToBase64String(Encoding.ASCII.GetBytes(Username + ":" + Password))
                );
            }

            if (!string.IsNullOrEmpty(request.Body))
            {
                httpRequest.ContentType = "application/json; charset=utf-8";

                var data = Encoding.UTF8.GetBytes(request.Body);

                var task = httpRequest.GetRequestStreamAsync();
                task.Wait();

                using (var stream = task.Result)
                {
                    stream.Write(data, 0, data.Length);
                    stream.Flush();
                    stream.Dispose();
                }
            }

            var response = new Response();

            try
            {
                var responseTask = httpRequest.GetResponseAsync();
                responseTask.Wait();

                using (var httpResponse = (HttpWebResponse)responseTask.Result)
                using (var responseStream = httpResponse.GetResponseStream())
                using (var reader = new StreamReader(responseStream))
                {
                    response.StatusCode = (int)httpResponse.StatusCode;
                    response.Headers = httpResponse.Headers;
                    response.Body = reader.ReadToEnd();

                    reader.Dispose();
                    responseStream.Dispose();
                }

                response.GetBodyDataType();
            }
            catch (WebException webException)
            {
                if ((webException.Status == WebExceptionStatus.ProtocolError) && 
                    (webException.Response != null))
                {
                    using (var exceptionHttpResponse = (HttpWebResponse)webException.Response)
                    {
                        response.StatusCode = (int)exceptionHttpResponse.StatusCode;

                        if (exceptionHttpResponse.Headers.Count > 0)
                        {
                            response.Headers = exceptionHttpResponse.Headers;
                        }

                        if (exceptionHttpResponse.ContentLength > 0)
                        {
                            using (var exceptionResponseStream = exceptionHttpResponse.GetResponseStream())
                            using (var exceptionReader = new StreamReader(exceptionResponseStream))
                            {
                                response.Body = exceptionReader.ReadToEnd();

                                exceptionReader.Dispose();
                                exceptionResponseStream.Dispose();
                            }
                            
                            response.GetBodyDataType();
                        }
                    }

                    response.Error = new AEerror();
                    response.Error.Exception = webException;

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
                        response.Error.Message = "Protocol error: " + webException.Message;
                    }
                }
                else
                {
                    throw;
                }
            }

            return response;
        }
    }
}
