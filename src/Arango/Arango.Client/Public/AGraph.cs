using System;
using System.Collections.Generic;
using Arango.Client.Protocol;
using Arango.fastJSON;

namespace Arango.Client
{
    public class AGraph
    {
        readonly Dictionary<string, object> _parameters = new Dictionary<string, object>();
        readonly Connection _connection;
        
        internal AGraph(Connection connection)
        {
            _connection = connection;
        }
        
        #region Parameters
        
        /// <summary>
        /// Determines whether or not to wait until data are synchronised to disk. Default value: false.
        /// </summary>
        public AGraph WaitForSync(bool value)
        {
            _parameters.Bool(ParameterName.WaitForSync, value);
        	
        	return this;
        }

        /// <summary>
        /// Conditionally operate on document with specified revision.
        /// </summary>
        public AGraph IfMatch(string revision)
        {
            _parameters.String(ParameterName.IfMatch, revision);

            return this;
        }

        /// <summary>
        /// Determines whether to keep any attributes from existing document that are contained in the patch document which contains null value. Default value: true.
        /// </summary>
        public AGraph KeepNull(bool value)
        {
            // needs to be string value
            _parameters.String(ParameterName.KeepNull, value.ToString().ToLower());

            return this;
        }

        /// <summary>
        /// Determines maximum size of a journal or datafile in bytes. Default value: server configured.
        /// </summary>
        public AGraph JournalSize(long value)
        {
            _parameters.Long(ParameterName.JournalSize, value);
        	
        	return this;
        }
        
        /// <summary>
        /// Determines whether the collection will be compacted. Default value: true.
        /// </summary>
        public AGraph DoCompact(bool value)
        {
            _parameters.Bool(ParameterName.DoCompact, value);
        	
        	return this;
        }
        
        /// <summary>
        /// Determines whether the collection is a system collection. Default value: false.
        /// </summary>
        public AGraph IsSystem(bool value)
        {
            _parameters.Bool(ParameterName.IsSystem, value);
        	
        	return this;
        }
        
        /// <summary>
        /// Determines whether the collection data is kept in-memory only and not made persistent. Default value: false.
        /// </summary>
        public AGraph IsVolatile(bool value)
        {
            _parameters.Bool(ParameterName.IsVolatile, value);
        	
        	return this;
        }
        
        #endregion
        
        #region Create Graph (POST)
        
        /// <summary>
        /// Creates new graph in current database context.
        /// </summary>
        public AResult<Dictionary<string, object>> Create(string graphName)
        {
            var request = new Request(HttpMethod.POST, ApiBaseUri.Graph, "");
            var bodyDocument = new Dictionary<string, object>();
            
            // required
            bodyDocument.String(ParameterName.Name, graphName);
            
            request.Body = JSON.ToJSON(bodyDocument, ASettings.JsonParameters);
            
            var response = _connection.Send(request);
            var result = new AResult<Dictionary<string, object>>(response);
            
            switch (response.StatusCode)
            {
                case 200:
                    var body = response.ParseBody<Dictionary<string, object>>();
                    
                    result.Success = (body != null);
                    result.Value = body;
                    break;
                default:
                    // Arango error
                    break;
            }
            
            _parameters.Clear();
            
            return result;
        }
        
        #endregion
        
        #region Get graph (GET)
        
        /// <summary>
        /// Retrieves basic information about specified collection.
        /// </summary>
        public AResult<Dictionary<string, object>> Get(string graphName)
        {
            var request = new Request(HttpMethod.GET, ApiBaseUri.Graph, "/" + graphName);

            var response = _connection.Send(request);
            var result = new AResult<Dictionary<string, object>>(response);
            
            switch (response.StatusCode)
            {
                case 200:
                    var body = response.ParseBody<Dictionary<string, object>>();
                    
                    result.Success = (body != null);
                    result.Value = body;
                    break;
                case 404:
                default:
                    // Arango error
                    break;
            }
            
            _parameters.Clear();
            
            return result;
        }

        #endregion

        #region List Vertex collections (GET)

        /// <summary>
        /// Retrieves all vertex collections of a graph
        /// </summary>
        public AResult<Dictionary<string, object>> GetVertexCollection(string graphName)
        {
            var request = new Request(HttpMethod.GET, ApiBaseUri.Graph, "/" + graphName + "/vertex");

            var response = _connection.Send(request);
            var result = new AResult<Dictionary<string, object>>(response);

            switch (response.StatusCode)
            {
                case 200:
                    var body = response.ParseBody<Dictionary<string, object>>();

                    result.Success = (body != null);
                    result.Value = body;
                    break;
                case 404:
                default:
                    // Arango error
                    break;
            }

            _parameters.Clear();

            return result;
        }

        #endregion

        #region Add vertex collection (POST)

        /// <summary>
        /// Adds a vertex collection
        /// </summary>
        public AResult<Dictionary<string, object>> CreateVertexCollection(string graphName, string collectionName)
        {
            var request = new Request(HttpMethod.POST, ApiBaseUri.Graph, "/" + graphName + "/vertex");
            var bodyDocument = new Dictionary<string, object>();

            // required
            bodyDocument.String(ParameterName.Collection, collectionName);

            request.Body = JSON.ToJSON(bodyDocument, ASettings.JsonParameters);

            var response = _connection.Send(request);
            var result = new AResult<Dictionary<string, object>>(response);

            switch (response.StatusCode)
            {
                case 200:
                    var body = response.ParseBody<Dictionary<string, object>>();

                    result.Success = (body != null);
                    result.Value = body;
                    break;
                default:
                    // Arango error
                    break;
            }

            _parameters.Clear();

            return result;
        }

        #endregion

        #region Remove Vertex Collection (DELETE)

        /// <summary>
        /// Removes specified vertex collection.
        /// </summary>
        public AResult<Dictionary<string, object>> DeleteVertexCollection(string graphName, string collectionName)
        {
            var request = new Request(HttpMethod.DELETE, ApiBaseUri.Graph, "/" + graphName + "/vertex/" + collectionName);

            var response = _connection.Send(request);
            var result = new AResult<Dictionary<string, object>>(response);

            switch (response.StatusCode)
            {
                case 200:
                    var body = response.ParseBody<Dictionary<string, object>>();

                    result.Success = (body != null);
                    result.Value = body;
                    break;
                case 400:
                case 404:
                default:
                    // Arango error
                    break;
            }

            _parameters.Clear();

            return result;
        }

        #endregion

        #region Add edge definition (POST)

        /// <summary>
        /// Retrieves basic information about specified collection.
        /// </summary>
        public AResult<Dictionary<string, object>> CreateEdgeDefinition(string graphName, string definitionName, List<string> fromCollections, List<string> toCollections)
        {
            var request = new Request(HttpMethod.POST, ApiBaseUri.Graph, "/" + graphName + "/edge");
            var bodyDocument = new Dictionary<string, object>();

            // required
            bodyDocument.String(ParameterName.Collection, definitionName);

            bodyDocument.List<string>(ParameterName.From, fromCollections);

            bodyDocument.List<string>(ParameterName.To, toCollections);

            request.Body = JSON.ToJSON(bodyDocument, ASettings.JsonParameters);

            var response = _connection.Send(request);
            var result = new AResult<Dictionary<string, object>>(response);

            switch (response.StatusCode)
            {
                case 200:
                    var body = response.ParseBody<Dictionary<string, object>>();

                    result.Success = (body != null);
                    result.Value = body;
                    break;
                default:
                    // Arango error
                    break;
            }

            _parameters.Clear();

            return result;
        }

        #endregion

        #region Remove Edge Definition (DELETE)

        /// <summary>
        /// Deletes specified edge definition.
        /// </summary>
        public AResult<Dictionary<string, object>> DeleteEdgeDefinition(string graphName, string definitionName)
        {
            var request = new Request(HttpMethod.DELETE, ApiBaseUri.Graph, "/" + graphName + "/edge/" + definitionName);

            var response = _connection.Send(request);
            var result = new AResult<Dictionary<string, object>>(response);

            switch (response.StatusCode)
            {
                case 200:
                    var body = response.ParseBody<Dictionary<string, object>>();

                    result.Success = (body != null);
                    result.Value = body;
                    break;
                case 400:
                case 404:
                default:
                    // Arango error
                    break;
            }

            _parameters.Clear();

            return result;
        }

        #endregion

        #region Create Vertex (POST)

        /// <summary>
        /// Creates new vertex within specified collection in current database context.
        /// </summary>
        public AResult<Dictionary<string, object>> CreateVertex<T>(string graphName, string collectionName, T obj)
        {
            //return Create(collectionName, JSON.ToJSON(DictionaryExtensions.StripObject(obj), ASettings.JsonParameters));
            return CreateVertex(graphName, collectionName, Dictator.ToDocument(obj));
        }

        /// <summary>
        /// Creates new vertex within specified collection in current database context.
        /// </summary>
        public AResult<Dictionary<string, object>> CreateVertex(string graphName, string collectionName, Dictionary<string, object> vertex)
        {
            return CreateVertex(graphName, collectionName, JSON.ToJSON(vertex, ASettings.JsonParameters));
        }

        /// <summary>
        /// Creates new vertex within specified collection in current database context.
        /// </summary>
        public AResult<Dictionary<string, object>> CreateVertex(string graphName,string collectionName, string json)
        {
            var request = new Request(HttpMethod.POST, ApiBaseUri.Graph, "/" + graphName + "/vertex/" + collectionName);

            // optional
            request.TrySetQueryStringParameter(ParameterName.WaitForSync, _parameters);

            request.Body = json;

            var response = _connection.Send(request);
            var result = new AResult<Dictionary<string, object>>(response);

            switch (response.StatusCode)
            {
                case 201:
                case 202:
                    var body = response.ParseBody<Dictionary<string, object>>();

                    result.Success = (body != null);
                    result.Value = body;
                    break;
                case 400:
                case 404:
                default:
                    // Arango error
                    break;
            }

            _parameters.Clear();

            return result;
        }

        #endregion

        #region Get Vertex (GET)

        /// <summary>
        /// Retrieves specified vertex.
        /// </summary>
        /// <exception cref="ArgumentException">Specified 'id' value has invalid format.</exception>
        public AResult<T> GetVertex<T>(string graphName, string collectionName, string id)
        {
            var request = new Request(HttpMethod.GET, ApiBaseUri.Graph, "/" + graphName +"/vertex/" + collectionName + "/" + id);

            // optional
            request.TrySetHeaderParameter(ParameterName.IfMatch, _parameters);

            var response = _connection.Send(request);
            var result = new AResult<T>(response);

            switch (response.StatusCode)
            {
                case 200:
                    var body = response.ParseBody<T>();

                    result.Success = (body != null);
                    result.Value = body;
                    break;
                case 412:
                    body = response.ParseBody<T>();

                    result.Value = body;
                    break;
                case 304:
                case 404:
                default:
                    // Arango error
                    break;
            }

            _parameters.Clear();

            return result;
        }

        #endregion

        #region Update vertex (PATCH)

        /// <summary>
        /// Updates existing vertex identified by its handle with new vertex data.
        /// </summary>
        /// <exception cref="ArgumentException">Specified 'id' value has invalid format.</exception>
        public AResult<Dictionary<string, object>> UpdateVertex(string graphName, string collectionName, string id, string json)
        {
            var request = new Request(HttpMethod.PATCH, ApiBaseUri.Graph, "/" + graphName + "/vertex/" + collectionName + "/" + id);

            // optional
            request.TrySetQueryStringParameter(ParameterName.WaitForSync, _parameters);
            // optional
            request.TrySetQueryStringParameter(ParameterName.KeepNull, _parameters);
            // optional
            request.TrySetHeaderParameter(ParameterName.IfMatch, _parameters);

            request.Body = json;

            var response = _connection.Send(request);
            var result = new AResult<Dictionary<string, object>>(response);

            switch (response.StatusCode)
            {
                case 201:
                case 202:
                    var body = response.ParseBody<Dictionary<string, object>>();

                    result.Success = (body != null);
                    result.Value = body;
                    break;
                case 412:
                    body = response.ParseBody<Dictionary<string, object>>();

                    result.Value = body;
                    break;
                case 400:
                case 404:
                default:
                    // Arango error
                    break;
            }

            _parameters.Clear();

            return result;
        }

        /// <summary>
        /// Updates existing vertex identified by its handle with new vertex data.
        /// </summary>
        public AResult<Dictionary<string, object>> UpdateVertex(string graphName, string collectionName, string id, Dictionary<string, object> vertex)
        {
            return UpdateVertex(graphName, collectionName, id, JSON.ToJSON(vertex, ASettings.JsonParameters));
        }

        /// <summary>
        /// Updates existing vertex identified by its handle with new vertex data.
        /// </summary>
        public AResult<Dictionary<string, object>> UpdateVertex<T>(string graphName, string collectionName, string id, T obj)
        {
            return UpdateVertex(graphName, collectionName, id, Dictator.ToDocument(obj));
        }

        #endregion

        #region Replace (PUT)

        /// <summary>
        /// Completely replaces existing vertex identified by its handle with new vertex data.
        /// </summary>
        /// <exception cref="ArgumentException">Specified 'id' value has invalid format.</exception>
        public AResult<Dictionary<string, object>> ReplaceVertex(string graphName, string collectionName, string id, string json)
        {
            var request = new Request(HttpMethod.PUT, ApiBaseUri.Graph, "/" + graphName + "/vertex/" + collectionName + "/" + id);

            // optional
            request.TrySetQueryStringParameter(ParameterName.WaitForSync, _parameters);
            // optional
            request.TrySetHeaderParameter(ParameterName.IfMatch, _parameters);

            request.Body = json;

            var response = _connection.Send(request);
            var result = new AResult<Dictionary<string, object>>(response);

            switch (response.StatusCode)
            {
                case 201:
                case 202:
                    var body = response.ParseBody<Dictionary<string, object>>();

                    result.Success = (body != null);
                    result.Value = body;
                    break;
                case 412:
                    body = response.ParseBody<Dictionary<string, object>>();

                    result.Value = body;
                    break;
                case 400:
                case 404:
                default:
                    // Arango error
                    break;
            }

            _parameters.Clear();

            return result;
        }

        /// <summary>
        /// Completely replaces existing vertex identified by its handle with new vertex data.
        /// </summary>
        /// <exception cref="ArgumentException">Specified id value has invalid format.</exception>
        public AResult<Dictionary<string, object>> ReplaceVertex(string graphName, string collectionName, string id, Dictionary<string, object> vertex)
        {
            return ReplaceVertex(graphName, collectionName, id, JSON.ToJSON(vertex, ASettings.JsonParameters));
        }

        /// <summary>
        /// Completely replaces existing vertex identified by its handle with new vertex data.
        /// </summary>
        /// <exception cref="ArgumentException">Specified id value has invalid format.</exception>
        public AResult<Dictionary<string, object>> ReplaceVertex<T>(string graphName, string collectionName, string id, T obj)
        {
            return ReplaceVertex(graphName, collectionName, id, Dictator.ToDocument(obj));
        }

        #endregion

        #region Delete Vertex (DELETE)

        /// <summary>
        /// Deletes specified vertex.
        /// </summary>
        /// <exception cref="ArgumentException">Specified 'id' value has invalid format.</exception>
        public AResult<Dictionary<string, object>> DeleteVertex(string graphName, string collectionName, string id)
        {
            var request = new Request(HttpMethod.DELETE, ApiBaseUri.Graph, "/" + graphName + "/vertex/" + collectionName + "/" + id);

            // optional
            request.TrySetQueryStringParameter(ParameterName.WaitForSync, _parameters);
            // optional
            request.TrySetHeaderParameter(ParameterName.IfMatch, _parameters);

            var response = _connection.Send(request);
            var result = new AResult<Dictionary<string, object>>(response);

            switch (response.StatusCode)
            {
                case 200:
                case 202:
                    var body = response.ParseBody<Dictionary<string, object>>();

                    result.Success = (body != null);
                    result.Value = body;
                    break;
                case 412:
                    body = response.ParseBody<Dictionary<string, object>>();

                    result.Value = body;
                    break;
                case 404:
                default:
                    // Arango error
                    break;
            }

            _parameters.Clear();

            return result;
        }

        #endregion

        #region Delete graph (DELETE)

        /// <summary>
        /// Deletes specified collection.
        /// </summary>
        public AResult<Dictionary<string, object>> Delete(string graphName)
        {
            var request = new Request(HttpMethod.DELETE, ApiBaseUri.Graph, "/" + graphName);
            
            var response = _connection.Send(request);
            var result = new AResult<Dictionary<string, object>>(response);
            
            switch (response.StatusCode)
            {
                case 200:
                    var body = response.ParseBody<Dictionary<string, object>>();
                    
                    result.Success = (body != null);
                    result.Value = body;
                    break;
                case 400:
                case 404:
                default:
                    // Arango error
                    break;
            }
            
            _parameters.Clear();
            
            return result;
        }
        
        #endregion
    }
}
