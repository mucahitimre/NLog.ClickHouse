using ClickHouse.Client.ADO;
using ClickHouse.Client.Copy;
using Dapper;
using NLog.Common;
using NLog.Config;
using NLog.Layouts;
using NLog.Targets;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Threading.Tasks;

namespace NLog.ClickHouse
{
    /// <summary>
    /// NLog message target for ClickHouseDB.
    /// </summary>
    [Target("ClickHouse")]
    public class ClickHouseTarget : Target
    {
        private static readonly LogEventInfo DefaultLogEvent = LogEventInfo.CreateNullEvent();

        /// <summary>
        /// Initializes a new instance of the <see cref="ClickHouseTarget"/> class.
        /// </summary>
        public ClickHouseTarget()
        {
            Fields = new List<ClickHouseField>();
            Properties = new List<ClickHouseField>();
            IncludeDefaults = true;
            OptimizeBufferReuse = true;
            IncludeEventProperties = true;
        }

        /// <summary>
        /// Gets the fields collection.
        /// </summary>
        /// <value>
        /// The fields.
        /// </value>
        [ArrayParameter(typeof(ClickHouseField), "field")]
        public IList<ClickHouseField> Fields { get; private set; }

        /// <summary>
        /// Gets the properties collection.
        /// </summary>
        /// <value>
        /// The properties.
        /// </value>
        [ArrayParameter(typeof(ClickHouseField), "property")]
        public IList<ClickHouseField> Properties { get; private set; }

        /// <summary>
        /// Gets or sets the connection string name string.
        /// </summary>
        /// <value>
        /// The connection name string.
        /// </value>
        public string ConnectionString
        {
            get => (_connectionString as SimpleLayout)?.Text;
            set => _connectionString = value ?? string.Empty;
        }
        private Layout _connectionString;

        /// <summary>
        /// Gets or sets a value indicating whether to use the default document format.
        /// </summary>
        /// <value>
        ///   <c>true</c> to use the default document format; otherwise, <c>false</c>.
        /// </value>
        public bool IncludeDefaults { get; set; }

        /// <summary>
        /// Gets or sets the name of the collection.
        /// </summary>
        /// <value>
        /// The name of the collection.
        /// </value>
        public string TableName
        {
            get => (_tableName as SimpleLayout)?.Text;
            set => _tableName = value ?? string.Empty;
        }
        private Layout _tableName;

        /// <summary>
        /// Gets or sets a value indicating whether [include event properties].
        /// </summary>
        /// <value>
        ///   <c>true</c> if [include event properties]; otherwise, <c>false</c>.
        /// </value>
        public bool IncludeEventProperties { get; set; }

        /// <summary>
        /// Gets or sets the cluster.
        /// </summary>
        /// <value>
        /// The cluster.
        /// </value>
        public string Cluster { get; set; }

        /// <summary>
        /// Initializes the target before writing starts
        /// </summary>
        /// <exception cref="NLog.NLogConfigurationException">
        /// Can not resolve ClickHouse ConnectionString. Please make sure the ConnectionString property is set.
        /// or
        /// 'Database' field not found in clickHouse connection string.
        /// or
        /// '{nameof(TableName)}' field not found in clickHouse connection string.
        /// </exception>
        protected override void InitializeTarget()
        {
            base.InitializeTarget();

            var connectionString = _connectionString?.Render(DefaultLogEvent);
            if (string.IsNullOrEmpty(connectionString))
                throw new NLogConfigurationException("Can not resolve ClickHouse ConnectionString. Please make sure the ConnectionString property is set.");

            var splitedConnectionString = connectionString.Split(';')
                .Where(w => !string.IsNullOrEmpty(w))
                .ToDictionary(w => w.Split('=')[0], w => w.Split('=')[1]);
            if (!splitedConnectionString.ContainsKey("Database"))
            {
                throw new NLogConfigurationException("'Database' field not found in clickHouse connection string.");
            }

            if (string.IsNullOrEmpty(TableName))
            {
                throw new NLogConfigurationException($"'{nameof(TableName)}' field not found in clickHouse connection string.");
            }

            var columns = GetColumnsData(Fields);
            var database = splitedConnectionString["Database"];
            var query = CreateTableQuery(database, TableName, Cluster, columns);
            using (var session = new ClickHouseConnection(connectionString))
            {
                session.Execute(query);
            }
        }

        private Func<AsyncLogEventInfo, Dictionary<string, object>> _createDocumentDelegate;

        /// <summary>
        /// Writes an array of logging events to the log target. By default it iterates on all
        /// events and passes them to "Write" method. Inheriting classes can use this method to
        /// optimize batch writes.
        /// </summary>
        /// <param name="logEvents">Logging events to be written out.</param>
        protected override void Write(IList<AsyncLogEventInfo> logEvents)
        {
            if (logEvents.Count == 0)
                return;

            List<object[]> values = null;
            try
            {
                if (_createDocumentDelegate == null)
                    _createDocumentDelegate = e => CreateDocument(e.LogEvent);

                var documents = logEvents.Select(_createDocumentDelegate);

                using (var connection = new ClickHouseConnection(_connectionString?.Render(DefaultLogEvent)))
                using (var bulkCopyInterface = new ClickHouseBulkCopy(connection)
                {
                    DestinationTableName = TableName,
                    BatchSize = 100000,
                    MaxDegreeOfParallelism = 15
                })
                {
                    var columns = documents.First().Keys.ToList();
                    values = documents.Select(w => w.Values.ToArray()).ToList();

                    ExecuteBulk(bulkCopyInterface, values, columns).Wait();
                }

                for (int i = 0; i < logEvents.Count; ++i)
                    logEvents[i].Continuation(null);
            }
            catch (Exception ex)
            {
                InternalLogger.Error("Error when writing to ClickHouse {0}", ex);

                if (ex.MustBeRethrownImmediately())
                    throw;

                for (int i = 0; i < logEvents.Count; ++i)
                    logEvents[i].Continuation(ex);

                if (ex.MustBeRethrown())
                    throw;
            }
        }

        /// <summary>
        /// Writes logging event to the log target.
        /// classes.
        /// </summary>
        /// <param name="logEvent">Logging event to be written out.</param>
        protected override void Write(LogEventInfo logEvent)
        {
            try
            {
                var document = CreateDocument(logEvent);

                using (var connection = new ClickHouseConnection(_connectionString?.Render(DefaultLogEvent)))
                using (var bulkCopyInterface = new ClickHouseBulkCopy(connection)
                {
                    DestinationTableName = TableName,
                    BatchSize = 100000
                })
                {
                    var columns = document.Keys.ToList();
                    var values = document.Select(w => new object[] { w.Value }).ToList();

                    ExecuteBulk(bulkCopyInterface, values, columns).Wait();
                }
            }
            catch (Exception ex)
            {
                InternalLogger.Error("Error when writing to ClickHouse {0}", ex);
                throw;
            }
        }

        private static async Task ExecuteBulk(ClickHouseBulkCopy session, List<object[]> data, List<string> columns)
        {
            await session.WriteToServerAsync(data, columns)
                .ConfigureAwait(false);
        }

        private Dictionary<string, object> CreateDocument(LogEventInfo logEvent)
        {
            var document = new Dictionary<string, object>();
            if (IncludeDefaults || Fields.Count == 0)
                AddDefaults(document, logEvent);

            // extra fields
            for (int i = 0; i < Fields.Count; ++i)
            {
                var value = GetValue(Fields[i], logEvent);
                if (value != null)
                {
                    if (document.ContainsKey(Fields[i].Name))
                    {
                        document[Fields[i].Name] = value;
                    }
                    else
                    {
                        document.Add(Fields[i].Name, value);
                    }
                }
            }

            AddProperties(document, logEvent);

            return document;
        }

        private void AddDefaults(Dictionary<string, object> document, LogEventInfo logEvent)
        {
            document.Add("Date", logEvent.TimeStamp);

            if (logEvent.Level != null)
                document.Add("Level", logEvent.Level.Name);

            if (logEvent.LoggerName != null)
                document.Add("Logger", logEvent.LoggerName);

            if (logEvent.FormattedMessage != null)
                document.Add("Message", logEvent.FormattedMessage);

            if (logEvent.Exception != null)
                document.Add("Exception", CreateException(logEvent.Exception));
            else
                document.Add("Exception", null);
        }

        private void AddProperties(Dictionary<string, object> document, LogEventInfo logEvent)
        {
            if ((IncludeEventProperties && logEvent.HasProperties) || Properties.Count > 0)
            {
                var propertiesDocument = new Dictionary<string, object>();
                for (int i = 0; i < Properties.Count; ++i)
                {
                    string key = Properties[i].Name;
                    var value = GetValue(Properties[i], logEvent);

                    if (value != null)
                        propertiesDocument[key] = value;
                }

                if (IncludeEventProperties && logEvent.HasProperties)
                {
                    foreach (var property in logEvent.Properties)
                    {
                        if (property.Key == null || property.Value == null)
                            continue;

                        string key = Convert.ToString(property.Key, CultureInfo.InvariantCulture);
                        if (string.IsNullOrEmpty(key))
                            continue;

                        string value = Convert.ToString(property.Value, CultureInfo.InvariantCulture);
                        if (string.IsNullOrEmpty(value))
                            continue;

                        if (key.IndexOf('.') >= 0)
                            key = key.Replace('.', '_');

                        if (propertiesDocument.ContainsKey(key))
                        {
                            propertiesDocument[key] = value;
                        }
                        else
                        {
                            propertiesDocument.Add(key, value);
                        }
                    }
                }

                if (propertiesDocument.Count > 0)
                    document.Add("Properties", propertiesDocument);
            }
        }

        private Dictionary<string, object> CreateException(Exception exception)
        {
            if (exception == null)
                return new Dictionary<string, object>();

            if (exception is AggregateException aggregateException)
            {
                aggregateException = aggregateException.Flatten();
                if (aggregateException.InnerExceptions?.Count == 1)
                {
                    exception = aggregateException.InnerExceptions[0];
                }
                else
                {
                    exception = aggregateException;
                }
            }

            var document = new Dictionary<string, object>
            {
                { "Message", exception.Message },
                { "BaseMessage", exception.GetBaseException().Message },
                { "Text", exception.ToString() },
                { "Type", exception.GetType().ToString() }
            };

#if !NETSTANDARD1_5
            if (exception is ExternalException external)
                document.Add("ErrorCode", external.ErrorCode);
#endif
            document.Add("HResult", exception.HResult);
            document.Add("Source", exception.Source ?? string.Empty);

#if !NETSTANDARD1_5
            var method = exception.TargetSite;
            if (method != null)
            {
                document.Add("MethodName", method.Name ?? string.Empty);

                AssemblyName assembly = method.Module?.Assembly?.GetName();
                if (assembly != null)
                {
                    document.Add("ModuleName", assembly.Name);
                    document.Add("ModuleVersion", assembly.Version.ToString());
                }
            }
#endif

            return document;
        }

        private object GetValue(ClickHouseField field, LogEventInfo logEvent)
        {
            var value = (field.Layout != null ? RenderLogEvent(field.Layout, logEvent) : string.Empty).Trim();
            if (string.IsNullOrEmpty(value))
                return null;

            object result;
            // int - bool - Datetime - UUID - string 
            switch (field.CHColumnType)
            {
                case "Datetime":
                    DateTime.TryParse(value, out var datetimeResult);
                    result = datetimeResult;
                    break;
                case "bool":
                    bool.TryParse(value, out var boolResult);
                    result = boolResult;
                    break;
                case "int":
                    int.TryParse(value, out var intResult);
                    result = intResult;
                    break;
                case "UUID":
                    Guid.TryParse(value, out var guidResult);
                    result = guidResult.ToString();
                    break;
                default:
                    result = value;
                    break;
            }

            return result;
        }

        private InternalPropertyModel GetColumnsData(IList<ClickHouseField> properties)
        {
            var propData = new InternalPropertyModel
            {
                SqlProperties = new List<string>(),
                PrimaryKeys = new List<string>()
            };

            propData.PrimaryKeys.Add("Id");

            foreach (var prop in properties)
            {
                // int - bool - DateTime - UUID - string 
                propData.SqlProperties.Add($"{prop.Name} {prop?.CHColumnType}");
            }

            return propData;
        }

        private string CreateTableQuery(string database, string tableName, string cluster, InternalPropertyModel model)
        {
            var primaryKeys = model.PrimaryKeys;
            var props = model.SqlProperties;
            var isPrimaryKey = primaryKeys == null || !primaryKeys.Any();
            var primaryKey = isPrimaryKey
                ? string.Empty
                : $"PRIMARY KEY ({string.Join(",", primaryKeys)})";

            var clusterData = string.IsNullOrEmpty(cluster) ? string.Empty : $" ON CLUSTER {cluster}";

            var createSql = $"CREATE TABLE IF NOT EXISTS {database}.`{tableName}` {clusterData} ({string.Join(", ", props)}) " +
                $"ENGINE = MergeTree" +
                $" {primaryKey};";

            return createSql;
        }
    }

    internal class InternalPropertyModel
    {
        public List<string> SqlProperties { get; set; }
        public List<string> PrimaryKeys { get; set; }
    }
}
