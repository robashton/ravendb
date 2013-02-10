//-----------------------------------------------------------------------
// <copyright file="DocumentConvention.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Reflection;
using System.Runtime.Serialization.Formatters;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Connection.Async;
using Raven.Client.Indexes;
using Raven.Imports.Newtonsoft.Json;
using Raven.Imports.Newtonsoft.Json.Serialization;
using Raven.Abstractions;
using Raven.Abstractions.Json;
using Raven.Client.Connection;
using Raven.Client.Converters;
using Raven.Client.Util;
using Raven.Json.Linq;

namespace Raven.Client.Document
{
	/// <summary>
	/// The set of conventions used by the <see cref="DocumentStore"/> which allow the users to customize
	/// the way the Raven client API behaves
	/// </summary>
	public class DocumentConvention
	{
		public delegate IEnumerable<object> ApplyReduceFunctionFunc(
			Type indexType,
			Type resultType,
			IEnumerable<object> results,
			Func<Func<IEnumerable<object>, IEnumerable>> generateTransformResults);

		private Dictionary<Type, PropertyInfo> idPropertyCache = new Dictionary<Type, PropertyInfo>();
		private Dictionary<Type, Func<IEnumerable<object>, IEnumerable>> compiledReduceCache = new Dictionary<Type, Func<IEnumerable<object>, IEnumerable>>();

#if !SILVERLIGHT
		private readonly IList<Tuple<Type, Func<string, IDatabaseCommands, object, string>>> listOfRegisteredIdConventions =
			new List<Tuple<Type, Func<string, IDatabaseCommands, object, string>>>();
#endif
		private readonly IList<Tuple<Type, Func<string, IAsyncDatabaseCommands, object, Task<string>>>> listOfRegisteredIdConventionsAsync =
			new List<Tuple<Type, Func<string, IAsyncDatabaseCommands, object, Task<string>>>>();

		/// <summary>
		/// Initializes a new instance of the <see cref="DocumentConvention"/> class.
		/// </summary>
		public DocumentConvention()
		{
			IdentityTypeConvertors = new List<ITypeConverter>
			{
				new GuidConverter(),
				new Int32Converter(),
				new Int64Converter(),
			};
			MaxFailoverCheckPeriod = TimeSpan.FromMinutes(5);
			DisableProfiling = true;
			EnlistInDistributedTransactions = true;
			UseParallelMultiGet = true;
			DefaultQueryingConsistency = ConsistencyOptions.MonotonicRead;
			FailoverBehavior = FailoverBehavior.AllowReadsFromSecondaries;
			ShouldCacheRequest = url => true;
			FindIdentityProperty = q => q.Name == "Id";
			FindClrType = (id, doc, metadata) => metadata.Value<string>(Abstractions.Data.Constants.RavenClrType);

#if !SILVERLIGHT
			FindClrTypeName = entityType => ReflectionUtil.GetFullNameWithoutVersionInformation(entityType);
#else
			FindClrTypeName = entityType => entityType.AssemblyQualifiedName;
#endif
			TransformTypeTagNameToDocumentKeyPrefix = DefaultTransformTypeTagNameToDocumentKeyPrefix;
			FindFullDocumentKeyFromNonStringIdentifier = DefaultFindFullDocumentKeyFromNonStringIdentifier;
			FindIdentityPropertyNameFromEntityName = entityName => "Id";
			FindTypeTagName = DefaultTypeTagName;
			FindPropertyNameForIndex = (indexedType, indexedName, path, prop) => (path + prop).Replace(",", "_").Replace(".", "_");
			FindPropertyNameForDynamicIndex = (indexedType, indexedName, path, prop) => path + prop;
			IdentityPartsSeparator = "/";
			JsonContractResolver = new DefaultRavenContractResolver(shareCache: true)
			{
				DefaultMembersSearchFlags = BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance
			};
			MaxNumberOfRequestsPerSession = 30;
			ApplyReduceFunction = DefaultApplyReduceFunction;
			ReplicationInformerFactory = url => new ReplicationInformer(this);
			CustomizeJsonSerializer = serializer => { };
			FindIdValuePartForValueTypeConversion = (entity, id) => id.Split(new[] { IdentityPartsSeparator }, StringSplitOptions.RemoveEmptyEntries).Last();
		}

		private IEnumerable<object> DefaultApplyReduceFunction(
			Type indexType,
			Type resultType,
			IEnumerable<object> results,
			Func<Func<IEnumerable<object>, IEnumerable>> generateTransformResults)
		{
			Func<IEnumerable<object>, IEnumerable> compile;
			if (compiledReduceCache.TryGetValue(indexType, out compile) == false)
			{
				compile = generateTransformResults();
				compiledReduceCache = new Dictionary<Type, Func<IEnumerable<object>, IEnumerable>>(compiledReduceCache)
				{
					{indexType, compile}
				};
			}
			return compile(results).Cast<object>()
				.Select(result =>
				{
					// we got an anonymous object and we need to get the reduce results
					var ravenJTokenWriter = new RavenJTokenWriter();
					var jsonSerializer = CreateSerializer();
					jsonSerializer.Serialize(ravenJTokenWriter, result);
					return jsonSerializer.Deserialize(new RavenJTokenReader(ravenJTokenWriter.Token), resultType);
				});
		}

		public static string DefaultTransformTypeTagNameToDocumentKeyPrefix(string typeTagName)
		{
			var count = typeTagName.Count(char.IsUpper);

			if (count <= 1) // simple name, just lower case it
				return typeTagName.ToLowerInvariant();

			// multiple capital letters, so probably something that we want to preserve caps on.
			return typeTagName;
		}

		///<summary>
		/// Find the full document name assuming that we are using the standard conventions
		/// for generating a document key
		///</summary>
		///<returns></returns>
		public string DefaultFindFullDocumentKeyFromNonStringIdentifier(object id, Type type, bool allowNull)
		{
			var converter = IdentityTypeConvertors.FirstOrDefault(x => x.CanConvertFrom(id.GetType()));
			var tag = GetTypeTagName(type);
			if (tag != null)
			{
				tag = TransformTypeTagNameToDocumentKeyPrefix(tag);
				tag += IdentityPartsSeparator;
			}
			if (converter != null)
			{
				return converter.ConvertFrom(tag, id, allowNull);
			}
			return tag + id;
		}


		/// <summary>
		/// How should we behave in a replicated environment when we can't 
		/// reach the primary node and need to failover to secondary node(s).
		/// </summary>
		public FailoverBehavior FailoverBehavior { get; set; }

		/// <summary>
		/// Register an action to customize the json serializer used by the <see cref="DocumentStore"/>
		/// </summary>
		public Action<JsonSerializer> CustomizeJsonSerializer { get; set; }

		/// <summary>
		/// Disable all profiling support
		/// </summary>
		public bool DisableProfiling { get; set; }

		/// <summary>
		/// Enable multipule async operations
		/// </summary>
		public bool AllowMultipuleAsyncOperations { get; set; }

		///<summary>
		/// A list of type converters that can be used to translate the document key (string)
		/// to whatever type it is that is used on the entity, if the type isn't already a string
		///</summary>
		public List<ITypeConverter> IdentityTypeConvertors { get; set; }

		/// <summary>
		/// Gets or sets the identity parts separator used by the HiLo generators
		/// </summary>
		/// <value>The identity parts separator.</value>
		public string IdentityPartsSeparator { get; set; }

		/// <summary>
		/// Gets or sets the default max number of requests per session.
		/// </summary>
		/// <value>The max number of requests per session.</value>
		public int MaxNumberOfRequestsPerSession { get; set; }

		/// <summary>
		/// Whatever to allow queries on document id.
		/// By default, queries on id are disabled, because it is far more efficient
		/// to do a Load() than a Query() if you already know the id.
		/// This is NOT recommended and provided for backward compatibility purposes only.
		/// </summary>
		public bool AllowQueriesOnId { get; set; }

		/// <summary>
		/// The consistency options used when querying the database by default
		/// Note that this option impact only queries, since we have Strong Consistency model for the documents
		/// </summary>
		public ConsistencyOptions DefaultQueryingConsistency { get; set; }

		/// <summary>
		/// Generates the document key using identity.
		/// </summary>
		/// <param name="conventions">The conventions.</param>
		/// <param name="entity">The entity.</param>
		/// <returns></returns>
		public static string GenerateDocumentKeyUsingIdentity(DocumentConvention conventions, object entity)
		{
			return conventions.FindTypeTagName(entity.GetType()).ToLower() + "/";
		}

		private static IDictionary<Type, string> cachedDefaultTypeTagNames = new Dictionary<Type, string>();
		private int requestCount;

		/// <summary>
		/// Get the default tag name for the specified type.
		/// </summary>
		public static string DefaultTypeTagName(Type t)
		{
			string result;
			if (cachedDefaultTypeTagNames.TryGetValue(t, out result))
				return result;

			if (t.Name.Contains("<>"))
				return null;
			if (t.IsGenericType)
			{
				var name = t.GetGenericTypeDefinition().Name;
				if (name.Contains('`'))
				{
					name = name.Substring(0, name.IndexOf('`'));
				}
				var sb = new StringBuilder(Inflector.Pluralize(name));
				foreach (var argument in t.GetGenericArguments())
				{
					sb.Append("Of")
						.Append(DefaultTypeTagName(argument));
				}
				result = sb.ToString();
			}
			else
			{
				result = Inflector.Pluralize(t.Name);
			}
			var temp = new Dictionary<Type, string>(cachedDefaultTypeTagNames);
			temp[t] = result;
			cachedDefaultTypeTagNames = temp;
			return result;
		}

		/// <summary>
		/// Gets the name of the type tag.
		/// </summary>
		/// <param name="type">The type.</param>
		/// <returns></returns>
		public string GetTypeTagName(Type type)
		{
			return FindTypeTagName(type) ?? DefaultTypeTagName(type);
		}

#if !SILVERLIGHT
		/// <summary>
		/// Generates the document key.
		/// </summary>
		/// <param name="entity">The entity.</param>
		/// <returns></returns>
		public string GenerateDocumentKey(string dbName, IDatabaseCommands databaseCommands, object entity)
		{
			var type = entity.GetType();
			foreach (var typeToRegisteredIdConvention in listOfRegisteredIdConventions
				.Where(typeToRegisteredIdConvention => typeToRegisteredIdConvention.Item1.IsAssignableFrom(type)))
			{
				return typeToRegisteredIdConvention.Item2(dbName, databaseCommands, entity);
			}

			if (listOfRegisteredIdConventionsAsync.Any(x => x.Item1.IsAssignableFrom(type)))
			{
				throw new InvalidOperationException("Id covention for synchronous operation was not found for entity " + type.FullName + ", but convention for asynchronous operation exists.");
			}

			return DocumentKeyGenerator(dbName, databaseCommands, entity);
		}
#endif

		public Task<string> GenerateDocumentKeyAsync(string dbName, IAsyncDatabaseCommands databaseCommands, object entity)
		{
			var type = entity.GetType();
			foreach (var typeToRegisteredIdConvention in listOfRegisteredIdConventionsAsync
				.Where(typeToRegisteredIdConvention => typeToRegisteredIdConvention.Item1.IsAssignableFrom(type)))
			{
				return typeToRegisteredIdConvention.Item2(dbName, databaseCommands, entity);
			}

#if !SILVERLIGHT
			if (listOfRegisteredIdConventions.Any(x => x.Item1.IsAssignableFrom(type)))
			{
				throw new InvalidOperationException("Id covention for asynchronous operation was not found for entity " + type.FullName + ", but convention for synchronous operation exists.");
			}
#endif

			return AsyncDocumentKeyGenerator(dbName, databaseCommands, entity);
		}

		/// <summary>
		/// Gets the identity property.
		/// </summary>
		/// <param name="type">The type.</param>
		/// <returns></returns>
		public PropertyInfo GetIdentityProperty(Type type)
		{
			PropertyInfo info;
			var currentIdPropertyCache = idPropertyCache;
			if (currentIdPropertyCache.TryGetValue(type, out info))
				return info;

			// we want to ignore nested entities from index creation tasks
			if(type.IsNested && type.DeclaringType != null && 
				typeof(AbstractIndexCreationTask).IsAssignableFrom(type.DeclaringType))
			{
				idPropertyCache = new Dictionary<Type, PropertyInfo>(currentIdPropertyCache)
				{
					{type, null}
				};
				return null;
			}

			var identityProperty = GetPropertiesForType(type).FirstOrDefault(FindIdentityProperty);

			if (identityProperty != null && identityProperty.DeclaringType != type)
			{
				var propertyInfo = identityProperty.DeclaringType.GetProperty(identityProperty.Name);
				identityProperty = propertyInfo ?? identityProperty;
			}

			idPropertyCache = new Dictionary<Type, PropertyInfo>(currentIdPropertyCache)
			{
				{type, identityProperty}
			};

			return identityProperty;
		}

		private static IEnumerable<PropertyInfo> GetPropertiesForType(Type type)
		{
			foreach (var propertyInfo in type.GetProperties())
			{
				yield return propertyInfo;
			}

			foreach (var @interface in type.GetInterfaces())
			{
				foreach (var propertyInfo in GetPropertiesForType(@interface))
				{
					yield return propertyInfo;
				}
			}
		}

		/// <summary>
		/// Gets or sets the function to find the clr type of a document.
		/// </summary>
		public Func<string, RavenJObject, RavenJObject, string> FindClrType { get; set; }

		/// <summary>
		/// Gets or sets the function to find the clr type name from a clr type
		/// </summary>
		public Func<Type, string> FindClrTypeName { get; set; }

		/// <summary>
		/// Gets or sets the function to find the full document key based on the type of a document
		/// and the value type identifier (just the numeric part of the id).
		/// </summary>
		public Func<object, Type, bool, string> FindFullDocumentKeyFromNonStringIdentifier { get; set; }

		/// <summary>
		/// Gets or sets the json contract resolver.
		/// </summary>
		/// <value>The json contract resolver.</value>
		public IContractResolver JsonContractResolver { get; set; }

		/// <summary>
		/// Gets or sets the function to find the type tag.
		/// </summary>
		/// <value>The name of the find type tag.</value>
		public Func<Type, string> FindTypeTagName { get; set; }

		/// <summary>
		/// Gets or sets the function to find the indexed property name
		/// given the indexed document type, the index name, the current path and the property path.
		/// </summary>
		public Func<Type, string, string, string, string> FindPropertyNameForIndex { get; set; }

		/// <summary>
		/// Gets or sets the function to find the indexed property name
		/// given the indexed document type, the index name, the current path and the property path.
		/// </summary>
		public Func<Type, string, string, string, string> FindPropertyNameForDynamicIndex { get; set; }

		/// <summary>
		/// Whatever or not RavenDB should cache the request to the specified url.
		/// </summary>
		public Func<string, bool> ShouldCacheRequest { get; set; }

		/// <summary>
		/// Gets or sets the function to find the identity property.
		/// </summary>
		/// <value>The find identity property.</value>
		public Func<PropertyInfo, bool> FindIdentityProperty { get; set; }

		/// <summary>
		/// Get or sets the function to get the identity property name from the entity name
		/// </summary>
		public Func<string, string> FindIdentityPropertyNameFromEntityName { get; set; }
#if !SILVERLIGHT
		/// <summary>
		/// Gets or sets the document key generator.
		/// </summary>
		/// <value>The document key generator.</value>
		public Func<string, IDatabaseCommands, object, string> DocumentKeyGenerator { get; set; }
#endif

		/// <summary>
		/// Gets or sets the document key generator.
		/// </summary>
		/// <value>The document key generator.</value>
		public Func<string, IAsyncDatabaseCommands, object, Task<string>> AsyncDocumentKeyGenerator { get; set; }

		/// <summary>
		/// Instruct RavenDB to parallel Multi Get processing 
		/// when handling lazy requests
		/// </summary>
		public bool UseParallelMultiGet { get; set; }
#if !SILVERLIGHT
		/// <summary>
		/// Register an id convention for a single type (and all of its derived types.
		/// Note that you can still fall back to the DocumentKeyGenerator if you want.
		/// </summary>
		public DocumentConvention RegisterIdConvention<TEntity>(Func<string, IDatabaseCommands, TEntity, string> func)
		{
			var type = typeof(TEntity);
			var entryToRemove = listOfRegisteredIdConventions.FirstOrDefault(x => x.Item1 == type);
			if (entryToRemove != null)
			{
				listOfRegisteredIdConventions.Remove(entryToRemove);
			}

			int index;
			for (index = 0; index < listOfRegisteredIdConventions.Count; index++)
			{
				var entry = listOfRegisteredIdConventions[index];
				if (entry.Item1.IsAssignableFrom(type))
				{
					break;
				}
			}

			var item = new Tuple<Type, Func<string, IDatabaseCommands, object, string>>(typeof(TEntity), (dbName, commands, o) => func(dbName, commands, (TEntity)o));
			listOfRegisteredIdConventions.Insert(index, item);

			return this;
		}
#endif

		/// <summary>
		/// Register an async id convention for a single type (and all of its derived types.
		/// Note that you can still fall back to the DocumentKeyGenerator if you want.
		/// </summary>
		public DocumentConvention RegisterAsyncIdConvention<TEntity>(Func<string, IAsyncDatabaseCommands, TEntity, Task<string>> func)
		{
			var type = typeof(TEntity);
			var entryToRemove = listOfRegisteredIdConventionsAsync.FirstOrDefault(x => x.Item1 == type);
			if (entryToRemove != null)
			{
				listOfRegisteredIdConventionsAsync.Remove(entryToRemove);
			}

			int index;
			for (index = 0; index < listOfRegisteredIdConventionsAsync.Count; index++)
			{
				var entry = listOfRegisteredIdConventionsAsync[index];
				if (entry.Item1.IsAssignableFrom(type))
				{
					break;
				}
			}

			var item = new Tuple<Type, Func<string, IAsyncDatabaseCommands, object, Task<string>>>(typeof(TEntity), (dbName, commands, o) => func(dbName, commands, (TEntity)o));
			listOfRegisteredIdConventionsAsync.Insert(index, item);

			return this;
		}

		/// <summary>
		/// Creates the serializer.
		/// </summary>
		/// <returns></returns>
		public JsonSerializer CreateSerializer()
		{
			var jsonSerializer = new JsonSerializer
			{
				DateParseHandling = DateParseHandling.None,
				ObjectCreationHandling = ObjectCreationHandling.Replace,
				ContractResolver = JsonContractResolver,
				TypeNameHandling = TypeNameHandling.Auto,
				TypeNameAssemblyFormat = FormatterAssemblyStyle.Simple,
				ConstructorHandling = ConstructorHandling.AllowNonPublicDefaultConstructor,
				Converters =
					{
						new JsonLuceneDateTimeConverter(),
						new JsonFloatConverter(),
						new JsonNumericConverter<int>(int.TryParse),
						new JsonNumericConverter<long>(long.TryParse),
						new JsonNumericConverter<decimal>(decimal.TryParse),
						new JsonNumericConverter<double>(double.TryParse),
						new JsonNumericConverter<short>(short.TryParse),
						new JsonMultiDimensionalArrayConverter(),
#if !SILVERLIGHT
						new JsonDynamicConverter()
#endif
					}
			};

			for (var i = Default.Converters.Length - 1; i >= 0; i--)
			{
				jsonSerializer.Converters.Insert(0, Default.Converters[i]);
			}

			if (SaveEnumsAsIntegers)
			{
				var converter = jsonSerializer.Converters.FirstOrDefault(x => x is JsonEnumConverter);
				if (converter != null)
					jsonSerializer.Converters.Remove(converter);
			}

			CustomizeJsonSerializer(jsonSerializer);
			return jsonSerializer;
		}

		/// <summary>
		/// Get the CLR type (if exists) from the document
		/// </summary>
		public string GetClrType(string id, RavenJObject document, RavenJObject metadata)
		{
			return FindClrType(id, document, metadata);
		}

		/// <summary>
		///  Get the CLR type name to be stored in the entity metadata
		/// </summary>
		public string GetClrTypeName(Type entityType)
		{
			return FindClrTypeName(entityType);
		}

		/// <summary>
		/// Handles unauthenticated responses, usually by authenticating against the oauth server
		/// </summary>
		public Func<HttpWebResponse, Action<HttpWebRequest>> HandleUnauthorizedResponse { get; set; }

		/// <summary>
		/// Handles forbidden responses
		/// </summary>
		public Func<HttpWebResponse, Action<HttpWebRequest>> HandleForbiddenResponse { get; set; }

		/// <summary>
		/// Begins handling of unauthenticated responses, usually by authenticating against the oauth server
		/// in async manner
		/// </summary>
		public Func<HttpWebResponse, Task<Action<HttpWebRequest>>> HandleUnauthorizedResponseAsync { get; set; }

		/// <summary>
		/// Begins handling of forbidden responses
		/// in async manner
		/// </summary>
		public Func<HttpWebResponse, Task<Action<HttpWebRequest>>> HandleForbiddenResponseAsync { get; set; }

		/// <summary>
		/// When RavenDB needs to convert between a string id to a value type like int or guid, it calls
		/// this to perform the actual work
		/// </summary>
		public Func<object, string, string> FindIdValuePartForValueTypeConversion { get; set; }

		/// <summary>
		/// Saves Enums as integers and instruct the Linq provider to query enums as integer values.
		/// </summary>
		public bool SaveEnumsAsIntegers { get; set; }

		/// <summary>
		/// Translate the type tag name to the document key prefix
		/// </summary>
		public Func<string, string> TransformTypeTagNameToDocumentKeyPrefix { get; set; }

		///<summary>
		/// Whatever or not RavenDB will automatically enlist in distributed transactions
		///</summary>
		public bool EnlistInDistributedTransactions { get; set; }

		/// <summary>
		/// Clone the current conventions to a new instance
		/// </summary>
		public DocumentConvention Clone()
		{
			return (DocumentConvention)MemberwiseClone();
		}

		/// <summary>
		/// This is called in order to ensure that reduce function in a sharded environment is run 
		/// over the merged results
		/// </summary>
		public ApplyReduceFunctionFunc ApplyReduceFunction { get; set; }


		/// <summary>
		/// This is called to provide replication behavior for the client. You can customize 
		/// this to inject your own replication / failover logic.
		/// </summary>
		public Func<string, ReplicationInformer> ReplicationInformerFactory { get; set; }


		public FailoverBehavior FailoverBehaviorWithoutFlags
		{
			get { return FailoverBehavior & (~FailoverBehavior.ReadFromAllServers); }
		}

		/// <summary>
		/// The maximum amount of time that we will wait before checking
		/// that a failed node is still up or not.
		/// Default: 5 minutes
		/// </summary>
		public TimeSpan MaxFailoverCheckPeriod { get; set; }

		public int IncrementRequestCount()
		{
			return Interlocked.Increment(ref requestCount);
		}

		public delegate bool TryConvertValueForQueryDelegate<in T>(string fieldName, T value, QueryValueConvertionType convertionType, out string strValue);

		private readonly List<Tuple<Type,TryConvertValueForQueryDelegate<object>>> listOfQueryValueConverters	 = new List<Tuple<Type, TryConvertValueForQueryDelegate<object>>>();

		public void RegisterQueryValueConverter<T>(TryConvertValueForQueryDelegate<T> converter)
		{
			TryConvertValueForQueryDelegate<object> actual = (string name, object value, QueryValueConvertionType convertionType, out string strValue) =>
			{
				if(value is T)
					return converter(name, (T)value, convertionType, out strValue);
				strValue = null;
				return false;
			};

			int index;
			for (index = 0; index < listOfQueryValueConverters.Count; index++)
			{
				var entry = listOfQueryValueConverters[index];
				if (entry.Item1.IsAssignableFrom(typeof(T)))
				{
					break;
				}
			}

			listOfQueryValueConverters.Insert(index, Tuple.Create(typeof(T), actual));
		}



		public bool TryConvertValueForQuery(string fieldName, object value, QueryValueConvertionType convertionType, out string strValue)
		{
			foreach (var queryValueConverterTuple in listOfQueryValueConverters
					.Where(tuple => tuple.Item1.IsInstanceOfType(value)))
			{
				return queryValueConverterTuple.Item2(fieldName, value, convertionType, out strValue);
			}
			strValue = null;
			return false;
		}
	}

	public enum QueryValueConvertionType
	{
		Equality,
		Range
	}

	/// <summary>
	/// The consistency options for all queries, fore more details about the consistency options, see:
	/// http://www.allthingsdistributed.com/2008/12/eventually_consistent.html
	/// 
	/// Note that this option impact only queries, since we have Strong Consistency model for the documents
	/// </summary>
	public enum ConsistencyOptions
	{
		/// <summary>
		/// Ensures that after querying an index at time T, you will never see the results
		/// of the index at a time prior to T.
		/// This is ensured by the server, and require no action from the client
		/// </summary>
		MonotonicRead,
		/// <summary>
		///  After updating a documents, will only accept queries which already indexed the updated value.
		/// </summary>
		QueryYourWrites,
	}
}
