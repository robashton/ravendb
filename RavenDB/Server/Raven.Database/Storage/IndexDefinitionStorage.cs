//-----------------------------------------------------------------------
// <copyright file="IndexDefinitionStorage.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using Lucene.Net.Analysis.Standard;
using Microsoft.Isam.Esent.Interop;
using Raven.Abstractions.Logging;
using Raven.Imports.Newtonsoft.Json;
using Raven.Abstractions;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Indexing;
using Raven.Abstractions.MEF;
using Raven.Database.Config;
using Raven.Database.Extensions;
using Raven.Database.Linq;
using Raven.Database.Plugins;

namespace Raven.Database.Storage
{
	using Raven.Abstractions.Util.Encryptors;

	public class IndexDefinitionStorage
    {
        private const string IndexDefDir = "IndexDefinitions";

        private readonly ReaderWriterLockSlim currentlyIndexingLock = new ReaderWriterLockSlim();

        private readonly ConcurrentDictionary<int, AbstractViewGenerator> indexCache =
            new ConcurrentDictionary<int, AbstractViewGenerator>();

        private readonly ConcurrentDictionary<int, AbstractTransformer> transformCache =
            new ConcurrentDictionary<int, AbstractTransformer>();

        private readonly ConcurrentDictionary<int, TransformerDefinition> transformDefinitions =
            new ConcurrentDictionary<int, TransformerDefinition>();


        private readonly ConcurrentDictionary<int, IndexDefinition> indexDefinitions =
            new ConcurrentDictionary<int, IndexDefinition>();

        private readonly ConcurrentDictionary<int, IndexDefinition> newDefinitionsThisSession = new ConcurrentDictionary<int, IndexDefinition>();

        private static readonly ILog logger = LogManager.GetCurrentClassLogger();
        private readonly string path;
        private readonly InMemoryRavenConfiguration configuration;
        private readonly OrderedPartCollection<AbstractDynamicCompilationExtension> extensions;

        public IndexDefinitionStorage(
            InMemoryRavenConfiguration configuration,
            ITransactionalStorage transactionalStorage,
            string path,
            IEnumerable<AbstractViewGenerator> compiledGenerators,
            OrderedPartCollection<AbstractDynamicCompilationExtension> extensions)
        {
            this.configuration = configuration;
            this.extensions = extensions; // this is used later in the ctor, so it must appears first
            this.path = Path.Combine(path, IndexDefDir);

            if (Directory.Exists(this.path) == false && configuration.RunInMemory == false)
                Directory.CreateDirectory(this.path);

            if (configuration.RunInMemory == false)
                ReadFromDisk();

            newDefinitionsThisSession.Clear();
        }

        public bool IsNewThisSession(IndexDefinition definition)
        {
            return newDefinitionsThisSession.ContainsKey(definition.IndexId);
        }

        private void ReadFromDisk()
        {
            foreach (var index in Directory.GetFiles(path, "*.index"))
            {
                try
                {
                    var indexDefinition = JsonConvert.DeserializeObject<IndexDefinition>(File.ReadAllText(index), Default.Converters);
                    ResolveAnalyzers(indexDefinition);
                    AddAndCompileIndex(indexDefinition);
                    AddIndex(indexDefinition.IndexId, indexDefinition);
                }
                catch (Exception e)
                {
                    logger.WarnException("Could not compile index " + index + ", skipping bad index", e);
                }
            }

            foreach (var index in Directory.GetFiles(path, "*.transform"))
            {
                try
                {
                    var indexDefinition = JsonConvert.DeserializeObject<TransformerDefinition>(File.ReadAllText(index), Default.Converters);
                    AddAndCompileTransform(indexDefinition);
                    AddTransform(indexDefinition.IndexId, indexDefinition);
                }
                catch (Exception e)
                {
                    logger.WarnException("Could not compile index " + index + ", skipping bad index", e);
                }
            }
        }

        public int IndexesCount
        {
            get { return indexCache.Count; }
        }

        public string[] IndexNames
        {
            get { return indexDefinitions.Values.OrderBy(x=>x.PublicName).Select(x=>x.PublicName).ToArray(); }
        }

	    public int[] Indexes
	    {
            get { return indexCache.Keys.OrderBy(name => name).ToArray(); }
	    }

        public string IndexDefinitionsPath
        {
            get { return path; }
        }

        public string[] TransformerNames
        {
            get { return transformDefinitions.Values
                .OrderBy(x => x.PublicName)
                .Select(x=>x.PublicName)
                .ToArray(); }
        }

        public string CreateAndPersistIndex(IndexDefinition indexDefinition)
        {
            var transformer = AddAndCompileIndex(indexDefinition);
            if (configuration.RunInMemory == false)
            {
                WriteIndexDefinition(indexDefinition);
            }
            return transformer.Name;
        }

        private void WriteIndexDefinition(IndexDefinition indexDefinition)
        {
	        if (configuration.RunInMemory)
		        return;
            var indexName = Path.Combine(path, indexDefinition.IndexId + ".index");
            // Hash the name if it's too long (as a path)
            File.WriteAllText(indexName, JsonConvert.SerializeObject(indexDefinition, Formatting.Indented, Default.Converters));
        }

        public string CreateAndPersistTransform(TransformerDefinition transformerDefinition)
        {
            var transformer = AddAndCompileTransform(transformerDefinition);
            if (configuration.RunInMemory == false)
            {
                var indexName = Path.Combine(path, transformerDefinition.IndexId + ".transform");
                // Hash the name if it's too long (as a path)
                File.WriteAllText(indexName, JsonConvert.SerializeObject(transformerDefinition, Formatting.Indented, Default.Converters));
            }
            return transformer.Name;
        }

        public void UpdateIndexDefinitionWithoutUpdatingCompiledIndex(IndexDefinition definition)
        {
            indexDefinitions.AddOrUpdate(definition.IndexId, s => 
            {
                throw new InvalidOperationException("Cannot find index named: " + definition.IndexId);
            }, (s, indexDefinition) => definition);
            WriteIndexDefinition(definition);
        }

        private DynamicViewCompiler AddAndCompileIndex(IndexDefinition indexDefinition)
        {
            var transformer = new DynamicViewCompiler(indexDefinition.PublicName, indexDefinition, extensions, path, configuration);
            var generator = transformer.GenerateInstance();
            indexCache.AddOrUpdate(indexDefinition.IndexId, generator, (s, viewGenerator) => generator);

            logger.Info("New index {0}:\r\n{1}\r\nCompiled to:\r\n{2}", transformer.Name, transformer.CompiledQueryText,
                              transformer.CompiledQueryText);
            return transformer;
        }

        private DynamicTransformerCompiler AddAndCompileTransform(TransformerDefinition transformerDefinition)
        {
            var transformer = new DynamicTransformerCompiler(transformerDefinition, configuration, extensions, transformerDefinition.PublicName, path);
            var generator = transformer.GenerateInstance();
            transformCache.AddOrUpdate(transformerDefinition.IndexId, generator, (s, viewGenerator) => generator);

            logger.Info("New transformer {0}:\r\n{1}\r\nCompiled to:\r\n{2}", transformer.Name, transformer.CompiledQueryText,
                              transformer.CompiledQueryText);
            return transformer;
        }

		public void RegisterNewIndexInThisSession(string name, IndexDefinition definition)
		{
			newDefinitionsThisSession.TryAdd(definition.IndexId, definition);
		}

        public void AddIndex(int id, IndexDefinition definition)
        {
            indexDefinitions.AddOrUpdate(id, definition, (s1, def) =>
            {
                if (def.IsCompiled)
                    throw new InvalidOperationException("Index " + id + " is a compiled index, and cannot be replaced");
                return definition;
            });
        }

        public void AddTransform(int id, TransformerDefinition definition)
        {
            transformDefinitions.AddOrUpdate(id, definition, (s1, def) => definition);
        }

        public void RemoveIndex(int id)
        {
            AbstractViewGenerator ignoredViewGenerator;
            indexCache.TryRemove(id, out ignoredViewGenerator);
            IndexDefinition ignoredIndexDefinition;
            indexDefinitions.TryRemove(id, out ignoredIndexDefinition);
            newDefinitionsThisSession.TryRemove(id, out ignoredIndexDefinition);
            if (configuration.RunInMemory)
                return;
            File.Delete(GetIndexSourcePath(id.ToString()) + ".index");
        }

        private string GetIndexSourcePath(string name)
        {
            var encodeIndexNameIfNeeded = FixupIndexName(name, path);
            return Path.Combine(path, MonoHttpUtility.UrlEncode(encodeIndexNameIfNeeded));
        }
        
        public IndexDefinition GetIndexDefinition(string name)
        {
            return indexDefinitions.Values.FirstOrDefault(x => String.Compare(x.PublicName, name, StringComparison.OrdinalIgnoreCase) == 0);
        }


        public IndexDefinition GetIndexDefinition(int id)
        {
            IndexDefinition value;
            indexDefinitions.TryGetValue(id, out value);
            return value;
        }

        public TransformerDefinition GetTransformerDefinition(string name)
        {
            return transformDefinitions.Values.FirstOrDefault(x => String.Compare(x.PublicName, name, StringComparison.OrdinalIgnoreCase) == 0);
        }

        public TransformerDefinition GetTransformerDefinition(int id)
        {
            TransformerDefinition value;
            transformDefinitions.TryGetValue(id, out value);
            return value;
        }

        public AbstractViewGenerator GetViewGenerator(string name)
        {
            return indexCache.Values.FirstOrDefault(x => String.CompareOrdinal(x.Name, name) == 0);
        }

        public AbstractViewGenerator GetViewGenerator(int id)
        {
            AbstractViewGenerator value;
            if (indexCache.TryGetValue(id, out value) == false)
                return null;
            return value;
        }

        public IndexCreationOptions FindIndexCreationOptions(IndexDefinition indexDef)
        {
            var indexDefinition = GetIndexDefinition(indexDef.PublicName);
            if (indexDefinition != null)
            {
                return indexDefinition.Equals(indexDef)
                    ? IndexCreationOptions.Noop
                    : IndexCreationOptions.Update;
            }
            return IndexCreationOptions.Create;
        }

        public bool Contains(string indexName)
        {
            return indexDefinitions.Any(x => String.CompareOrdinal(x.Value.PublicName, indexName) == 0);
        }

        public string FixupIndexName(string index)
        {
            return FixupIndexName(index, path);
        }

        public static string FixupIndexName(string index, string path)
        {
            index = index.Trim();
            string prefix = null;
            if (index.StartsWith("Temp/") || index.StartsWith("Auto/"))
            {
                prefix = index.Substring(0, 5);
            }
            if (path.Length + index.Length > 230 ||
                Encoding.Unicode.GetByteCount(index) >= 255)
            {
                var bytes = Encryptor.Current.Hash.Compute(Encoding.UTF8.GetBytes(index));
                return prefix + Convert.ToBase64String(bytes);
            }
            return index;
        }

        public static void ResolveAnalyzers(IndexDefinition indexDefinition)
        {
            // Stick Lucene.Net's namespace to all analyzer aliases that are missing a namespace
            var analyzerNames = (from analyzer in indexDefinition.Analyzers
                                 where analyzer.Value.IndexOf('.') == -1
                                 select analyzer).ToArray();

            // Only do this for analyzer that actually exist; we do this here to be able to throw a correct error later on
            foreach (var a in analyzerNames.Where(a => typeof(StandardAnalyzer).Assembly.GetType("Lucene.Net.Analysis." + a.Value) != null))
            {
                indexDefinition.Analyzers[a.Key] = "Lucene.Net.Analysis." + a.Value;
            }
        }

        public IDisposable TryRemoveIndexContext()
        {
            if (currentlyIndexingLock.TryEnterWriteLock(TimeSpan.FromSeconds(60)) == false)
                throw new InvalidOperationException("Cannot modify indexes while indexing is in progress (already waited full minute). Try again later");
            return new DisposableAction(currentlyIndexingLock.ExitWriteLock);
        }

        public IDisposable CurrentlyIndexing()
        {
            currentlyIndexingLock.EnterReadLock();

            return new DisposableAction(currentlyIndexingLock.ExitReadLock);

        }

        public void RemoveTransformer(string name)
        {
            var transformer = GetTransformerDefinition(name);
            if (transformer == null) return;
            RemoveTransformer(transformer.IndexId);
        }

        public void RemoveIndex(string name)
        {
            var index = GetIndexDefinition(name);
            if (index == null) return;
            RemoveIndex(index.IndexId);
        }

        public void RemoveTransformer(int id)
        {
            AbstractTransformer ignoredViewGenerator;
            transformCache.TryRemove(id, out ignoredViewGenerator);
            TransformerDefinition ignoredIndexDefinition;
            transformDefinitions.TryRemove(id, out ignoredIndexDefinition);
            if (configuration.RunInMemory)
                return;
            File.Delete(GetIndexSourcePath(id.ToString()) + ".transform");
        }

        public AbstractTransformer GetTransformer(int id)
        {
            AbstractTransformer value;
            if (transformCache.TryGetValue(id, out value) == false)
                return null;
            return value;
        }

        public AbstractTransformer GetTransformer(string name)
        {
            return transformCache.Values.FirstOrDefault(x => String.Compare(x.Name, name, StringComparison.OrdinalIgnoreCase) == 0);
        }

	    public int NextIndexId()
	    {
	        return indexDefinitions.Any() ? indexDefinitions.Values.Max(x => x.IndexId) + 1 : 0;
	    }
    }
}