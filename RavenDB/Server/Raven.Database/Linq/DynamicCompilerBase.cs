using System;
using System.IO;
using Raven.Abstractions.MEF;
using Raven.Database.Config;
using Raven.Database.Extensions;
using Raven.Database.Plugins;
using Raven.Database.Util;

namespace Raven.Database.Linq
{
	public class DynamicCompilerBase
	{
		protected const string uniqueTextToken = "96E65595-1C9E-4BFB-A0E5-80BF2D6FC185";
		
		protected OrderedPartCollection<AbstractDynamicCompilationExtension> extensions;
		protected string basePath;
		protected readonly InMemoryRavenConfiguration configuration;
		protected int id;
		public string CompiledQueryText { get; set; }
		public Type GeneratedType { get; set; }
		public int Name
		{
          get { return id; }
		}
		public string CSharpSafeName { get; set; }

		public DynamicCompilerBase(InMemoryRavenConfiguration configuration, OrderedPartCollection<AbstractDynamicCompilationExtension> extensions, int id, string basePath)
		{
			this.configuration = configuration;
			this.id = id;
			this.extensions = extensions;
			if (configuration.RunInMemory == false)
			{
				this.basePath = Path.Combine(basePath, "temp");
				if (Directory.Exists(this.basePath) == false)
					Directory.CreateDirectory(this.basePath);
			}
		}
	}
}