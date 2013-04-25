﻿using System;
using System.Collections.Generic;
using System.IO;

namespace RavenFS.Synchronization.Rdc.Wrapper
{
	public interface ISignatureRepository : IDisposable
	{
		Stream GetContentForReading(string sigName);
		Stream CreateContent(string sigName);
		void Flush(IEnumerable<SignatureInfo> signatureInfos);
		IEnumerable<SignatureInfo> GetByFileName();
		DateTime? GetLastUpdate();
	}
}