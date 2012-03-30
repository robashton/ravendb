﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Collections.Specialized;
using System.IO;
using System.Threading.Tasks;
using RavenFS.Client;
using RavenFS.Rdc;
using RavenFS.Rdc.Wrapper;
using RavenFS.Storage;
using RavenFS.Util;
using RavenFS.Extensions;
using RavenFS.Infrastructure;

namespace RavenFS.Controllers
{
    public class SynchronizationController : RavenController
    {
        public Task<HttpResponseMessage<SynchronizationReport>> Get(string fileName, string sourceServerUrl)
        {
            var remoteSignatureCache = new VolatileSignatureRepository(TempDirectoryTools.Create());

            var sourceRavenFileSystemClient = new RavenFileSystemClient(sourceServerUrl);
            var localRdcManager = new LocalRdcManager(SignatureRepository, Storage, SigGenerator);
            var remoteRdcManager = new RemoteRdcManager(sourceRavenFileSystemClient, SignatureRepository, remoteSignatureCache);

            if (String.IsNullOrEmpty(sourceServerUrl))
            {
                throw new Exception("Unknown server identifier " + sourceServerUrl);
            }
        	var sourceMetadataAsync = sourceRavenFileSystemClient.GetMetadataForAsync(fileName);
            var localFileDataInfo = GetLocalFileDataInfo(fileName);

            var seedSignatureManifest = localRdcManager.GetSignatureManifest(localFileDataInfo);
            return remoteRdcManager.SynchronizeSignaturesAsync(localFileDataInfo)
                .ContinueWith(
                    task =>
                    {
                        var sourceSignatureManifest = task.Result;
                        return sourceMetadataAsync.ContinueWith(
                            sourceMetadataTask =>
                            {
                                if(sourceSignatureManifest.Signatures.Count > 0)
									return  Synchronize(remoteSignatureCache, sourceServerUrl, fileName,
                                                  sourceSignatureManifest, seedSignatureManifest,
                                                  sourceMetadataTask.Result);
                            	return Download(sourceRavenFileSystemClient, fileName, sourceMetadataAsync);
                            }).Unwrap()
							.ContinueWith( synchronizationTask => new HttpResponseMessage<SynchronizationReport>(synchronizationTask.Result));
                    }).Unwrap();
        }

        private Task<SynchronizationReport> Synchronize(ISignatureRepository remoteSignatureRepository, string sourceServerUrl, string fileName, SignatureManifest sourceSignatureManifest, SignatureManifest seedSignatureManifest, NameValueCollection sourceMetadata)
        {
            var seedSignatureInfo = SignatureInfo.Parse(seedSignatureManifest.Signatures.Last().Name);
            var sourceSignatureInfo = SignatureInfo.Parse(sourceSignatureManifest.Signatures.Last().Name);
            var needListGenerator = new NeedListGenerator(SignatureRepository, remoteSignatureRepository);
            var outputFile = StorageStream.CreatingNewAndWritting(Storage, Search,
                                                                  fileName + ".result",
                                                                  sourceMetadata.FilterHeaders());
            var needList = needListGenerator.CreateNeedsList(seedSignatureInfo, sourceSignatureInfo);

            return NeedListParser.ParseAsync(
                new RemotePartialAccess(sourceServerUrl, fileName),
                new StoragePartialAccess(Storage, fileName),
                outputFile, needList).ContinueWith(
                    _ =>
                    {
                        outputFile.Dispose();
                        needListGenerator.Dispose();
						_.AssertNotFaulted();
                    	return new SynchronizationReport
                    	{
                    		FileName = fileName,
                    		BytesTransfered = needList.Sum(
                    			item =>
                    			item.BlockType == RdcNeedType.Source
                    				? (long) item.BlockLength
                    				: 0L),
                    		BytesCopied = needList.Sum(
                    			item =>
                    			item.BlockType == RdcNeedType.Seed
                    				? (long) item.BlockLength
                    				: 0L),
                    		NeedListLength = needList.Count
                    	};
                    });

        }

        private Task<SynchronizationReport> Download(RavenFileSystemClient sourceRavenFileSystemClient, string fileName, Task<NameValueCollection> sourceMetadataAsync)
        {
            return sourceMetadataAsync.ContinueWith(
                task => StorageStream.CreatingNewAndWritting(Storage, Search, fileName + ".result",
                                                             task.Result.FilterHeaders()))
                .ContinueWith(
                    task => sourceRavenFileSystemClient.DownloadAsync(fileName, task.Result)
                            	.ContinueWith(
                            		_ =>
                            		{
                            			task.Result.Dispose();
                            			return new SynchronizationReport
                            			{
                            				FileName = fileName,
                            				BytesCopied = StorageStream.Reading(Storage, fileName + ".result").Length
                            			};
                            		})).Unwrap();
        }


        private DataInfo GetLocalFileDataInfo(string fileName)
        {
            FileAndPages fileAndPages = null;
            Storage.Batch(accessor => fileAndPages = accessor.GetFile(fileName, 0, 0));
            return new DataInfo
            {
                CreatedAt = Convert.ToDateTime(fileAndPages.Metadata["Last-Modified"]),
                Length = fileAndPages.TotalSize ?? 0,
                Name = fileAndPages.Name
            };
        }
    }
}