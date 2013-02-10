Include ".\build_utils.ps1"

properties {
	$base_dir  = resolve-path .
	$lib_dir = "$base_dir\SharedLibs"
	$build_dir = "$base_dir\build"
	$packages_dir = "$base_dir\packages"
	$buildartifacts_dir = "$build_dir\"
	$sln_file = "$base_dir\zzz_RavenDB_Release.sln"
	$version = "2.0"
	$tools_dir = "$base_dir\Tools"
	$release_dir = "$base_dir\Release"
	$uploader = "..\Uploader\S3Uploader.exe"
	$global:configuration = "Release"
	
	
	$web_dlls = @( "Raven.Abstractions.???","Raven.Web.???", (Get-DependencyPackageFiles 'NLog.2'), (Get-DependencyPackageFiles Microsoft.Web.Infrastructure), "Jint.Raven.???",
				"Lucene.Net.???", "Lucene.Net.Contrib.Spatial.NTS.???","Lucene.Net.Contrib.FastVectorHighlighter.???", "Spatial4n.Core.NTS.???", "GeoAPI.dll", "NetTopologySuite.dll", "PowerCollections.dll", 
				"ICSharpCode.NRefactory.???", "ICSharpCode.NRefactory.CSharp.???", "Mono.Cecil.???", "Rhino.Licensing.???", "Esent.Interop.???", "Raven.Database.???" ) |
		ForEach-Object { 
			if ([System.IO.Path]::IsPathRooted($_)) { return $_ }
			return "$build_dir\$_"
		}
	
	$web_files = @("..\DefaultConfigs\web.config" )
	
	$server_files = @( "Raven.Server.???", (Get-DependencyPackageFiles 'NLog.2'), "Lucene.Net.???",
					 "Lucene.Net.Contrib.Spatial.NTS.???","Lucene.Net.Contrib.FastVectorHighlighter.???", "Spatial4n.Core.NTS.???", "GeoAPI.dll", "NetTopologySuite.dll", "PowerCollections.dll",  "ICSharpCode.NRefactory.???", "ICSharpCode.NRefactory.CSharp.???", "Mono.Cecil.???", "Rhino.Licensing.???", 
					"Esent.Interop.???", "Jint.Raven.???","Raven.Abstractions.???", "Raven.Database.???" ) |
		ForEach-Object { 
			if ([System.IO.Path]::IsPathRooted($_)) { return $_ }
			return "$build_dir\$_"
		}
		
	$client_dlls = @( (Get-DependencyPackageFiles 'NLog.2'), "Raven.Client.MvcIntegration.???", 
					"Raven.Abstractions.???", "Raven.Client.Lightweight.???") |
		ForEach-Object { 
			if ([System.IO.Path]::IsPathRooted($_)) { return $_ }
			return "$build_dir\$_"
		}
  
	$silverlight4_dlls = @("Raven.Client.Silverlight-4.???", "AsyncCtpLibrary_Silverlight.???", "DH.Scrypt.???") |
		ForEach-Object { 
			if ([System.IO.Path]::IsPathRooted($_)) { return $_ }
			return "$build_dir\$_"
		}
		
	$silverlight_dlls = @("Raven.Client.Silverlight.???", "AsyncCtpLibrary_Silverlight5.???", "DH.Scrypt.???") |
		ForEach-Object { 
			if ([System.IO.Path]::IsPathRooted($_)) { return $_ }
			return "$build_dir\$_"
		}
 
	$all_client_dlls = @( "Raven.Client.MvcIntegration.???", "Raven.Client.Lightweight.???", "Raven.Client.Embedded.???", "Raven.Abstractions.???", "Raven.Database.???", "BouncyCastle.Crypto.???",
						  "Esent.Interop.???", "Jint.Raven.???","ICSharpCode.NRefactory.???", "ICSharpCode.NRefactory.CSharp.???", "Mono.Cecil.???", "Lucene.Net.???", "Lucene.Net.Contrib.Spatial.NTS.???","Lucene.Net.Contrib.FastVectorHighlighter.???",
						"Spatial4n.Core.NTS.???", "GeoAPI.dll", "NetTopologySuite.dll", "PowerCollections.dll",(Get-DependencyPackageFiles 'NLog.2'),
						   "AsyncCtpLibrary.???") |
		ForEach-Object { 
			if ([System.IO.Path]::IsPathRooted($_)) { return $_ }
			return "$build_dir\$_"
		}
	  
		$test_prjs = @("Raven.Tests.dll","Raven.Bundles.Tests.dll" )
}

task default -depends Stable,Release

task Verify40 {
	if( (ls "$env:windir\Microsoft.NET\Framework\v4.0*") -eq $null ) {
		throw "Building Raven requires .NET 4.0, which doesn't appear to be installed on this machine"
	}
}


task Clean {
	Remove-Item -force -recurse $buildartifacts_dir -ErrorAction SilentlyContinue
	Remove-Item -force -recurse $release_dir -ErrorAction SilentlyContinue
}

task Init -depends Verify40, Clean {

	if($env:BUILD_NUMBER -ne $null) {
		$env:buildlabel  = $env:BUILD_NUMBER
	}
	if($env:buildlabel -eq $null) {
		$env:buildlabel = "13"
	}
	
	$commit = Get-Git-Commit
	(Get-Content "$base_dir\CommonAssemblyInfo.cs") | 
		Foreach-Object { $_ -replace ".13.", ".$($env:buildlabel)." } |
		Foreach-Object { $_ -replace "{commit}", $commit } |
		Set-Content "$base_dir\CommonAssemblyInfo.cs" -Encoding UTF8
	
	
	New-Item $release_dir -itemType directory -ErrorAction SilentlyContinue | Out-Null
	New-Item $build_dir -itemType directory -ErrorAction SilentlyContinue | Out-Null
}

task Compile -depends Init {
	
	$v4_net_version = (ls "$env:windir\Microsoft.NET\Framework\v4.0*").Name
	
	exec { &"C:\Windows\Microsoft.NET\Framework\$v4_net_version\MSBuild.exe" "$base_dir\Utilities\Raven.ProjectRewriter\Raven.ProjectRewriter.csproj" /p:OutDir="$buildartifacts_dir\" }
	exec { &"$build_dir\Raven.ProjectRewriter.exe" }
	
	$dat = "$base_dir\..\BuildsInfo\RavenDB\Settings.dat"
	$datDest = "$base_dir\Raven.Studio\Settings.dat"
	echo $dat
	if (Test-Path $dat) {
		Copy-Item $dat $datDest -force
	}
	ElseIf ((Test-Path $datDest) -eq $false) {
		New-Item $datDest -type file -force
	}
	
	Write-Host "Compiling with '$global:configuration' configuration" -ForegroundColor Yellow
	exec { &"C:\Windows\Microsoft.NET\Framework\$v4_net_version\MSBuild.exe" "$sln_file" /p:OutDir="$buildartifacts_dir\" /p:Configuration=$global:configuration /p:nowarn="1591 1573" }
	remove-item "$build_dir\nlog.config" -force  -ErrorAction SilentlyContinue 
}

task FullStorageTest {
	$global:full_storage_test = $true
}

task Test -depends Compile {
	Clear-Host
	
	Write-Host $test_prjs
	Copy-Item (Get-DependencyPackageFiles 'Rx-Main' -frameworkVersion 'Net4') $build_dir -force
	
	$xUnit = Get-PackagePath xunit.runners
	$xUnit = "$xUnit\tools\xunit.console.clr4.exe"
	Write-Host "xUnit location: $xUnit"
	
	$test_prjs | ForEach-Object { 
		if($global:full_storage_test) {
			$env:raventest_storage_engine = 'esent';
			Write-Host "Testing $build_dir\$_ (esent)"
			exec { &"$xUnit" "$build_dir\$_" }
		}
		else {
			$env:raventest_storage_engine = $null;
			Write-Host "Testing $build_dir\$_ (default)"
			exec { &"$xUnit" "$build_dir\$_" }
		}
	}
}

task StressTest -depends Compile {
	Copy-Item (Get-DependencyPackageFiles 'NLog.2') $build_dir -force
	
	$xUnit = Get-PackagePath xunit.runners
	$xUnit = "$xUnit\tools\xunit.console.clr4.exe"
	
	@("Raven.StressTests.dll") | ForEach-Object { 
		Write-Host "Testing $build_dir\$_"
		
		if($global:full_storage_test) {
			$env:raventest_storage_engine = 'esent';
			Write-Host "Testing $build_dir\$_ (esent)"
			&"$xUnit" "$build_dir\$_"
		}
		else {
			$env:raventest_storage_engine = $null;
			Write-Host "Testing $build_dir\$_ (default)"
			&"$xUnit" "$build_dir\$_"
		}
	}
}

task MeasurePerformance -depends Compile {
	$RavenDbStableLocation = "F:\RavenDB"
	$DataLocation = "F:\Data"
	$LogsLocation = "F:\PerformanceLogs"
	$stableBuildToTests = @(616, 573, 531, 499, 482, 457, 371)
	$stableBuildToTests | ForEach-Object { 
		$RavenServer = $RavenDbStableLocation + "\RavenDB-Build-$_\Server"
		Write-Host "Measure performance against RavenDB Build #$_, Path: $RavenServer"
		exec { &"$build_dir\Raven.Performance.exe" "--database-location=$RavenDbStableLocation --build-number=$_ --data-location=$DataLocation --logs-location=$LogsLocation" }
	}
}

task TestSilverlight -depends Compile, CopyServer {
	try
	{
		$process = Start-Process "$build_dir\Output\Server\Raven.Server.exe" "--ram --set=Raven/Port==8079" -PassThru
		& ".\Tools\StatLight\StatLight.exe" "-x=.\build\Raven.Tests.Silverlight.xap" "--OverrideTestProvider=MSTestWithCustomProvider" "--ReportOutputFile=.\Raven.Tests.Silverlight.Results.xml"
	}
	finally
	{
		Stop-Process -InputObject $process
	}
}

task ReleaseNoTests -depends Stable,DoRelease {

}

task Unstable {
	$global:uploadCategory = "RavenDB-Unstable"
}

task Stable {
	$global:uploadCategory = "RavenDB"
}

task RunTests -depends Test,TestSilverlight

task RunAllTests -depends FullStorageTest,Test,TestSilverlight,StressTest

task Release -depends RunTests,DoRelease

task CopySamples {
	Remove-Item "$build_dir\Output\Samples\" -recurse -force -ErrorAction SilentlyContinue 

	Copy-Item "$base_dir\.nuget\" "$build_dir\Output\Samples\.nuget" -recurse -force
	Copy-Item "$base_dir\CommonAssemblyInfo.cs" "$build_dir\Output\Samples\CommonAssemblyInfo.cs" -force
	Copy-Item "$base_dir\Raven.Samples.sln" "$build_dir\Output\Samples" -force
	Copy-Item $base_dir\Raven.VisualHost "$build_dir\Output\Samples\Raven.VisualHost" -recurse -force
	
	$samples =  Get-ChildItem $base_dir\Samples | Where-Object { $_.PsIsContainer }
	$samples = $samples
	foreach ($sample in $samples) {
		Write-Output $sample
		Copy-Item "$base_dir\Samples\$sample" "$build_dir\Output\Samples\$sample" -recurse -force
		
		Remove-Item "$sample_dir\bin" -force -recurse -ErrorAction SilentlyContinue
		Remove-Item "$sample_dir\obj" -force -recurse -ErrorAction SilentlyContinue

		Remove-Item "$sample_dir\Servers\Shard1\Data" -force -recurse -ErrorAction SilentlyContinue
		Remove-Item "$sample_dir\Servers\Shard2\Data" -force -recurse -ErrorAction SilentlyContinue
		Remove-Item "$sample_dir\Servers\Shard1\Plugins" -force -recurse -ErrorAction SilentlyContinue
		Remove-Item "$sample_dir\Servers\Shard2\Plugins" -force -recurse -ErrorAction SilentlyContinue
		Remove-Item "$sample_dir\Servers\Shard1\RavenDB.exe" -force -recurse -ErrorAction SilentlyContinue
		Remove-Item "$sample_dir\Servers\Shard2\RavenDB.exe" -force -recurse -ErrorAction SilentlyContinue 
	}
	
	$v4_net_version = (ls "$env:windir\Microsoft.NET\Framework\v4.0*").Name
	exec { &"C:\Windows\Microsoft.NET\Framework\$v4_net_version\MSBuild.exe" "$base_dir\Utilities\Raven.Samples.PrepareForRelease\Raven.Samples.PrepareForRelease.csproj" /p:OutDir="$buildartifacts_dir\" }
	exec { &"$build_dir\Raven.Samples.PrepareForRelease.exe" "$build_dir\Output\Samples\Raven.Samples.sln" "$build_dir\Output" }
}

task CreateOutpuDirectories -depends CleanOutputDirectory {
	New-Item $build_dir\Output -Type directory -ErrorAction SilentlyContinue | Out-Null
	New-Item $build_dir\Output\Server -Type directory | Out-Null
	New-Item $build_dir\Output\Web -Type directory | Out-Null
	New-Item $build_dir\Output\Web\bin -Type directory | Out-Null
	New-Item $build_dir\Output\EmbeddedClient -Type directory | Out-Null
	New-Item $build_dir\Output\Client -Type directory | Out-Null
	New-Item $build_dir\Output\Silverlight -Type directory | Out-Null
	New-Item $build_dir\Output\Silverlight-4 -Type directory | Out-Null
	New-Item $build_dir\Output\Bundles -Type directory | Out-Null
	New-Item $build_dir\Output\Samples -Type directory | Out-Null
	New-Item $build_dir\Output\Smuggler -Type directory | Out-Null
	New-Item $build_dir\Output\Backup -Type directory | Out-Null
}

task CleanOutputDirectory { 
	Remove-Item $build_dir\Output -Recurse -Force -ErrorAction SilentlyContinue
}

task CopyEmbeddedClient { 
	$all_client_dlls | ForEach-Object { Copy-Item "$_" $build_dir\Output\EmbeddedClient }
}

task CopySilverlight { 
	$silverlight_dlls + @((Get-DependencyPackageFiles 'NLog.2' -FrameworkVersion sl4)) | 
		ForEach-Object { Copy-Item "$_" $build_dir\Output\Silverlight }
}

task CopySilverlight-4 { 
	$silverlight4_dlls + @((Get-DependencyPackageFiles 'NLog.2' -FrameworkVersion sl4)) | 
		ForEach-Object { Copy-Item "$_" $build_dir\Output\Silverlight-4 }
}

task CopySmuggler {
	Copy-Item $build_dir\Raven.Abstractions.??? $build_dir\Output\Smuggler
	Copy-Item $build_dir\Raven.Smuggler.??? $build_dir\Output\Smuggler
}

task CopyBackup {
	Copy-Item $build_dir\Raven.Abstractions.??? $build_dir\Output\Smuggler
	Copy-Item $build_dir\Raven.Backup.??? $build_dir\Output\Backup
}

task CopyClient {
	$client_dlls | ForEach-Object { Copy-Item "$_" $build_dir\Output\Client }
}

task CopyWeb {
	$web_dlls | ForEach-Object { Copy-Item "$_" $build_dir\Output\Web\bin }
	$web_files | ForEach-Object { Copy-Item "$build_dir\$_" $build_dir\Output\Web }
}

task CopyBundles {
	$items = (Get-ChildItem $build_dir\Raven.Bundles.*.???) + (Get-ChildItem $build_dir\Raven.Client.*.???) | 
				Where-Object { $_.Name.Contains(".Tests.") -eq $false } | ForEach-Object { $_.FullName }
	Copy-Item $items $build_dir\Output\Bundles
}

task CopyServer -depends CreateOutpuDirectories {
	$server_files | ForEach-Object { Copy-Item "$_" $build_dir\Output\Server }
	Copy-Item $base_dir\DefaultConfigs\RavenDb.exe.config $build_dir\Output\Server\Raven.Server.exe.config
}

task CreateDocs {
	$v4_net_version = (ls "$env:windir\Microsoft.NET\Framework\v4.0*").Name
	
	if($env:buildlabel -eq 13)
	{
	  return 
	}
	 
	# we expliclty allows this to fail
	exec { &"C:\Windows\Microsoft.NET\Framework\$v4_net_version\MSBuild.exe" "$base_dir\Raven.Docs.shfbproj" /p:OutDir="$buildartifacts_dir\" }
}

task CopyRootFiles -depends CreateDocs {
	cp $base_dir\license.txt $build_dir\Output\license.txt
	cp $base_dir\Scripts\Start.cmd $build_dir\Output\Start.cmd
	cp $base_dir\Scripts\Raven-UpdateBundles.ps1 $build_dir\Output\Raven-UpdateBundles.ps1
	cp $base_dir\Scripts\Raven-GetBundles.ps1 $build_dir\Output\Raven-GetBundles.ps1
	cp $base_dir\readme.txt $build_dir\Output\readme.txt
	cp $base_dir\Help\Documentation.chm $build_dir\Output\Documentation.chm  -ErrorAction SilentlyContinue
	cp $base_dir\acknowledgments.txt $build_dir\Output\acknowledgments.txt
	cp $base_dir\CommonAssemblyInfo.cs $build_dir\Output\CommonAssemblyInfo.cs
}

task ZipOutput {
	
	if($env:buildlabel -eq 13)
	{
		return 
	}

	$old = pwd
	cd $build_dir\Output
	
	$file = "$release_dir\$global:uploadCategory-Build-$env:buildlabel.zip"
		
	exec { 
		& $tools_dir\zip.exe -9 -A -r `
			$file `
			EmbeddedClient\*.* `
			Client\*.* `
			Samples\*.* `
			Smuggler\*.* `
			Backup\*.* `
			Web\*.* `
			Bundles\*.* `
			Web\bin\*.* `
			Server\*.* `
			*.*
	}
	
	cd $old
}

task ResetBuildArtifcats {
	git checkout "Raven.Database\RavenDB.snk"
}

task DoRelease -depends Compile, `
	CleanOutputDirectory, `
	CreateOutpuDirectories, `
	CopyEmbeddedClient, `
	CopySmuggler, `
	CopyBackup, `
	CopyClient, `
	CopySilverlight, `
	CopySilverlight-4, `
	CopyWeb, `
	CopyBundles, `
	CopyServer, `
	CopyRootFiles, `
	CopySamples, `
	ZipOutput, `
	CreateNugetPackages, `
	ResetBuildArtifcats {	
	Write-Host "Done building RavenDB"
}


task Upload -depends DoRelease {
	Write-Host "Starting upload"
	if (Test-Path $uploader) {
		$log = $env:push_msg 
		if(($log -eq $null) -or ($log.Length -eq 0)) {
		  $log = git log -n 1 --oneline		
		}
		
		$log = $log.Replace('"','''') # avoid problems because of " escaping the output
		
		$file = "$release_dir\$global:uploadCategory-Build-$env:buildlabel.zip"
		write-host "Executing: $uploader ""$global:uploadCategory"" ""$env:buildlabel"" $file ""$log"""
		
		$uploadTryCount = 0
		while ($uploadTryCount -lt 5){
			$uploadTryCount += 1
			Exec { &$uploader "$uploadCategory" "$env:buildlabel" $file "$log" }
			
			if ($lastExitCode -ne 0) {
				write-host "Failed to upload to S3: $lastExitCode. UploadTryCount: $uploadTryCount"
			}
			else {
				break
			}
		}
		
		if ($lastExitCode -ne 0) {
			write-host "Failed to upload to S3: $lastExitCode. UploadTryCount: $uploadTryCount. Build will fail."
			throw "Error: Failed to publish build"
		}
	}
	else {
		Write-Host "could not find upload script $uploadScript, skipping upload"
	}
}	

task UploadStable -depends Stable, DoRelease, Upload	

task UploadUnstable -depends Unstable, DoRelease, Upload

task CreateNugetPackages -depends Compile {

	Remove-Item $base_dir\RavenDB*.nupkg
	
	$nuget_dir = "$build_dir\NuGet"
	Remove-Item $nuget_dir -Force -Recurse -ErrorAction SilentlyContinue
	New-Item $nuget_dir -Type directory | Out-Null
	
	New-Item $nuget_dir\RavenDB.Client\lib\net40 -Type directory | Out-Null
	New-Item $nuget_dir\RavenDB.Client\lib\sl40 -Type directory | Out-Null
	New-Item $nuget_dir\RavenDB.Client\lib\sl50 -Type directory | Out-Null
	Copy-Item $base_dir\NuGet\RavenDB.Client.nuspec $nuget_dir\RavenDB.Client\RavenDB.Client.nuspec
	
	@("Raven.Abstractions.???", "Raven.Client.Lightweight.???") |% { Copy-Item "$build_dir\$_" $nuget_dir\RavenDB.Client\lib\net40 }
	@("Raven.Client.Silverlight-4.???", "AsyncCtpLibrary_Silverlight.???") |% { Copy-Item "$build_dir\$_" $nuget_dir\RavenDB.Client\lib\sl40 }
	@("Raven.Client.Silverlight.???", "AsyncCtpLibrary_Silverlight5.???") |% { Copy-Item "$build_dir\$_" $nuget_dir\RavenDB.Client\lib\sl50	}
		
	New-Item $nuget_dir\RavenDB.Client.MvcIntegration\lib\net40 -Type directory | Out-Null
	Copy-Item $base_dir\NuGet\RavenDB.Client.MvcIntegration.nuspec $nuget_dir\RavenDB.Client.MvcIntegration\RavenDB.Client.MvcIntegration.nuspec
	@("Raven.Client.MvcIntegration.???") |% { Copy-Item "$build_dir\$_" $nuget_dir\RavenDB.Client.MvcIntegration\lib\net40 }
		
	New-Item $nuget_dir\RavenDB.Database\lib\net40 -Type directory | Out-Null
	Copy-Item $base_dir\NuGet\RavenDB.Database.nuspec $nuget_dir\RavenDB.Database\RavenDB.Database.nuspec
	@("Raven.Abstractions.???", "Raven.Database.???", "BouncyCastle.Crypto.???",
		 "Esent.Interop.???", "ICSharpCode.NRefactory.???", "ICSharpCode.NRefactory.CSharp.???", "Mono.Cecil.???", "Lucene.Net.???", "Lucene.Net.Contrib.Spatial.NTS.???","Lucene.Net.Contrib.FastVectorHighlighter.???",
		 "Jint.Raven.???", "Spatial4n.Core.NTS.???", "GeoAPI.dll", "NetTopologySuite.dll", "PowerCollections.dll") `
		 |% { Copy-Item "$build_dir\$_" $nuget_dir\RavenDB.Database\lib\net40 }
	
	New-Item $nuget_dir\RavenDB.Server -Type directory | Out-Null
	Copy-Item $base_dir\NuGet\RavenDB.Server.nuspec $nuget_dir\RavenDB.Server\RavenDB.Server.nuspec
	New-Item $nuget_dir\RavenDB.Server\tools -Type directory | Out-Null
	@("Esent.Interop.???", "ICSharpCode.NRefactory.???", "ICSharpCode.NRefactory.CSharp.???", "Mono.Cecil.???", "Lucene.Net.???", "Lucene.Net.Contrib.Spatial.NTS.???","Lucene.Net.Contrib.FastVectorHighlighter.???",
		"Spatial4n.Core.NTS.???", "GeoAPI.dll", "NetTopologySuite.dll", "PowerCollections.dll",	"NewtonSoft.Json.???", "NLog.???", "Jint.Raven.???",
		"Raven.Abstractions.???", "Raven.Database.???", "Raven.Server.???") |% { Copy-Item "$build_dir\$_" $nuget_dir\RavenDB.Server\tools }
	Copy-Item $base_dir\DefaultConfigs\RavenDb.exe.config $nuget_dir\RavenDB.Server\tools\Raven.Server.exe.config

	New-Item $nuget_dir\RavenDB.Embedded\lib\net40 -Type directory | Out-Null
	Copy-Item $base_dir\NuGet\RavenDB.Embedded.nuspec $nuget_dir\RavenDB.Embedded\RavenDB.Embedded.nuspec
	@("Raven.Client.Embedded.???") |% { Copy-Item "$build_dir\$_" $nuget_dir\RavenDB.Embedded\lib\net40 }
	New-Item $nuget_dir\RavenDB.Embedded\tools -Type directory | Out-Null
	
	# Client packages
	@("Authorization", "UniqueConstraints") | Foreach-Object { 
		$name = $_;
		New-Item $nuget_dir\RavenDB.Client.$name\lib\net40 -Type directory | Out-Null
		Copy-Item $base_dir\NuGet\RavenDB.Client.$name.nuspec $nuget_dir\RavenDB.Client.$name\RavenDB.Client.$name.nuspec
		@("Raven.Client.$_.???") |% { Copy-Item $build_dir\$_ $nuget_dir\RavenDB.Client.$name\lib\net40 }
	}
	
	New-Item $nuget_dir\RavenDB.Bundles.Authorization\lib\net40 -Type directory | Out-Null
	Copy-Item $base_dir\NuGet\RavenDB.Bundles.Authorization.nuspec $nuget_dir\RavenDB.Bundles.Authorization\RavenDB.Bundles.Authorization.nuspec
	@("Raven.Bundles.Authorization.???") |% { Copy-Item "$build_dir\$_" $nuget_dir\RavenDB.Bundles.Authorization\lib\net40 }
	
	New-Item $nuget_dir\RavenDB.Bundles.CascadeDelete\lib\net40 -Type directory | Out-Null
	Copy-Item $base_dir\NuGet\RavenDB.Bundles.CascadeDelete.nuspec $nuget_dir\RavenDB.Bundles.CascadeDelete\RavenDB.Bundles.CascadeDelete.nuspec
	@("Raven.Bundles.CascadeDelete.???") |% { Copy-Item "$build_dir\$_" $nuget_dir\RavenDB.Bundles.CascadeDelete\lib\net40 }
	
	New-Item $nuget_dir\RavenDB.Bundles.IndexReplication\lib\net40 -Type directory | Out-Null
	Copy-Item $base_dir\NuGet\RavenDB.Bundles.IndexReplication.nuspec $nuget_dir\RavenDB.Bundles.IndexReplication\RavenDB.Bundles.IndexReplication.nuspec
	@("Raven.Bundles.IndexReplication.???") |% { Copy-Item "$build_dir\$_" $nuget_dir\RavenDB.Bundles.IndexReplication\lib\net40 }

	New-Item $nuget_dir\RavenDB.Bundles.UniqueConstraints\lib\net40 -Type directory | Out-Null
	Copy-Item $base_dir\NuGet\RavenDB.Bundles.UniqueConstraints.nuspec $nuget_dir\RavenDB.Bundles.UniqueConstraints\RavenDB.Bundles.UniqueConstraints.nuspec
	@("Raven.Bundles.UniqueConstraints.???") |% { Copy-Item "$build_dir\$_" $nuget_dir\RavenDB.Bundles.UniqueConstraints\lib\net40 }
	
	New-Item $nuget_dir\RavenDB.AspNetHost\content -Type directory | Out-Null
	New-Item $nuget_dir\RavenDB.AspNetHost\lib\net40 -Type directory | Out-Null
	@("Raven.Web.???") |% { Copy-Item "$build_dir\$_" $nuget_dir\RavenDB.AspNetHost\lib\net40 }
	Copy-Item $base_dir\NuGet\RavenDB.AspNetHost.nuspec $nuget_dir\RavenDB.AspNetHost\RavenDB.AspNetHost.nuspec
	Copy-Item $base_dir\DefaultConfigs\Nupack.Web.config $nuget_dir\RavenDB.AspNetHost\content\Web.config.transform
	New-Item $nuget_dir\RavenDB.AspNetHost\tools -Type directory | Out-Null
	
	New-Item $nuget_dir\RavenDB.Tests.Helpers\lib\net40 -Type directory | Out-Null
	Copy-Item $base_dir\NuGet\RavenDB.Tests.Helpers.nuspec $nuget_dir\RavenDB.Tests.Helpers\RavenDB.Tests.Helpers.nuspec
	@("Raven.Tests.Helpers.???") |% { Copy-Item "$build_dir\$_" $nuget_dir\RavenDB.Tests.Helpers\lib\net40 }
	New-Item $nuget_dir\RavenDB.Tests.Helpers\content -Type directory | Out-Null
	Copy-Item $base_dir\NuGet\RavenTests $nuget_dir\RavenDB.Tests.Helpers\content\RavenTests -Recurse
	
	$nugetVersion = "$version.$env:buildlabel"
	if ($global:uploadCategory -and $global:uploadCategory.EndsWith("-Unstable")){
		$nugetVersion += "-Unstable"
	}
	
	# Sets the package version in all the nuspec as well as any RavenDB package dependency versions
	$packages = Get-ChildItem $nuget_dir *.nuspec -recurse
	$packages |% { 
		$nuspec = [xml](Get-Content $_.FullName)
		$nuspec.package.metadata.version = $nugetVersion
		$nuspec | Select-Xml '//dependency' |% {
			if($_.Node.Id.StartsWith('RavenDB')){
				$_.Node.Version = "[$nugetVersion]"
			}
		}
		$nuspec.Save($_.FullName);
		Exec { &"$base_dir\.nuget\nuget.exe" pack $_.FullName }
	}
	
	# Upload packages
	$accessPath = "$base_dir\..\Nuget-Access-Key.txt"
	if ( (Test-Path $accessPath) ) {
		$accessKey = Get-Content $accessPath
		$accessKey = $accessKey.Trim()
		
		# Push to nuget repository
		$packages | ForEach-Object {
			Exec { &"$base_dir\.nuget\NuGet.exe" push "$($_.BaseName).$nugetVersion.nupkg" $accessKey }
		}
	}
	else {
		Write-Host "Nuget-Access-Key.txt does not exit. Cannot publish the nuget package." -ForegroundColor Yellow
	}
}

TaskTearDown {
	
	if ($LastExitCode -ne 0) {
		write-host "TaskTearDown detected an error. Build failed." -BackgroundColor Red -ForegroundColor Yellow
		write-host "Yes, something was failed!!!!!!!!!!!!!!!!!!!!!" -BackgroundColor Red -ForegroundColor Yellow
		# throw "TaskTearDown detected an error. Build failed."
		exit 1
	}
}