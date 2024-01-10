module vframes

import os


const bin = $if linux && x64 { os.join_path(@VMODROOT, 'thirdparty', 'duckdb_linux_64') } 
$else $if windows && x64 { os.join_path(@VMODROOT, 'thirdparty', 'duckdb_windows_64.exe') } 
$else $if (darwin || macos) && x64 { os.join_path(@VMODROOT, 'thirdparty', 'duckdb_macos_64') }
$else { 
	eprintln('Not supported OS') 
	exit(1)
}