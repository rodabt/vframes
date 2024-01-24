#!/bin/bash

LATEST_TAG=$(curl -s https://api.github.com/repos/duckdb/duckdb/releases/latest | jq -r '.tag_name')
LIB_LINUX="https://github.com/duckdb/duckdb/releases/download/${LATEST_TAG}/duckdb_cli-linux-amd64.zip"
LIB_MAC="https://github.com/duckdb/duckdb/releases/download/${LATEST_TAG}/duckdb_cli-osx-universal.zip"
LIB_WINDOWS="https://github.com/duckdb/duckdb/releases/download/${LATEST_TAG}/duckdb_cli-windows-amd64.zip"

echo "Updating..."
wget $LIB_LINUX -P ./thirdparty/
wget $LIB_MAC -P ./thirdparty/
wget $LIB_WINDOWS -P ./thirdparty/

echo "Uncompressing..."
unzip -j -o ./thirdparty/duckdb_cli-linux-amd64.zip -d "./thirdparty/"
mv ./thirdparty/duckdb ./thirdparty/duckdb_linux_64 
unzip -j -o ./thirdparty/duckdb_cli-osx-universal.zip -d "./thirdparty/"
mv ./thirdparty/duckdb ./thirdparty/duckdb_macos_64
unzip -j -o ./thirdparty/duckdb_cli-windows-amd64.zip -d "./thirdparty/"
mv ./thirdparty/duckdb.exe ./thirdparty/duckdb_windows_64.exe
rm ./thirdparty/*.zip
chmod +x ./thirdparty/*
