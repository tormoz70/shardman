$ErrorActionPreference = "Stop"
$root = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
Set-Location $root

$go = if (Get-Command go -ErrorAction SilentlyContinue) { "go" } else { "C:\Program Files\Go\bin\go.exe" }

& $go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
& $go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

$protoc = Get-Command protoc -ErrorAction SilentlyContinue
if (-not $protoc) {
    Write-Error "protoc not found; install Protocol Buffers compiler"
}

$env:PATH = "$env:USERPROFILE\go\bin;$env:PATH"
& protoc `
  -I api/proto `
  --go_out=api/gen --go_opt=paths=source_relative `
  --go-grpc_out=api/gen --go-grpc_opt=paths=source_relative `
  api/proto/shardman/v1/shardman.proto

Write-Host "proto generated"
