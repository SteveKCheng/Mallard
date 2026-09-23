#!/bin/sh
set -euf
script_dir="$(dirname "$(readlink -f "$0")")"

usage() {
    cat <<EOF
Usage: $0 [OPTIONS]

Builds NuGet packages for Mallard, and optionally pushes them (into a package source).

Options:
    --source=NAME     Push the packages to NuGet package source NAME
    --help            Display this usage text
EOF
    exit 0
}

run() {
  printf '+ %s\n' "$*"
  "$@"
}

# Call GNU getopt to canonicalize arguments.
parsed=$(getopt -o "hs:" --long "help,source:" -n "$0" -- "$@")
if [ $? -ne 0 ]; then
    exit 1
fi

# Re-assign positional parameters using eval to safely retain quotes/spaces
eval set -- "$parsed"

# Parse options until reaching the '--' separator
while true; do
    case "$1" in
        -h|--help)
            usage
            ;;
        -s|--source)
            nuget_source="$2"
            shift 2
            ;;
        --)
            shift
            break
            ;;
        *)
            echo "$0: error in parsing arguments" >&2
            exit 1
            ;;
    esac
done

for package_name in Mallard Mallard.DataFrames ; do
    run pushd "$script_dir/$package_name"
    run dotnet pack 
    package_version="$(dotnet msbuild -t:GetVersion -getProperty:Version)"
    if [ -n "${nuget_source+set}" ] ; then
        run dotnet nuget push --source="${nuget_source}" "../out/package/release/${package_name}.${package_version}.nupkg"
    fi
    run popd
done

run pushd "$script_dir/Mallard.Runtime"
duckdb_version="$(dotnet msbuild -getProperty:DuckDbVersion)"
for platform in linux-x64 win-x64 osx-arm64 ; do
    run dotnet pack -p:RuntimeIdentifier="$platform"
    if [ -n "${nuget_source+set}" ] ; then
        run dotnet nuget push --source="${nuget_source}" "../out/package/release/Mallard.Runtime.${platform}.${duckdb_version}.nupkg"
    fi
done
run popd

