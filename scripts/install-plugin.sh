#!/usr/bin/env bash
# Install the packaged plugin into an existing Apache JMeter installation.
set -euo pipefail

if [[ $# -lt 1 || $# -gt 2 ]]; then
    printf 'Usage: %s <jmeter-home> [plugin-jar]\n' "$0" >&2
    exit 2
fi

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
project_dir=$(CDPATH= cd -- "$script_dir/.." && pwd)
jmeter_home=$1
plugin_jar=${2:-"$project_dir/target/iotdb-jmeter-plugin.jar"}
if [[ $# -lt 2 && ! -f "$plugin_jar" ]]; then
    plugin_jar="$project_dir/lib/ext/iotdb-jmeter-plugin.jar"
fi

if [[ ! -f "$jmeter_home/bin/ApacheJMeter.jar" || ! -d "$jmeter_home/lib/ext" ]]; then
    printf 'Not an Apache JMeter installation: %s\n' "$jmeter_home" >&2
    exit 2
fi
if [[ ! -f "$plugin_jar" ]]; then
    printf 'Plugin JAR not found: %s. Build with mvn clean package first.\n' "$plugin_jar" >&2
    exit 2
fi

destination="$jmeter_home/lib/ext/iotdb-jmeter-plugin.jar"
shopt -s nullglob
for old_plugin in "$jmeter_home"/lib/ext/jmeter-connector*.jar \
                  "$jmeter_home"/lib/ext/iotdb-jmeter-plugin*.jar; do
    if [[ "$old_plugin" != "$destination" ]]; then
        printf 'Remove the older plugin before installing: %s\n' "$old_plugin" >&2
        exit 2
    fi
done

temp_jar=$(mktemp "$jmeter_home/lib/ext/.iotdb-jmeter-plugin.XXXXXX")
trap 'rm -f -- "$temp_jar"' EXIT
cp -- "$plugin_jar" "$temp_jar"
chmod 0644 "$temp_jar"
mv -f -- "$temp_jar" "$destination"
printf 'Installed %s\nRestart JMeter to load the plugin.\n' "$destination"
