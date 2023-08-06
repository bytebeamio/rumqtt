#!/usr/bin/env sh

# Fail on error
set -o errexit
# Disable wildcard character expansion
set -o noglob
# Disable uninitialized variable usage
set -o nounset
# Disable error masking in pipe
# shellcheck disable=SC3040
if (set -o pipefail 2> /dev/null); then
    set -o pipefail
fi

# Current directory
# shellcheck disable=SC1007
DIRNAME=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)

# Load commons
# shellcheck source=SCRIPTDIR/../__commons.sh
. "$DIRNAME/../__commons.sh"

# ================
# CONFIGURATION
# ================
# Version
RUMQTTD_VERSION=$(grep -m 1 'version' "$DIRNAME/../../rumqttd/Cargo.toml" | grep -o -P '(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)')
# Cross configuration
CROSS_CONFIG="$DIRNAME/../../Cross.toml"
# Copy binaries
COPY=false
# Output directory
OUT_DIR=

# ================
# FUNCTIONS
# ================
# Show help message
show_help() {
    cat << EOF
Usage: $(basename "$0") [--copy] [--cross-config <FILE>] [--disable-color]
                        [--help] [--log-level <LEVEL>] [--output <DIR>]
                        [--version <VERSION>]

rumqttd binary builder.

Options:
  --copy                 Copy binaries instead of move
                         Default: $COPY

  --cross-config <FILE>  Cross configuration file
                         Default: $CROSS_CONFIG

  --disable-color        Disable color

  --help                 Show this help message and exit

  --log-level <LEVEL>    Logger level
                         Default: $(log_level_name "$LOG_LEVEL")
                         Values:
                           fatal    Fatal level
                           error    Error level
                           warn     Warning level
                           info     Informational level
                           debug    Debug level

  --output <DIR>         Output directory
                         Default: $OUT_DIR

  --version <VERSION>    Version
                         Default: $RUMQTTD_VERSION
EOF
}

# Parse command line arguments
# @param $@ Arguments
parse_args() {
    while [ $# -gt 0 ]; do
        # Number of shift
        _shifts=1

        case $1 in
            --copy)
                # Copy binaries
                COPY=true
                ;;
            --cross-config)
                # Cross configuration
                parse_args_assert_value "$@"

                CROSS_CONFIG=$2
                _shifts=2
                ;;
            --help)
                # Display help message and exit
                show_help
                exit 0
                ;;
            --output)
                # Output directory
                parse_args_assert_value "$@"

                OUT_DIR=$2
                _shifts=2
                ;;
            --version)
                # version
                parse_args_assert_value "$@"

                RUMQTTD_VERSION=$2
                _shifts=2
                ;;
            *)
                # Commons
                parse_args_commons "$@"
                _shifts=$RETVAL
                ;;
        esac

        # Shift arguments
        while [ "$_shifts" -gt 0 ]; do
            shift
            _shifts=$((_shifts = _shifts - 1))
        done
    done
}

# Verify system
verify_system() {
    DEBUG "Verifying system"

    assert_cmd cross
    assert_cmd docker

    [ -f "$CROSS_CONFIG" ] || FATAL "Cross configuration '$CROSS_CONFIG' does not exists"
    printf "%s\n" "$RUMQTTD_VERSION" | grep -q -P '(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)' || FATAL "Version '$RUMQTTD_VERSION' is not valid"
    if [ -n "$OUT_DIR" ] && [ ! -d "$OUT_DIR" ]; then FATAL "Output directory '$OUT_DIR' does not exists"; fi

    DEBUG "Successfully verified system"
}

# Build target
# @param $1 Target
build_target() {
    _target=$1

    INFO "Building '$_target'"

    CROSS_CONFIG="$CROSS_CONFIG" \
        cross build \
        --release \
        --package rumqttd \
        --target "$_target"

    if [ -n "$OUT_DIR" ]; then
        _name="rumqttd-$RUMQTTD_VERSION-$_target"
        # Add .exe if target is windows
        if printf "%s\n" "$_target" | grep -q -P '^.+-.+-windows-.+$'; then _name="$_name.exe"; fi
        _binary_old="$DIRNAME/../../target/$_target/release/rumqttd"
        _binary_new="$OUT_DIR/$_name"

        if [ "$COPY" = true ]; then
            INFO "Copying '$_binary_old' to '$_binary_new'"
            cp "$_binary_old" "$_binary_new"
        else
            INFO "Moving '$_binary_old' to '$_binary_new'"
            mv "$_binary_old" "$_binary_new"
        fi
    fi

    DEBUG "Successfully built '$_target'"
}

# Build targets
# @param $1 Targets
build_targets() {
    while [ $# -gt 0 ]; do
        build_target "$1"
        shift
    done
}

# ================
# MAIN
# ================
{
    parse_args "$@"
    verify_system
    build_targets \
        aarch64-unknown-linux-gnu \
        aarch64-unknown-linux-musl \
        x86_64-unknown-linux-gnu \
        x86_64-unknown-linux-musl
}
