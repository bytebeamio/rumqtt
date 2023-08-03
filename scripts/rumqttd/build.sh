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
RUMQTTD_VERSION=$(grep -m 1 'version' "$DIRNAME/../../rumqttc/Cargo.toml" | grep -o -P '(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)')
# Cross configuration
CROSS_CONFIG="$DIRNAME/../../Cross.toml"
# Move directory
MOVE_DIR=

# ================
# FUNCTIONS
# ================
# Show help message
show_help() {
    cat << EOF
Usage: $(basename "$0") [--disable-color] [--cross-config <FILE>] [--help]
                        [--log-level <LEVEL>] [--move <DIR>] [--version <VERSION>]

rumqttd binary builder.

Options:
  --disable-color        Disable color

  --cross-config <FILE>  Cross configuration file
                         Default: $CROSS_CONFIG

  --help                 Show this help message and exit

  --log-level <LEVEL>    Logger level
                         Default: $(log_level_name "$LOG_LEVEL")
                         Values:
                           fatal    Fatal level
                           error    Error level
                           warn     Warning level
                           info     Informational level
                           debug    Debug level

  --move <DIR>           Move directory
                         Default: $MOVE_DIR

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
            --move)
                # Move directory
                parse_args_assert_value "$@"

                MOVE_DIR=$2
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
    if [ -n "$MOVE_DIR" ] && [ ! -d "$MOVE_DIR" ]; then FATAL "Move directory '$MOVE_DIR' does not exists"; fi

    DEBUG "Successfully verified system"
}

# Build target
# @param $1 Target
build_target() {
    _target=$1
    _name="rumqttd-$RUMQTTD_VERSION-$_target"
    # Add .exe if target is windows
    if printf "%s\n" "$_target" | grep -q -P '^.+-.+-windows-.+$'; then _name="$_name.exe"; fi
    _binary_old="$DIRNAME/../../target/$_target/release/rumqttd"
    _binary_new="$DIRNAME/../../target/$_target/release/$_name"

    INFO "Building '$_target'"

    CROSS_CONFIG="$CROSS_CONFIG" \
        cross build \
        --release \
        --package rumqttd \
        --target "$_target"

    INFO "Renaming '$_binary_old' as '$_binary_new'"
    mv "$_binary_old" "$_binary_new"

    if [ -n "$MOVE_DIR" ]; then
        _binary_old=$_binary_new
        _binary_new="$MOVE_DIR/$_name"

        INFO "Moving '$_binary_old' to '$_binary_new'"
        mv "$_binary_old" "$_binary_new"
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
